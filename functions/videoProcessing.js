const {onObjectFinalized} = require("firebase-functions/v2/storage");
const {onCall, HttpsError} = require("firebase-functions/v2/https");
const logger = require("firebase-functions/logger");
const admin = require("firebase-admin");
const path = require("path");
const os = require("os");
const fs = require("fs/promises");
const {spawn} = require("child_process");
const ffmpegPath = require("ffmpeg-static");
const ffprobePath = require("@ffprobe-installer/ffprobe").path;

if (!admin.apps.length) {
  admin.initializeApp();
}

const db = admin.firestore();
const FieldValue = admin.firestore.FieldValue;

const CACHE_SEGMENTS = "public, max-age=31536000, immutable";
const CACHE_PLAYLISTS = "public, max-age=30";
const CACHE_THUMBS = "public, max-age=31536000, immutable";

function runCommand(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {stdio: "inherit", ...options});
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) return resolve();
      reject(new Error(`${command} failed with code ${code}`));
    });
  });
}

async function probeVideo(sourcePath) {
  const args = [
    "-v",
    "error",
    "-select_streams",
    "v:0",
    "-show_entries",
    "stream=width,height,duration",
    "-of",
    "json",
    sourcePath,
  ];
  return new Promise((resolve, reject) => {
    const child = spawn(ffprobePath, args);
    let data = "";
    let errData = "";
    child.stdout.on("data", (chunk) => {
      data += chunk.toString();
    });
    child.stderr.on("data", (chunk) => {
      errData += chunk.toString();
    });
    child.on("error", reject);
    child.on("close", (code) => {
      if (code !== 0) {
        return reject(new Error(`ffprobe failed: ${errData}`));
      }
      try {
        const parsed = JSON.parse(data || "{}");
        const stream = parsed.streams?.[0] || {};
        resolve({
          width: Number(stream.width || 0) || 0,
          height: Number(stream.height || 0) || 0,
          duration: Number(stream.duration || 0) || 0,
        });
      } catch (err) {
        reject(err);
      }
    });
  });
}

function getVariantConfigs(sourceHeight, sourceWidth) {
  const targets = [360, 720, 1080].filter((height) => !sourceHeight || sourceHeight >= height);
  const heights = targets.length ? targets : [Math.max(240, sourceHeight || 360)];
  return heights.map((height) => {
    const ratio = sourceHeight && sourceWidth ? sourceWidth / sourceHeight : 16 / 9;
    const width = Math.round((height * ratio) / 2) * 2;
    const bandwidth = height >= 1080 ? 4500000 : height >= 720 ? 2500000 : 900000;
    return {height, width, bandwidth};
  });
}

async function uploadDirectory(bucket, localDir, destinationPrefix) {
  const entries = await fs.readdir(localDir, {withFileTypes: true});
  await Promise.all(entries.map(async (entry) => {
    const entryPath = path.join(localDir, entry.name);
    if (entry.isDirectory()) {
      await uploadDirectory(bucket, entryPath, `${destinationPrefix}/${entry.name}`);
      return;
    }
    const isPlaylist = entry.name.endsWith(".m3u8");
    const isSegment = entry.name.endsWith(".ts");
    const cacheControl = isSegment ? CACHE_SEGMENTS : CACHE_PLAYLISTS;
    const contentType = isPlaylist
      ? "application/vnd.apple.mpegurl"
      : isSegment
        ? "video/MP2T"
        : undefined;
    await bucket.upload(entryPath, {
      destination: `${destinationPrefix}/${entry.name}`,
      metadata: {
        cacheControl,
        contentType,
      },
    });
  }));
}

async function processVideoObject({bucket, filePath, uid, videoId}) {
  const videoRef = db.collection("videos").doc(videoId);
  const videoSnap = await videoRef.get();
  const currentData = videoSnap.exists ? videoSnap.data() : {};
  if ((currentData?.processing?.status || "").toUpperCase() === "READY") {
    logger.info("Video already processed", {videoId});
    return;
  }

  await videoRef.set({
    storage: {
      sourcePath: filePath,
    },
    processing: {
      status: "PROCESSING",
      startedAt: FieldValue.serverTimestamp(),
      updatedAt: FieldValue.serverTimestamp(),
    },
  }, {merge: true});

  const tmpDir = path.join(os.tmpdir(), `video-${videoId}`);
  await fs.rm(tmpDir, {recursive: true, force: true});
  await fs.mkdir(tmpDir, {recursive: true});
  const localSource = path.join(tmpDir, "source.mp4");
  await bucket.file(filePath).download({destination: localSource});

  const meta = await probeVideo(localSource);
  const variantConfigs = getVariantConfigs(meta.height, meta.width);

  const thumbLocal = path.join(tmpDir, "thumb_720.webp");
  await runCommand(ffmpegPath, [
    "-y",
    "-ss",
    "1",
    "-i",
    localSource,
    "-vf",
    "scale=-2:720",
    "-frames:v",
    "1",
    thumbLocal,
  ]);

  const thumbPath = `videos/${uid}/${videoId}/thumb_720.webp`;
  await bucket.upload(thumbLocal, {
    destination: thumbPath,
    metadata: {
      cacheControl: CACHE_THUMBS,
      contentType: "image/webp",
    },
  });

  const hlsRoot = path.join(tmpDir, "hls");
  await fs.mkdir(hlsRoot, {recursive: true});

  for (const variant of variantConfigs) {
    const variantDir = path.join(hlsRoot, `${variant.height}p`);
    await fs.mkdir(variantDir, {recursive: true});
    const playlistPath = path.join(variantDir, "index.m3u8");
    const segmentPattern = path.join(variantDir, "segment_%03d.ts");
    await runCommand(ffmpegPath, [
      "-y",
      "-i",
      localSource,
      "-vf",
      `scale=-2:${variant.height}`,
      "-c:v",
      "h264",
      "-profile:v",
      "main",
      "-crf",
      "20",
      "-sc_threshold",
      "0",
      "-g",
      "48",
      "-keyint_min",
      "48",
      "-c:a",
      "aac",
      "-ar",
      "48000",
      "-b:a",
      "128k",
      "-hls_time",
      "4",
      "-hls_playlist_type",
      "vod",
      "-hls_flags",
      "independent_segments",
      "-hls_segment_filename",
      segmentPattern,
      playlistPath,
    ]);
  }

  const masterLines = ["#EXTM3U"];
  variantConfigs.forEach((variant) => {
    masterLines.push(`#EXT-X-STREAM-INF:BANDWIDTH=${variant.bandwidth},RESOLUTION=${variant.width}x${variant.height}`);
    masterLines.push(`${variant.height}p/index.m3u8`);
  });
  const masterPathLocal = path.join(hlsRoot, "master.m3u8");
  await fs.writeFile(masterPathLocal, masterLines.join("\n"));

  const hlsDestination = `videos/${uid}/${videoId}/hls`;
  await uploadDirectory(bucket, hlsRoot, hlsDestination);

  const hlsMasterPath = `${hlsDestination}/master.m3u8`;

  await videoRef.set({
    storage: {
      sourcePath: filePath,
      thumbPath,
      hlsMasterPath,
    },
    media: {
      durationSeconds: meta.duration ? Math.round(meta.duration) : null,
      width: meta.width || null,
      height: meta.height || null,
    },
    processing: {
      status: "READY",
      completedAt: FieldValue.serverTimestamp(),
      updatedAt: FieldValue.serverTimestamp(),
    },
  }, {merge: true});

  await fs.rm(tmpDir, {recursive: true, force: true});
}

exports.processVideoOnUpload = onObjectFinalized({
  region: "us-central1",
  memory: "2GiB",
  timeoutSeconds: 540,
}, async (event) => {
  const object = event.data;
  const filePath = object?.name || "";
  const contentType = object?.contentType || "";
  if (!filePath || !filePath.endsWith("/source.mp4")) return;
  if (!contentType.startsWith("video/")) return;
  const parts = filePath.split("/");
  if (parts.length < 4) return;
  const uid = parts[1];
  const videoId = parts[2];
  if (!uid || !videoId) return;
  const bucket = admin.storage().bucket(object.bucket);
  try {
    await processVideoObject({bucket, filePath, uid, videoId});
  } catch (err) {
    logger.error("Video processing failed", {videoId, error: err.message});
    await db.collection("videos").doc(videoId).set({
      processing: {
        status: "FAILED",
        updatedAt: FieldValue.serverTimestamp(),
        error: err.message || "Processing failed",
      },
    }, {merge: true});
  }
});

exports.reprocessVideo = onCall({
  region: "us-central1",
  memory: "2GiB",
  timeoutSeconds: 540,
}, async (request) => {
  if (!request.auth) {
    throw new HttpsError("unauthenticated", "Authentication required");
  }
  if (!request.auth.token?.admin) {
    throw new HttpsError("permission-denied", "Admin access required");
  }
  const videoId = request.data?.videoId;
  if (!videoId) {
    throw new HttpsError("invalid-argument", "videoId is required");
  }
  const videoSnap = await db.collection("videos").doc(videoId).get();
  if (!videoSnap.exists) {
    throw new HttpsError("not-found", "Video not found");
  }
  const data = videoSnap.data() || {};
  const uid = data.ownerId || data.userId || "";
  const sourcePath = data.storage?.sourcePath || (data.storagePath ? `${data.storagePath}/source.mp4` : "");
  if (!sourcePath || !uid) {
    throw new HttpsError("failed-precondition", "Missing source path for video");
  }
  const bucket = admin.storage().bucket();
  await processVideoObject({bucket, filePath: sourcePath, uid, videoId});
  return {status: "processing"};
});
