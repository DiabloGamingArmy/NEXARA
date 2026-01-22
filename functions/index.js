/**
 * Import function triggers from their respective submodules:
 *
 * const {onCall} = require("firebase-functions/v2/https");
 * const {onDocumentWritten} = require("firebase-functions/v2/firestore");
 *
 * See a full list of supported triggers at https://firebase.google.com/docs/functions
 */

const {setGlobalOptions} = require("firebase-functions");
const {defineSecret} = require("firebase-functions/params");
const {onCall: onCallV2, HttpsError} = require("firebase-functions/v2/https");
const {onCall, onRequest} = require("firebase-functions/https");
const {onDocumentCreated, onDocumentDeleted, onDocumentUpdated} = require("firebase-functions/v2/firestore");
const {onObjectFinalized} = require("firebase-functions/v2/storage");
const logger = require("firebase-functions/logger");
const admin = require("firebase-admin");
const {AccessToken} = require("livekit-server-sdk");
// Secrets
const LIVEKIT_API_KEY = defineSecret("LIVEKIT_API_KEY");
const LIVEKIT_API_SECRET = defineSecret("LIVEKIT_API_SECRET");
const LIVEKIT_URL = defineSecret("LIVEKIT_URL");

// For cost control, you can set the maximum number of containers that can be
// running at the same time. This helps mitigate the impact of unexpected
// traffic spikes by instead downgrading performance. This limit is a
// per-function limit. You can override the limit for each function using the
// `maxInstances` option in the function's options, e.g.
// `onRequest({ maxInstances: 5 }, (req, res) => { ... })`.
// NOTE: setGlobalOptions does not apply to functions using the v1 API. V1
// functions should each use functions.runWith({ maxInstances: 10 }) instead.
// In the v1 API, each function can only serve one request per container, so
// this will be the maximum concurrent request count.
setGlobalOptions({ maxInstances: 10, region: "us-central1" });

// Create and deploy your first functions
// https://firebase.google.com/docs/functions/get-started

// exports.helloWorld = onRequest((request, response) => {
//   logger.info("Hello logs!", {structuredData: true});
//   response.send("Hello from Firebase!");
// });

const ivs = require("./ivs.js");
const uploads = require("./uploads.js");
const videoProcessing = require("./videoProcessing.js");

exports.initializeUserChannel = ivs.initializeUserChannel;
exports.createEphemeralChannel = ivs.createEphemeralChannel;
exports.generatePlaybackToken = ivs.generatePlaybackToken;
exports.processVideoOnUpload = videoProcessing.processVideoOnUpload;
exports.reprocessVideo = videoProcessing.reprocessVideo;

if (!admin.apps.length) {
  admin.initializeApp();
}

const db = admin.firestore();
const {FieldValue} = admin.firestore;
const RATE_LIMITS = {
  liveChat: {limit: 5, windowMs: 10 * 1000},
  comments: {limit: 3, windowMs: 30 * 1000},
  reviews: {limit: 1, windowMs: 10 * 60 * 1000},
  likes: {limit: 20, windowMs: 60 * 1000},
  assets: {limit: 10, windowMs: 60 * 1000},
};

logger.info("Functions build marker", {build: "createPost-previewText-fix-v1"});

const ASSET_POLICIES = {
  avatar: {maxBytes: 5 * 1024 * 1024, allowedPrefixes: ["image/"]},
  image: {maxBytes: 25 * 1024 * 1024, allowedPrefixes: ["image/"]},
  audio: {maxBytes: 200 * 1024 * 1024, allowedPrefixes: ["audio/"]},
  document: {
    maxBytes: 100 * 1024 * 1024,
    allowedPrefixes: [
      "application/pdf",
      "text/plain",
      "application/msword",
      "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ],
  },
  video: {maxBytes: 2 * 1024 * 1024 * 1024, allowedPrefixes: ["video/"]},
};

function assertAppCheckV2(request) {
  if (!request.app) {
    throw new HttpsError("failed-precondition", "Missing or invalid App Check token.");
  }
}

function assertAppCheckV1(context) {
  if (!context.app) {
    throw new HttpsError("failed-precondition", "Missing or invalid App Check token.");
  }
}

async function enforceRateLimit(uid, bucketKey, {limit, windowMs}) {
  const now = Date.now();
  const resetAt = now + windowMs;
  const docId = `${uid}_${bucketKey}`;
  const ref = db.collection("rateLimits").doc(docId);
  await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    let count = 0;
    let windowResetAt = resetAt;
    if (snap.exists) {
      const data = snap.data() || {};
      if (data.resetAt && data.resetAt > now) {
        count = data.count || 0;
        windowResetAt = data.resetAt;
      }
    }
    if (count >= limit) {
      throw new HttpsError("resource-exhausted", "Rate limit exceeded. Try again soon.");
    }
    tx.set(ref, {count: count + 1, resetAt: windowResetAt}, {merge: true});
  });
}

function normalizeText(value = "", maxLen = 1000) {
  const trimmed = String(value || "").trim();
  if (!trimmed) return "";
  return trimmed.slice(0, maxLen);
}

function normalizeReviewRating(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "number" && Number.isFinite(value)) return Math.round(value);
  const raw = String(value || "").trim().toLowerCase();
  if (raw === "verified") return 5;
  if (raw === "citation") return 3;
  if (raw === "misleading") return 1;
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? Math.round(parsed) : null;
}

function getReviewScoreDelta(ratingValue) {
  if (!Number.isFinite(ratingValue)) return 0;
  return ratingValue >= 4 ? 1 : -1;
}

function isStaffClaims(auth) {
  return auth?.token?.admin === true || auth?.token?.staff === true || auth?.token?.founder === true;
}

function isAllowedContentType(kind, contentType) {
  if (!kind || !contentType) return false;
  const policy = ASSET_POLICIES[kind];
  if (!policy) return false;
  return policy.allowedPrefixes.some((prefix) => contentType.startsWith(prefix));
}

function validateAssetPolicy(kind, contentType, sizeBytes) {
  const policy = ASSET_POLICIES[kind];
  if (!policy) return {ok: false, reason: "unsupported-kind"};
  if (!isAllowedContentType(kind, contentType)) return {ok: false, reason: "invalid-type"};
  if (sizeBytes > policy.maxBytes) return {ok: false, reason: "oversize"};
  return {ok: true};
}

function normalizeUrl(value = "") {
  try {
    const url = new URL(String(value || "").trim());
    if (!["http:", "https:"].includes(url.protocol)) return "";
    return url.toString();
  } catch (error) {
    return "";
  }
}

function extractMetaContent(html = "", matcher = "") {
  if (!html || !matcher) return "";
  const regex = new RegExp(`<meta[^>]+${matcher}[^>]+content=["']([^"']+)["']`, "i");
  const match = html.match(regex);
  return match ? match[1].trim() : "";
}

function extractTitleFromHtml(html = "") {
  const match = html.match(/<title[^>]*>([^<]+)<\/title>/i);
  return match ? match[1].trim() : "";
}

function collectModerationText(title = "", blocks = []) {
  const lines = [];
  if (title) lines.push(String(title));
  (blocks || []).forEach((block) => {
    if (!block || typeof block !== "object") return;
    if (block.type === "text" && block.text) lines.push(String(block.text));
    if (block.type === "asset" && block.caption) lines.push(String(block.caption));
    if (block.type === "link") {
      if (block.title) lines.push(String(block.title));
      if (block.description) lines.push(String(block.description));
      if (block.url) lines.push(String(block.url));
    }
  });
  return lines.join("\n").trim();
}

function stripUndefined(value) {
  if (Array.isArray(value)) {
    return value
      .map(stripUndefined)
      .filter((entry) => entry !== undefined);
  }
  if (!value || typeof value !== "object") return value;
  const output = {};
  for (const [key, val] of Object.entries(value)) {
    if (val === undefined) continue;
    output[key] = stripUndefined(val);
  }
  return output;
}

async function assertAssetOwnership(assetIds = [], uid) {
  const assets = {};
  for (const assetId of assetIds) {
    const snap = await db.collection("assets").doc(assetId).get();
    if (!snap.exists) throw new HttpsError("not-found", "Asset not found.");
    const asset = snap.data() || {};
    if (asset.ownerId !== uid) throw new HttpsError("permission-denied", "Asset ownership mismatch.");
    assets[assetId] = asset;
  }
  return assets;
}

function buildComposerPostPreview(blocks = [], title = "") {
  const preview = {
    previewText: "",
    previewAssetId: "",
    previewType: "text",
    previewLink: "",
  };
  if (title) preview.previewText = String(title).slice(0, 140);
  for (const block of blocks) {
    if (!block || typeof block !== "object") continue;
    if (!preview.previewText && block.type === "text" && block.text) {
      preview.previewText = String(block.text).slice(0, 140);
    }
    if (!preview.previewAssetId && block.type === "asset" && block.assetId) {
      preview.previewAssetId = block.assetId;
      preview.previewType = block.presentation || "asset";
    }
    if (!preview.previewLink && block.type === "link" && block.url) {
      preview.previewLink = block.url;
      preview.previewType = "link";
    }
  }
  if (!preview.previewType) {
    const fallbackBlock = blocks.find((block) => block && block.type);
    preview.previewType = fallbackBlock ? fallbackBlock.type : "text";
  }
  return preview;
}

function normalizeBlocks(rawBlocks = []) {
  if (!Array.isArray(rawBlocks)) return [];
  return rawBlocks.map((block) => {
    if (!block || typeof block !== "object") return null;
    const type = String(block.type || "").trim().toLowerCase();
    if (type === "text") {
      const text = normalizeText(block.text || "", 5000);
      if (!text) return null;
      return {type: "text", text};
    }
    if (type === "asset") {
      const assetId = String(block.assetId || "").trim();
      if (!assetId) return null;
      const presentation = String(block.presentation || "image").trim().toLowerCase();
      const caption = normalizeText(block.caption || "", 500);
      const blockPayload = {type: "asset", assetId, presentation};
      if (caption) blockPayload.caption = caption;
      if (presentation === "audio") {
        const chapters = normalizeText(block.chapters || "", 1000);
        const lyrics = normalizeText(block.lyrics || "", 3000);
        if (chapters) blockPayload.chapters = chapters;
        if (lyrics) blockPayload.lyrics = lyrics;
      }
      if (block.thumbnailAssetId) {
        blockPayload.thumbnailAssetId = String(block.thumbnailAssetId || "").trim();
      }
      return blockPayload;
    }
    if (type === "link") {
      const url = normalizeUrl(block.url || "");
      if (!url) return null;
      const linkBlock = {
        type: "link",
        url,
        title: normalizeText(block.title || "", 200),
        description: normalizeText(block.description || "", 500),
      };
      if (block.imageAssetId) linkBlock.imageAssetId = String(block.imageAssetId || "").trim();
      if (block.imageUrl) linkBlock.imageUrl = normalizeUrl(block.imageUrl || "");
      return linkBlock;
    }
    if (type === "capsule") {
      const capsuleId = String(block.capsuleId || "").trim();
      if (!capsuleId) return null;
      return {type: "capsule", capsuleId};
    }
    if (type === "live") {
      const sessionId = String(block.sessionId || "").trim();
      if (!sessionId) return null;
      return {type: "live", sessionId};
    }
    return null;
  }).filter(Boolean);
}

async function moderateTextContent(text, contextLabel) {
  if (!text) {
    return {status: "approved", labels: ["unconfigured"], scoreMap: {}, modelVersion: "none", reviewRequired: false};
  }
  return {
    status: "approved",
    labels: ["unconfigured"],
    scoreMap: {},
    modelVersion: "none",
    reviewRequired: false,
  };
}

async function moderateAssetContent(metadata) {
  if (!metadata) {
    return {status: "approved", labels: ["unconfigured"], scoreMap: {}, modelVersion: "none", reviewRequired: false};
  }
  return {
    status: "approved",
    labels: ["unconfigured"],
    scoreMap: {},
    modelVersion: "none",
    reviewRequired: false,
  };
}

async function resolveActorProfile(actorId) {
  if (!actorId) return {actorName: "Someone", actorPhotoUrl: ""};
  try {
    const userRecord = await admin.auth().getUser(actorId);
    return {
      actorName: userRecord.displayName || userRecord.email || "Someone",
      actorPhotoUrl: userRecord.photoURL || "",
    };
  } catch (error) {
    logger.warn("Unable to resolve actor profile", {actorId, error: error.message});
    return {actorName: "Someone", actorPhotoUrl: ""};
  }
}

function buildNotificationPostPreview(post = {}) {
  const titleCandidate = post.title || post.content || post.body || post.text || "";
  const previewSource = typeof titleCandidate === "string" ? titleCandidate : String(titleCandidate || "");
  const thumbnailCandidate = post.mediaUrl || post.thumbnailUrl || post.thumbnail || "";
  return {
    title: previewSource.slice(0, 80),
    thumbnail: typeof thumbnailCandidate === "string" ? thumbnailCandidate : String(thumbnailCandidate || ""),
  };
}

function buildVideoPreview(video = {}) {
  const title = video.title || video.description || "";
  return {
    title: typeof title === "string" ? title.slice(0, 80) : "",
    thumbnail: video.thumbURL || video.thumbnail || video.previewImage || "",
  };
}

function buildLivePreview(session = {}) {
  const title = session.title || session.name || "Live stream";
  return {
    title: typeof title === "string" ? title.slice(0, 80) : "Live stream",
    thumbnail: session.thumbnail || session.thumbnailUrl || session.coverImage || session.imageUrl || "",
  };
}

function buildMessagePreview(message = {}) {
  const text = (message.text || "").toString().trim();
  if (text) return text.slice(0, 120);
  const attachments = Array.isArray(message.attachments) ? message.attachments : [];
  const mediaType = (message.mediaType || message.type || "").toString().toLowerCase();
  const attachmentType = attachments[0]?.type || "";
  const typeHint = (attachmentType || mediaType).toLowerCase();
  if (typeHint.includes("image")) return "Sent a photo";
  if (typeHint.includes("video")) return "Sent a video";
  if (attachments.length) return "Sent an attachment";
  if (message.mediaURL || message.mediaPath) return "Sent media";
  return "New message";
}

const ACCOUNT_NOTIFICATION_FIELDS = [
  {field: "displayName", actionType: "name"},
  {field: "name", actionType: "name"},
  {field: "realName", actionType: "name"},
  {field: "nickname", actionType: "name"},
  {field: "username", actionType: "username"},
  {field: "email", actionType: "email"},
  {field: "bio", actionType: "profile"},
  {field: "links", actionType: "profile"},
  {field: "phone", actionType: "profile"},
  {field: "gender", actionType: "profile"},
  {field: "region", actionType: "profile"},
];

function normalizeAccountValue(value) {
  if (value === null || value === undefined) return "";
  return value.toString();
}

function pickOtherParticipantField(participants = [], values = [], targetUid = "") {
  if (!Array.isArray(values) || !Array.isArray(participants)) return [];
  const mapped = [];
  participants.forEach((uid, idx) => {
    if (uid !== targetUid) mapped.push(values[idx] || "");
  });
  return mapped;
}

async function createContentNotification({
  actorId,
  targetUserId,
  contentId,
  contentType,
  actionType,
  contentTitle,
  contentThumbnailUrl,
}) {
  if (!actorId || !targetUserId || actorId === targetUserId) return;
  const {actorName, actorPhotoUrl} = await resolveActorProfile(actorId);
  await db.collection("users").doc(targetUserId).collection("notifications").add({
    type: "content",
    actorId,
    actorName,
    actorAvatar: actorPhotoUrl,
    actorPhotoUrl,
    targetUserId,
    contentId,
    contentType,
    actionType,
    contentTitle: contentTitle || "",
    contentThumbnailUrl: contentThumbnailUrl || "",
    createdAt: FieldValue.serverTimestamp(),
    read: false,
    isRead: false,
  });
}

async function updateTrendingPopularity(slug, delta) {
  if (!slug) return;
  const ref = db.collection("trendingCategories").doc(slug);
  await ref.set(
    {
      slug,
      popularity: FieldValue.increment(delta),
      updatedAt: FieldValue.serverTimestamp(),
    },
    {merge: true},
  );
}

exports.onCategoryCreate = onDocumentCreated("categories/{categoryId}", async (event) => {
  const data = event.data?.data() || {};
  const slug = data.slug || event.params.categoryId;
  if (!slug) return;
  await db.collection("trendingCategories").doc(slug).set(
    {
      slug,
      popularity: 0,
      updatedAt: FieldValue.serverTimestamp(),
    },
    {merge: true},
  );
});

exports.onPostCreate = onDocumentCreated("posts/{postId}", async (event) => {
  const data = event.data?.data() || {};
  const slug = data.categoryId || data.categorySlug || data.category;
  await updateTrendingPopularity(slug, 5);
});

exports.onCommentCreate = onDocumentCreated("posts/{postId}/comments/{commentId}", async (event) => {
  const postId = event.params.postId;
  const postSnap = await db.collection("posts").doc(postId).get();
  const postData = postSnap.exists ? postSnap.data() : {};
  const slug = postData?.categoryId || postData?.categorySlug || postData?.category;
  await updateTrendingPopularity(slug, 1);
});

exports.onPostDelete = onDocumentDeleted("posts/{postId}", async (event) => {
  const data = event.data?.data() || {};
  const slug = data.categoryId || data.categorySlug || data.category;
  await updateTrendingPopularity(slug, -5);
});

exports.onCommentDelete = onDocumentDeleted("posts/{postId}/comments/{commentId}", async (event) => {
  const postId = event.params.postId;
  const postSnap = await db.collection("posts").doc(postId).get();
  const postData = postSnap.exists ? postSnap.data() : {};
  const slug = postData?.categoryId || postData?.categorySlug || postData?.category;
  await updateTrendingPopularity(slug, -1);
});

exports.onUserProfileUpdate = onDocumentUpdated("users/{userId}", async (event) => {
  const before = event.data?.before?.data() || {};
  const after = event.data?.after?.data() || {};
  const userId = event.params.userId;
  if (!userId) return;

  const notifications = [];
  const seen = new Set();
  ACCOUNT_NOTIFICATION_FIELDS.forEach(({field, actionType}) => {
    const beforeValue = normalizeAccountValue(before[field]);
    const afterValue = normalizeAccountValue(after[field]);
    if (beforeValue === afterValue) return;
    const key = `${actionType}:${beforeValue}:${afterValue}`;
    if (seen.has(key)) return;
    seen.add(key);
    notifications.push({
      targetUid: userId,
      type: "account",
      entityType: "account",
      actionType,
      accountField: field,
      from: beforeValue,
      to: afterValue,
      createdAt: FieldValue.serverTimestamp(),
      read: false,
    });
  });

  if (!notifications.length) return;
  const notifRef = db.collection("users").doc(userId).collection("notifications");
  await Promise.all(notifications.map((notif) => notifRef.add(notif)));
});

exports.onPostLike = onDocumentCreated("posts/{postId}/likes/{uid}", async (event) => {
  const actorId = event.params.uid;
  const postId = event.params.postId;
  const postSnap = await db.collection("posts").doc(postId).get();
  const postData = postSnap.exists ? postSnap.data() : null;
  if (!postData || !postData.userId) return;
  const preview = buildNotificationPostPreview(postData);
  await createContentNotification({
    actorId,
    targetUserId: postData.userId,
    contentId: postId,
    contentType: "post",
    actionType: "like",
    contentTitle: preview.title,
    contentThumbnailUrl: preview.thumbnail,
  });
});

exports.onPostDislike = onDocumentCreated("posts/{postId}/dislikes/{uid}", async (event) => {
  const actorId = event.params.uid;
  const postId = event.params.postId;
  const postSnap = await db.collection("posts").doc(postId).get();
  const postData = postSnap.exists ? postSnap.data() : null;
  if (!postData || !postData.userId) return;
  const preview = buildNotificationPostPreview(postData);
  await createContentNotification({
    actorId,
    targetUserId: postData.userId,
    contentId: postId,
    contentType: "post",
    actionType: "dislike",
    contentTitle: preview.title,
    contentThumbnailUrl: preview.thumbnail,
  });
});

exports.onPostCommentNotification = onDocumentCreated("posts/{postId}/comments/{commentId}", async (event) => {
  const comment = event.data?.data() || {};
  const actorId = comment.userId;
  if (!actorId) return;
  const postId = event.params.postId;
  const postSnap = await db.collection("posts").doc(postId).get();
  const postData = postSnap.exists ? postSnap.data() : null;
  if (!postData || !postData.userId) return;
  const preview = buildNotificationPostPreview(postData);
  await createContentNotification({
    actorId,
    targetUserId: postData.userId,
    contentId: postId,
    contentType: "post",
    actionType: "comment",
    contentTitle: preview.title,
    contentThumbnailUrl: preview.thumbnail,
  });
});

exports.onVideoLike = onDocumentCreated("videos/{videoId}/likes/{uid}", async (event) => {
  const actorId = event.params.uid;
  const videoId = event.params.videoId;
  const videoSnap = await db.collection("videos").doc(videoId).get();
  const videoData = videoSnap.exists ? videoSnap.data() : null;
  if (!videoData || !videoData.ownerId) return;
  const preview = buildVideoPreview(videoData);
  await createContentNotification({
    actorId,
    targetUserId: videoData.ownerId,
    contentId: videoId,
    contentType: "video",
    actionType: "like",
    contentTitle: preview.title,
    contentThumbnailUrl: preview.thumbnail,
  });
});

exports.onVideoDislike = onDocumentCreated("videos/{videoId}/dislikes/{uid}", async (event) => {
  const actorId = event.params.uid;
  const videoId = event.params.videoId;
  const videoSnap = await db.collection("videos").doc(videoId).get();
  const videoData = videoSnap.exists ? videoSnap.data() : null;
  if (!videoData || !videoData.ownerId) return;
  const preview = buildVideoPreview(videoData);
  await createContentNotification({
    actorId,
    targetUserId: videoData.ownerId,
    contentId: videoId,
    contentType: "video",
    actionType: "dislike",
    contentTitle: preview.title,
    contentThumbnailUrl: preview.thumbnail,
  });
});

exports.onLiveStreamChat = onDocumentCreated("liveStreams/{sessionId}/chat/{chatId}", async (event) => {
  const chat = event.data?.data() || {};
  const actorId = chat.senderId || chat.userId;
  if (!actorId) return;
  const sessionId = event.params.sessionId;
  const sessionSnap = await db.collection("liveStreams").doc(sessionId).get();
  const sessionData = sessionSnap.exists ? sessionSnap.data() : null;
  if (!sessionData || !sessionData.hostId) return;
  const preview = buildLivePreview(sessionData);
  await createContentNotification({
    actorId,
    targetUserId: sessionData.hostId,
    contentId: sessionId,
    contentType: "liveStream",
    actionType: "comment",
    contentTitle: preview.title,
    contentThumbnailUrl: preview.thumbnail,
  });
});

exports.onConversationMessage = onDocumentCreated("conversations/{conversationId}/messages/{messageId}", async (event) => {
  const message = event.data?.data() || {};
  const senderId = message.senderId;
  if (!senderId) return;
  const conversationId = event.params.conversationId;
  const messageId = event.params.messageId;

  const convoSnap = await db.collection("conversations").doc(conversationId).get();
  if (!convoSnap.exists) return;
  const convo = convoSnap.data() || {};
  const participants = Array.isArray(convo.participants) ? convo.participants : [];
  if (!participants.length) return;

  const recipients = participants.filter((uid) => uid && uid !== senderId);
  if (!recipients.length) return;

  const preview = buildMessagePreview(message);
  const {actorName} = await resolveActorProfile(senderId);
  const isGroup = participants.length > 2 || convo.type === "group";
  const convoTitle = convo.title || "";
  const notificationTitle = isGroup && convoTitle ? `${actorName} in ${convoTitle}` : actorName;
  const messageTimestamp = message.createdAt || FieldValue.serverTimestamp();

  const convoRef = db.collection("conversations").doc(conversationId);
  const convoUpdate = {
    lastMessagePreview: preview,
    lastMessageSenderId: senderId,
    lastMessageAt: messageTimestamp,
    updatedAt: FieldValue.serverTimestamp(),
    [`unreadCounts.${senderId}`]: 0,
  };
  recipients.forEach((uid) => {
    convoUpdate[`unreadCounts.${uid}`] = FieldValue.increment(1);
  });

  const batch = db.batch();
  batch.set(convoRef, convoUpdate, {merge: true});

  participants.forEach((uid) => {
    const mappingRef = db.collection("users").doc(uid).collection("conversations").doc(conversationId);
    const mappingUpdate = {
      conversationId,
      participants,
      otherParticipantIds: participants.filter((pid) => pid !== uid),
      otherParticipantUsernames: pickOtherParticipantField(participants, convo.participantUsernames || [], uid),
      otherParticipantNames: pickOtherParticipantField(participants, convo.participantNames || [], uid),
      otherParticipantAvatars: pickOtherParticipantField(participants, convo.participantAvatars || [], uid),
      lastMessagePreview: preview,
      lastMessageSenderId: senderId,
      lastMessageAt: messageTimestamp,
    };
    mappingUpdate.unreadCount = uid === senderId ? 0 : FieldValue.increment(1);
    batch.set(mappingRef, mappingUpdate, {merge: true});
  });

  recipients.forEach((uid) => {
    const notifRef = db.collection("users")
        .doc(uid)
        .collection("notifications")
        .doc(`dm_${conversationId}_${messageId}`);
    batch.set(notifRef, {
      type: "dm",
      conversationId,
      messageId,
      fromUid: senderId,
      title: notificationTitle,
      body: preview,
      createdAt: FieldValue.serverTimestamp(),
      read: false,
    }, {merge: false});
  });

  await batch.commit();

  await Promise.all(recipients.map(async (uid) => {
    const tokensSnap = await db.collection("users").doc(uid).collection("pushTokens").get();
    if (tokensSnap.empty) return;
    const tokenEntries = tokensSnap.docs
        .map((docSnap) => ({token: docSnap.data()?.token, ref: docSnap.ref}))
        .filter((entry) => entry.token);
    if (!tokenEntries.length) return;
    const response = await admin.messaging().sendEachForMulticast({
      tokens: tokenEntries.map((entry) => entry.token),
      notification: {
        title: notificationTitle,
        body: preview,
      },
      data: {
        kind: "dm",
        conversationId,
        messageId,
        fromUid: senderId,
      },
    });
    const deletions = [];
    response.responses.forEach((res, idx) => {
      if (res.success) return;
      const code = res.error?.code || "";
      if (code === "messaging/invalid-registration-token" || code === "messaging/registration-token-not-registered") {
        deletions.push(tokenEntries[idx].ref.delete());
      }
    });
    if (deletions.length) {
      await Promise.all(deletions);
    }
  }));
});

exports.createUploadSession = onCall((data, context) => {
  if (!context.auth) {
    throw new HttpsError("unauthenticated", "Sign-in required.");
  }
  assertAppCheckV1(context);
  const uploadId = `${Date.now()}`;
  const storagePath = `videos/${context.auth.uid}/${uploadId}`;
  return {
    uploadId,
    storagePath,
    contentType: data?.type || null,
    size: data?.size || null,
  };
});

exports.createAsset = onCallV2({enforceAppCheck: true}, async (request) => {
  assertAppCheckV2(request);
  const auth = request.auth;
  if (!auth?.uid) throw new HttpsError("unauthenticated", "Sign-in required.");
  const data = request.data || {};
  const kind = String(data.kind || "").trim().toLowerCase();
  const visibility = String(data.visibility || "private").trim().toLowerCase();
  if (!ASSET_POLICIES[kind]) throw new HttpsError("invalid-argument", "Unsupported asset kind.");
  if (!["public", "followers", "private"].includes(visibility)) {
    throw new HttpsError("invalid-argument", "Invalid visibility.");
  }
  await enforceRateLimit(auth.uid, `asset:create:${kind}:60s`, RATE_LIMITS.assets);
  const assetRef = db.collection("assets").doc();
  const storagePathOriginal = `uploads/${auth.uid}/${assetRef.id}/original`;
  await assetRef.set({
    ownerId: auth.uid,
    createdAt: FieldValue.serverTimestamp(),
    status: "uploading",
    visibility,
    kind,
    storagePathOriginal,
    contentType: data.contentType || "",
    sizeBytes: Number(data.sizeBytes || 0) || 0,
    variants: {},
    moderation: {
      status: "pending",
      labels: [],
      scoreMap: {},
      modelVersion: "ai-logic",
    },
  }, {merge: true});
  return {assetId: assetRef.id, storagePathOriginal};
});

exports.getAssetDownloadUrl = onCallV2({enforceAppCheck: true}, async (request) => {
  assertAppCheckV2(request);
  const auth = request.auth;
  if (!auth?.uid) throw new HttpsError("unauthenticated", "Sign-in required.");
  const data = request.data || {};
  const assetId = String(data.assetId || "").trim();
  const variant = String(data.variant || "original").trim();
  if (!assetId) throw new HttpsError("invalid-argument", "assetId is required.");
  const assetSnap = await db.collection("assets").doc(assetId).get();
  if (!assetSnap.exists) throw new HttpsError("not-found", "Asset not found.");
  const asset = assetSnap.data() || {};
  const isOwner = asset.ownerId === auth.uid;
  const isStaff = isStaffClaims(auth);
  if (asset.moderation?.status === "blocked" && !isOwner && !isStaff) {
    throw new HttpsError("permission-denied", "Asset is blocked.");
  }
  const visibility = asset.visibility || "private";
  if (visibility === "private" && !isOwner && !isStaff) {
    throw new HttpsError("permission-denied", "Asset is private.");
  }
  if (visibility === "followers" && !isOwner && !isStaff) {
    const followerSnap = await db.collection("users").doc(asset.ownerId).collection("followers").doc(auth.uid).get();
    if (!followerSnap.exists) throw new HttpsError("permission-denied", "Asset is restricted.");
  }
  const path = variant === "original" ? asset.storagePathOriginal : asset.variants?.[variant];
  if (!path) throw new HttpsError("not-found", "Asset variant unavailable.");
  const expiresAt = Date.now() + 15 * 60 * 1000;
  const [url] = await admin.storage().bucket().file(path).getSignedUrl({
    action: "read",
    expires: expiresAt,
  });
  return {url, expiresAt};
});

exports.createPost = onCallV2({enforceAppCheck: true}, async (request) => {
  try {
    assertAppCheckV2(request);
    const auth = request.auth;
    if (!auth?.uid) throw new HttpsError("unauthenticated", "Sign-in required.");
    const data = request.data || {};
    const title = normalizeText(data.title || "", 180);
    const visibility = String(data.visibility || "public").trim().toLowerCase();
    if (!["public", "followers", "private"].includes(visibility)) {
      throw new HttpsError("invalid-argument", "Invalid visibility.");
    }
    const categoryId = String(data.categoryId || "").trim();
    const contentType = String(data.contentType || "").trim().toLowerCase();
    const blocks = normalizeBlocks(data.blocks || []);
    if (!blocks.length) throw new HttpsError("invalid-argument", "Post must include at least one block.");
    if (blocks.length > 20) throw new HttpsError("invalid-argument", "Too many blocks.");

    const assetIds = blocks.flatMap((block) => {
      if (block.type === "asset") return [block.assetId, block.thumbnailAssetId].filter(Boolean);
      if (block.type === "link") return [block.imageAssetId].filter(Boolean);
      return [];
    });
    const assetDetails = assetIds.length ? await assertAssetOwnership(assetIds, auth.uid) : {};

    for (const block of blocks) {
      if (block.type === "capsule") {
        const capsuleSnap = await db.collection("capsules").doc(block.capsuleId).get();
        if (!capsuleSnap.exists) throw new HttpsError("not-found", "Capsule not found.");
        const capsule = capsuleSnap.data() || {};
        if (capsule.ownerId !== auth.uid) throw new HttpsError("permission-denied", "Capsule ownership mismatch.");
      }
      if (block.type === "live") {
        const sessionSnap = await db.collection("liveSessions").doc(block.sessionId).get();
        if (!sessionSnap.exists) throw new HttpsError("not-found", "Live session not found.");
        const session = sessionSnap.data() || {};
        if (session.hostId !== auth.uid) throw new HttpsError("permission-denied", "Live session ownership mismatch.");
      }
    }

    let categoryPayload = {categoryId: null, categoryName: null, categorySlug: null, categoryType: null, categoryVerified: false};
    if (categoryId) {
      const categorySnap = await db.collection("categories").doc(categoryId).get();
      if (categorySnap.exists) {
        const category = categorySnap.data() || {};
        categoryPayload = {
          categoryId,
          categoryName: category.name || null,
          categorySlug: category.slug || null,
          categoryType: category.type || null,
          categoryVerified: !!category.verified,
        };
      }
    }

    const moderationText = collectModerationText(title, blocks);
    const moderation = await moderateTextContent(moderationText, "post");
    const authorName = await resolveActorProfile(auth.uid);

    const firstTextBlock = blocks.find((block) => block.type === "text");
    const firstAssetBlock = blocks.find((block) => block.type === "asset");
    const legacyMediaAssetId = firstAssetBlock?.assetId || "";
    const legacyMediaPath = legacyMediaAssetId ? (assetDetails[legacyMediaAssetId]?.storagePathOriginal || "") : "";

    const tags = Array.isArray(data.tags)
      ? data.tags.map((value) => (typeof value === "string" ? value.trim() : "")).filter(Boolean).slice(0, 10)
      : [];
    const mentions = Array.isArray(data.mentions)
      ? data.mentions
          .map((mention) => {
            if (!mention || typeof mention !== "object") return null;
            const uid = typeof mention.uid === "string" ? mention.uid.trim() : "";
            const username = typeof mention.username === "string" ? mention.username.trim() : "";
            if (!uid && !username) return null;
            return {
              ...(uid ? {uid} : {}),
              ...(username ? {username} : {}),
            };
          })
          .filter(Boolean)
          .slice(0, 10)
      : [];
    const mentionUserIds = Array.isArray(data.mentionUserIds)
      ? data.mentionUserIds.map((value) => (typeof value === "string" ? value.trim() : "")).filter(Boolean).slice(0, 10)
      : [];

    const preview = buildComposerPostPreview(blocks, title);
    const postPayload = {
      ownerId: auth.uid,
      userId: auth.uid,
      author: authorName.actorName || "User",
      title: title ?? "",
      visibility,
      contentType: contentType || (firstAssetBlock ? firstAssetBlock.presentation : "text"),
      blocks,
      ...categoryPayload,
      category: categoryPayload.categoryName || categoryPayload.categorySlug || "",
      tags,
      mentions,
      mentionUserIds,
      poll: data.poll || null,
      scheduledFor: data.scheduledFor || null,
      location: data.location || "",
      content: {
        text: firstTextBlock?.text || "",
        mediaPath: legacyMediaPath,
        linkUrl: null,
        profileUid: null,
        meta: {tags, mentions},
      },
      mediaAssetId: legacyMediaAssetId,
      mediaPath: legacyMediaPath,
      moderation: {
        status: moderation.status,
        labels: moderation.labels,
        scoreMap: moderation.scoreMap,
        modelVersion: moderation.modelVersion,
        reviewRequired: moderation.reviewRequired,
      },
      previewText: String(preview?.previewText ?? ""),
      previewAssetId: String(preview?.previewAssetId ?? ""),
      previewType: String(preview?.previewType ?? "text"),
      previewLink: String(preview?.previewLink ?? ""),
      createdAt: FieldValue.serverTimestamp(),
      timestamp: FieldValue.serverTimestamp(),
    };

    const sanitized = stripUndefined(postPayload);
    const postRef = db.collection("posts").doc();
    await postRef.set(sanitized);
    return {ok: true, postId: postRef.id, moderation: postPayload.moderation};
  } catch (err) {
    if (err instanceof HttpsError) throw err;
    logger.error("createPost payload snapshot", {
      uid: request?.auth?.uid,
      categoryId: request?.data?.categoryId,
      blocksCount: Array.isArray(request?.data?.blocks) ? request.data.blocks.length : 0,
      tagsType: typeof request?.data?.tags,
      mentionsType: typeof request?.data?.mentions,
      mentionUserIdsType: typeof request?.data?.mentionUserIds,
    });
    logger.error("createPost failed", {uid: request?.auth?.uid, err: err?.message, stack: err?.stack});
    throw new HttpsError("internal", "Post creation failed.");
  }
});

exports.createLinkSnapshot = onCallV2({enforceAppCheck: true}, async (request) => {
  assertAppCheckV2(request);
  const auth = request.auth;
  if (!auth?.uid) throw new HttpsError("unauthenticated", "Sign-in required.");
  const data = request.data || {};
  const url = normalizeUrl(data.url || "");
  if (!url) throw new HttpsError("invalid-argument", "Valid URL required.");
  const shouldDownloadImage = data.downloadImage === true;
  const visibility = String(data.visibility || "public").trim().toLowerCase();

  const response = await fetch(url, {method: "GET"});
  if (!response.ok) throw new HttpsError("failed-precondition", "Unable to fetch link metadata.");
  const html = await response.text();

  const title = extractMetaContent(html, 'property=["\']og:title["\']')
    || extractMetaContent(html, 'name=["\']twitter:title["\']')
    || extractTitleFromHtml(html);
  const description = extractMetaContent(html, 'property=["\']og:description["\']')
    || extractMetaContent(html, 'name=["\']description["\']')
    || extractMetaContent(html, 'name=["\']twitter:description["\']');
  const ogImage = extractMetaContent(html, 'property=["\']og:image["\']')
    || extractMetaContent(html, 'name=["\']twitter:image["\']');

  let imageAssetId = "";
  let imageUrl = normalizeUrl(ogImage);
  if (shouldDownloadImage && imageUrl) {
    const imageResponse = await fetch(imageUrl);
    if (imageResponse.ok) {
      const contentType = imageResponse.headers.get("content-type") || "image/jpeg";
      const buffer = Buffer.from(await imageResponse.arrayBuffer());
      const sizeBytes = buffer.length;
      const policyCheck = validateAssetPolicy("image", contentType, sizeBytes);
      if (policyCheck.ok) {
        const assetRef = db.collection("assets").doc();
        const storagePathOriginal = `uploads/${auth.uid}/${assetRef.id}/original`;
        await assetRef.set({
          ownerId: auth.uid,
          createdAt: FieldValue.serverTimestamp(),
          status: "uploading",
          visibility,
          kind: "image",
          storagePathOriginal,
          contentType,
          sizeBytes,
          variants: {},
          moderation: {
            status: "pending",
            labels: [],
            scoreMap: {},
            modelVersion: "ai-logic",
          },
        }, {merge: true});
        await admin.storage().bucket().file(storagePathOriginal).save(buffer, {contentType});
        imageAssetId = assetRef.id;
        imageUrl = "";
      }
    }
  }

  return {
    url,
    title: title || "",
    description: description || "",
    imageAssetId,
    imageUrl,
  };
});

exports.createCapsule = onCallV2({enforceAppCheck: true}, async (request) => {
  try {
    assertAppCheckV2(request);
    const auth = request.auth;
    if (!auth?.uid) throw new HttpsError("unauthenticated", "Sign-in required.");
    const data = request.data || {};
    const title = normalizeText(data.title || "", 200);
    const description = normalizeText(data.description || "", 2000);
    const visibility = String(data.visibility || "public").trim().toLowerCase();
    if (!["public", "followers", "private"].includes(visibility)) {
      throw new HttpsError("invalid-argument", "Invalid visibility.");
    }
    const assetIds = Array.isArray(data.assetIds) ? data.assetIds.filter(Boolean).slice(0, 100) : [];
    await assertAssetOwnership(assetIds, auth.uid);

    const capsuleRef = db.collection("capsules").doc();
    const shareEnabled = data?.share?.enabled === true;
    const requestedSlug = data?.share?.slug ? String(data.share.slug).trim() : "";
    const slug = requestedSlug || capsuleRef.id.slice(0, 10);
    const expiresAt = data?.share?.expiresAt || null;
    const capsulePayload = {
      ownerId: auth.uid,
      createdAt: FieldValue.serverTimestamp(),
      visibility,
      title: title ?? "",
      description: description ?? "",
      assetIds,
      version: 1,
      share: {
        enabled: shareEnabled,
        slug,
        expiresAt,
      },
    };
    await capsuleRef.set(stripUndefined(capsulePayload), {merge: true});

    const blocks = [
      ...(description ? [{type: "text", text: description}] : []),
      {type: "capsule", capsuleId: capsuleRef.id},
    ];
    const moderationText = collectModerationText(title, blocks);
    const moderation = await moderateTextContent(moderationText, "capsule");
    const authorName = await resolveActorProfile(auth.uid);
    const preview = buildComposerPostPreview(blocks, title);

    const postPayload = {
      ownerId: auth.uid,
      userId: auth.uid,
      author: authorName.actorName || "User",
      title: title ?? "",
      visibility,
      contentType: "capsule",
      blocks,
      moderation: {
        status: moderation.status,
        labels: moderation.labels,
        scoreMap: moderation.scoreMap,
        modelVersion: moderation.modelVersion,
        reviewRequired: moderation.reviewRequired,
      },
      previewText: String(preview?.previewText ?? ""),
      previewAssetId: String(preview?.previewAssetId ?? ""),
      previewType: String(preview?.previewType ?? "text"),
      previewLink: String(preview?.previewLink ?? ""),
      createdAt: FieldValue.serverTimestamp(),
      timestamp: FieldValue.serverTimestamp(),
    };
    const sanitized = stripUndefined(postPayload);
    const postRef = db.collection("posts").doc();
    await postRef.set(sanitized);

    return {ok: true, capsuleId: capsuleRef.id, postId: postRef.id, moderation};
  } catch (err) {
    if (err instanceof HttpsError) throw err;
    logger.error("createCapsule failed", {uid: request?.auth?.uid, err: err?.message, stack: err?.stack});
    throw new HttpsError("internal", "Post creation failed.");
  }
});

exports.createLiveSession = onCallV2({enforceAppCheck: true}, async (request) => {
  assertAppCheckV2(request);
  const auth = request.auth;
  if (!auth?.uid) throw new HttpsError("unauthenticated", "Sign-in required.");
  const data = request.data || {};
  const title = normalizeText(data.title || "", 160);
  const category = normalizeText(data.category || "", 80);
  const tags = Array.isArray(data.tags) ? data.tags.slice(0, 20) : [];
  const playbackUrl = normalizeUrl(data.playbackUrl || "");
  const roomName = `live_${auth.uid}_${Date.now()}`;
  const payload = {
    hostId: auth.uid,
    roomName,
    title,
    category,
    tags,
    visibility: "public",
    status: "live",
    startedAt: FieldValue.serverTimestamp(),
    endedAt: null,
    egressMode: "hls",
    playbackUrl: playbackUrl || null,
    createdAt: FieldValue.serverTimestamp(),
  };
  const sessionRef = await db.collection("liveSessions").add(payload);
  return {ok: true, sessionId: sessionRef.id, roomName};
});

exports.createComment = onCallV2({enforceAppCheck: true}, async (request) => {
  assertAppCheckV2(request);
  const auth = request.auth;
  if (!auth?.uid) throw new HttpsError("unauthenticated", "Sign-in required.");
  const data = request.data || {};
  const postId = String(data.postId || "").trim();
  const text = normalizeText(data.text || "", 1200);
  const assetIds = Array.isArray(data.assetIds) ? data.assetIds.filter(Boolean).slice(0, 3) : [];
  if (!postId || !text) throw new HttpsError("invalid-argument", "postId and text are required.");
  await enforceRateLimit(auth.uid, `comment:${postId}:30s`, RATE_LIMITS.comments);

  const postRef = db.collection("posts").doc(postId);
  const postSnap = await postRef.get();
  if (!postSnap.exists) throw new HttpsError("not-found", "Post not found.");
  const postData = postSnap.data() || {};
  if (postData.visibility === "private" && postData.userId !== auth.uid) {
    throw new HttpsError("permission-denied", "You cannot comment on this post.");
  }

  let mediaAssetId = "";
  let mediaPath = "";
  if (assetIds.length) {
    const assetSnap = await db.collection("assets").doc(assetIds[0]).get();
    if (!assetSnap.exists) throw new HttpsError("not-found", "Asset not found.");
    const asset = assetSnap.data() || {};
    if (asset.ownerId !== auth.uid) throw new HttpsError("permission-denied", "Asset ownership mismatch.");
    mediaAssetId = assetIds[0];
    mediaPath = asset.storagePathOriginal || "";
  }
  const moderation = await moderateTextContent(text, "comment");
  let authorName = "User";
  try {
    const userSnap = await db.collection("users").doc(auth.uid).get();
    if (userSnap.exists) {
      const userData = userSnap.data() || {};
      authorName = userData.name || userData.displayName || userData.username || authorName;
    }
  } catch (error) {}
  const payload = {
    userId: auth.uid,
    text,
    mediaAssetId,
    mediaPath,
    assets: assetIds,
    createdAt: FieldValue.serverTimestamp(),
    timestamp: FieldValue.serverTimestamp(),
    moderation: {
      status: moderation.status,
      labels: moderation.labels,
      scoreMap: moderation.scoreMap,
      modelVersion: moderation.modelVersion,
      reviewRequired: moderation.reviewRequired,
    },
  };

  const commentRef = await postRef.collection("comments").add(payload);
  if (moderation.status !== "blocked") {
    await postRef.set({
      previewComment: {
        text: text.substring(0, 80) + (text.length > 80 ? "..." : ""),
        author: authorName,
        likes: 0,
      },
    }, {merge: true});
  }
  return {ok: true, commentId: commentRef.id, moderation: payload.moderation};
});

exports.sendLiveChatMessage = onCallV2({enforceAppCheck: true}, async (request) => {
  assertAppCheckV2(request);
  const auth = request.auth;
  if (!auth?.uid) throw new HttpsError("unauthenticated", "Sign-in required.");
  const data = request.data || {};
  const sessionId = String(data.sessionId || "").trim();
  const text = normalizeText(data.text || "", 500);
  if (!sessionId || !text) throw new HttpsError("invalid-argument", "sessionId and text are required.");
  await enforceRateLimit(auth.uid, `livechat:${sessionId}:10s`, RATE_LIMITS.liveChat);

  const sessionRef = db.collection("liveSessions").doc(sessionId);
  const sessionSnap = await sessionRef.get();
  if (!sessionSnap.exists) throw new HttpsError("not-found", "Session not found.");

  const moderation = await moderateTextContent(text, "livechat");
  let displayName = auth.token?.name || auth.token?.displayName || "";
  if (!displayName) {
    try {
      const userSnap = await db.collection("users").doc(auth.uid).get();
      const userData = userSnap.exists ? userSnap.data() : {};
      displayName = userData?.displayName || userData?.name || userData?.username || auth.uid;
    } catch (error) {
      displayName = auth.uid;
    }
  }
  const payload = {
    senderId: auth.uid,
    displayName,
    text,
    createdAt: FieldValue.serverTimestamp(),
    moderation: {
      status: moderation.status,
      labels: moderation.labels,
      scoreMap: moderation.scoreMap,
      modelVersion: moderation.modelVersion,
      reviewRequired: moderation.reviewRequired,
    },
  };

  const chatRef = await sessionRef.collection("chat").add(payload);
  return {ok: true, chatId: chatRef.id, moderation: payload.moderation};
});

exports.createReview = onCallV2({enforceAppCheck: false}, async (request) => {
  assertAppCheckV2(request);
  const auth = request.auth;
  if (!auth?.uid) throw new HttpsError("unauthenticated", "Sign-in required.");
  const data = request.data || {};
  const postId = String(data.postId || "").trim();
  const ratingValue = normalizeReviewRating(data.rating);
  const note = normalizeText(data.text || "", 2000);
  if (!postId || !note) throw new HttpsError("invalid-argument", "postId and text are required.");
  if (!Number.isInteger(ratingValue) || ratingValue < 1 || ratingValue > 5) {
    throw new HttpsError("invalid-argument", "Invalid rating.");
  }
  logger.info("[createReview]", {uid: auth.uid, hasApp: !!request.app, postId});
  await enforceRateLimit(auth.uid, `review:${postId}:10m`, RATE_LIMITS.reviews);

  const postRef = db.collection("posts").doc(postId);
  const moderation = await moderateTextContent(note, "review");
  await db.runTransaction(async (tx) => {
    const reviewRef = postRef.collection("reviews").doc(auth.uid);
    const [postSnap, reviewSnap] = await Promise.all([
      tx.get(postRef),
      tx.get(reviewRef),
    ]);
    if (!postSnap.exists) throw new HttpsError("not-found", "Post not found.");
    const existingRatingValue = normalizeReviewRating(reviewSnap.exists ? reviewSnap.data()?.rating : null);
    const previousScore = getReviewScoreDelta(existingRatingValue);
    const nextScore = getReviewScoreDelta(ratingValue);
    const scoreDelta = reviewSnap.exists ? (nextScore - previousScore) : nextScore;
    const createdAt = reviewSnap.exists ? (reviewSnap.data()?.createdAt || FieldValue.serverTimestamp()) : FieldValue.serverTimestamp();
    tx.set(reviewRef, {
      userId: auth.uid,
      rating: ratingValue,
      note,
      text: note,
      createdAt,
      updatedAt: FieldValue.serverTimestamp(),
      timestamp: FieldValue.serverTimestamp(),
      moderation: {
        status: moderation.status,
        labels: moderation.labels,
        scoreMap: moderation.scoreMap,
        modelVersion: moderation.modelVersion,
        reviewRequired: moderation.reviewRequired,
      },
    }, {merge: true});
    if (scoreDelta !== 0) {
      tx.update(postRef, {trustScore: FieldValue.increment(scoreDelta)});
    }
  });
  return {ok: true, reviewId: auth.uid, moderation};
});

exports.createReview_v2 = onCallV2({enforceAppCheck: false}, async (request) => {
  assertAppCheckV2(request);
  const auth = request.auth;
  if (!auth?.uid) throw new HttpsError("unauthenticated", "Sign-in required.");
  const data = request.data || {};
  const postId = String(data.postId || "").trim();
  const ratingValue = normalizeReviewRating(data.rating);
  const text = normalizeText(data.text || "", 2000);
  if (!postId || !text) throw new HttpsError("invalid-argument", "postId and text are required.");
  if (!Number.isInteger(ratingValue) || ratingValue < 1 || ratingValue > 5) {
    throw new HttpsError("invalid-argument", "Invalid rating.");
  }
  logger.info("[createReview_v2]", {uid: auth.uid, hasApp: !!request.app, postId});
  await enforceRateLimit(auth.uid, `review:${postId}:10m`, RATE_LIMITS.reviews);

  const moderation = await moderateTextContent(text, "review");
  const postRef = db.collection("posts").doc(postId);
  let response = null;
  await db.runTransaction(async (tx) => {
    const reviewRef = postRef.collection("reviews").doc(auth.uid);
    const [postSnap, reviewSnap] = await Promise.all([
      tx.get(postRef),
      tx.get(reviewRef),
    ]);
    if (!postSnap.exists) throw new HttpsError("not-found", "Post not found.");
    const postData = postSnap.data() || {};
    let reviewCount = Number.isFinite(postData.reviewCount) ? postData.reviewCount : 0;
    let ratingSum = Number.isFinite(postData.ratingSum) ? postData.ratingSum : 0;
    const previousRating = normalizeReviewRating(reviewSnap.exists ? reviewSnap.data()?.rating : null);
    if (Number.isInteger(previousRating)) {
      ratingSum -= previousRating;
    }
    ratingSum += ratingValue;
    if (!reviewSnap.exists) reviewCount += 1;
    const createdAt = reviewSnap.exists ? (reviewSnap.data()?.createdAt || FieldValue.serverTimestamp()) : FieldValue.serverTimestamp();
    const displayName = auth.token?.name || auth.token?.displayName || "";
    const photoURL = auth.token?.picture || auth.token?.photoURL || "";
    tx.set(reviewRef, {
      userId: auth.uid,
      rating: ratingValue,
      text,
      note: text,
      displayName,
      photoURL,
      createdAt,
      updatedAt: FieldValue.serverTimestamp(),
      timestamp: FieldValue.serverTimestamp(),
      moderation: {
        status: moderation.status,
        labels: moderation.labels,
        scoreMap: moderation.scoreMap,
        modelVersion: moderation.modelVersion,
        reviewRequired: moderation.reviewRequired,
      },
    }, {merge: true});
    tx.update(postRef, {reviewCount, ratingSum});
    response = {reviewCount, ratingSum};
  });
  return {ok: true, reviewId: auth.uid, moderation, ...response};
});

exports.removeReview = onCallV2({enforceAppCheck: false}, async (request) => {
  assertAppCheckV2(request);
  const auth = request.auth;
  if (!auth?.uid) throw new HttpsError("unauthenticated", "Sign-in required.");
  const data = request.data || {};
  const postId = String(data.postId || "").trim();
  if (!postId) throw new HttpsError("invalid-argument", "postId is required.");
  await enforceRateLimit(auth.uid, `review:${postId}:10m`, RATE_LIMITS.reviews);

  const postRef = db.collection("posts").doc(postId);
  let removed = false;
  await db.runTransaction(async (tx) => {
    const reviewRef = postRef.collection("reviews").doc(auth.uid);
    const reviewSnap = await tx.get(reviewRef);
    if (!reviewSnap.exists) return;
    const ratingValue = normalizeReviewRating(reviewSnap.data()?.rating);
    const scoreChange = getReviewScoreDelta(ratingValue);
    tx.delete(reviewRef);
    if (scoreChange !== 0) {
      tx.update(postRef, {trustScore: FieldValue.increment(-scoreChange)});
    }
    removed = true;
  });
  return {ok: true, removed};
});

exports.toggleLike = onCallV2({enforceAppCheck: false}, async (request) => {
  assertAppCheckV2(request);
  const auth = request.auth;
  if (!auth?.uid) throw new HttpsError("unauthenticated", "Sign-in required.");
  const data = request.data || {};
  const postId = String(data.postId || "").trim();
  const action = String(data.action || "").trim();
  if (!postId || !["like", "unlike"].includes(action)) {
    throw new HttpsError("invalid-argument", "Invalid action.");
  }
  await enforceRateLimit(auth.uid, `like:${postId}:60s`, RATE_LIMITS.likes);

  const postRef = db.collection("posts").doc(postId);
  const likeRef = postRef.collection("likes").doc(auth.uid);
  const dislikeRef = postRef.collection("dislikes").doc(auth.uid);
  await db.runTransaction(async (tx) => {
    const [postSnap, likeSnap, dislikeSnap] = await Promise.all([
      tx.get(postRef),
      tx.get(likeRef),
      tx.get(dislikeRef),
    ]);
    if (!postSnap.exists) throw new HttpsError("not-found", "Post not found.");
    const postData = postSnap.data() || {};
    let likeCount = Number.isFinite(postData.likeCount) ? postData.likeCount : (postData.likes || 0);
    let dislikeCount = Number.isFinite(postData.dislikeCount) ? postData.dislikeCount : (postData.dislikes || 0);

    const hasLike = likeSnap.exists;
    const hasDislike = dislikeSnap.exists;
    if (action === "like" && !hasLike) {
      tx.set(likeRef, {createdAt: FieldValue.serverTimestamp()});
      likeCount += 1;
      if (hasDislike) {
        tx.delete(dislikeRef);
        dislikeCount = Math.max(0, dislikeCount - 1);
      }
    } else if (action === "unlike" && hasLike) {
      tx.delete(likeRef);
      likeCount = Math.max(0, likeCount - 1);
    }
    tx.update(postRef, {likeCount, dislikeCount});
  });
  return {ok: true};
});

exports.toggleDislike = onCallV2({enforceAppCheck: false}, async (request) => {
  assertAppCheckV2(request);
  const auth = request.auth;
  if (!auth?.uid) throw new HttpsError("unauthenticated", "Sign-in required.");
  const data = request.data || {};
  const postId = String(data.postId || "").trim();
  const action = String(data.action || "").trim();
  if (!postId || !["dislike", "undislike"].includes(action)) {
    throw new HttpsError("invalid-argument", "Invalid action.");
  }
  await enforceRateLimit(auth.uid, `dislike:${postId}:60s`, RATE_LIMITS.likes);

  const postRef = db.collection("posts").doc(postId);
  const dislikeRef = postRef.collection("dislikes").doc(auth.uid);
  const likeRef = postRef.collection("likes").doc(auth.uid);
  await db.runTransaction(async (tx) => {
    const [postSnap, dislikeSnap, likeSnap] = await Promise.all([
      tx.get(postRef),
      tx.get(dislikeRef),
      tx.get(likeRef),
    ]);
    if (!postSnap.exists) throw new HttpsError("not-found", "Post not found.");
    const postData = postSnap.data() || {};
    let likeCount = Number.isFinite(postData.likeCount) ? postData.likeCount : (postData.likes || 0);
    let dislikeCount = Number.isFinite(postData.dislikeCount) ? postData.dislikeCount : (postData.dislikes || 0);
    const hasDislike = dislikeSnap.exists;
    const hasLike = likeSnap.exists;

    if (action === "dislike" && !hasDislike) {
      tx.set(dislikeRef, {createdAt: FieldValue.serverTimestamp()});
      dislikeCount += 1;
      if (hasLike) {
        tx.delete(likeRef);
        likeCount = Math.max(0, likeCount - 1);
      }
    } else if (action === "undislike" && hasDislike) {
      tx.delete(dislikeRef);
      dislikeCount = Math.max(0, dislikeCount - 1);
    }
    tx.update(postRef, {likeCount, dislikeCount});
  });
  return {ok: true};
});

exports.toggleLike_v2 = onCallV2({enforceAppCheck: false}, async (request) => {
  assertAppCheckV2(request);
  const auth = request.auth;
  if (!auth?.uid) throw new HttpsError("unauthenticated", "Sign-in required.");
  const data = request.data || {};
  const postId = String(data.postId || "").trim();
  const action = String(data.action || "").trim();
  if (!postId || !["like", "unlike"].includes(action)) {
    throw new HttpsError("invalid-argument", "Invalid action.");
  }
  logger.info("[toggleLike_v2]", {uid: auth.uid, hasApp: !!request.app, postId});
  await enforceRateLimit(auth.uid, `like:${postId}:60s`, RATE_LIMITS.likes);

  const postRef = db.collection("posts").doc(postId);
  const likeRef = postRef.collection("likes").doc(auth.uid);
  const dislikeRef = postRef.collection("dislikes").doc(auth.uid);
  let response = null;
  await db.runTransaction(async (tx) => {
    const [postSnap, likeSnap, dislikeSnap] = await Promise.all([
      tx.get(postRef),
      tx.get(likeRef),
      tx.get(dislikeRef),
    ]);
    if (!postSnap.exists) throw new HttpsError("not-found", "Post not found.");
    const postData = postSnap.data() || {};
    let likeCount = Number.isFinite(postData.likeCount) ? postData.likeCount : (postData.likes || 0);
    let dislikeCount = Number.isFinite(postData.dislikeCount) ? postData.dislikeCount : (postData.dislikes || 0);

    let liked = likeSnap.exists;
    let disliked = dislikeSnap.exists;
    if (action === "like" && !liked) {
      tx.set(likeRef, {createdAt: FieldValue.serverTimestamp()});
      likeCount += 1;
      liked = true;
      if (disliked) {
        tx.delete(dislikeRef);
        dislikeCount = Math.max(0, dislikeCount - 1);
        disliked = false;
      }
    } else if (action === "unlike" && liked) {
      tx.delete(likeRef);
      likeCount = Math.max(0, likeCount - 1);
      liked = false;
    }
    tx.update(postRef, {likeCount, dislikeCount});
    response = {liked, disliked, likeCount, dislikeCount};
  });
  return {ok: true, ...response};
});

exports.toggleDislike_v2 = onCallV2({enforceAppCheck: false}, async (request) => {
  assertAppCheckV2(request);
  const auth = request.auth;
  if (!auth?.uid) throw new HttpsError("unauthenticated", "Sign-in required.");
  const data = request.data || {};
  const postId = String(data.postId || "").trim();
  const action = String(data.action || "").trim();
  if (!postId || !["dislike", "undislike"].includes(action)) {
    throw new HttpsError("invalid-argument", "Invalid action.");
  }
  logger.info("[toggleDislike_v2]", {uid: auth.uid, hasApp: !!request.app, postId});
  await enforceRateLimit(auth.uid, `dislike:${postId}:60s`, RATE_LIMITS.likes);

  const postRef = db.collection("posts").doc(postId);
  const dislikeRef = postRef.collection("dislikes").doc(auth.uid);
  const likeRef = postRef.collection("likes").doc(auth.uid);
  let response = null;
  await db.runTransaction(async (tx) => {
    const [postSnap, dislikeSnap, likeSnap] = await Promise.all([
      tx.get(postRef),
      tx.get(dislikeRef),
      tx.get(likeRef),
    ]);
    if (!postSnap.exists) throw new HttpsError("not-found", "Post not found.");
    const postData = postSnap.data() || {};
    let likeCount = Number.isFinite(postData.likeCount) ? postData.likeCount : (postData.likes || 0);
    let dislikeCount = Number.isFinite(postData.dislikeCount) ? postData.dislikeCount : (postData.dislikes || 0);
    let disliked = dislikeSnap.exists;
    let liked = likeSnap.exists;

    if (action === "dislike" && !disliked) {
      tx.set(dislikeRef, {createdAt: FieldValue.serverTimestamp()});
      dislikeCount += 1;
      disliked = true;
      if (liked) {
        tx.delete(likeRef);
        likeCount = Math.max(0, likeCount - 1);
        liked = false;
      }
    } else if (action === "undislike" && disliked) {
      tx.delete(dislikeRef);
      dislikeCount = Math.max(0, dislikeCount - 1);
      disliked = false;
    }
    tx.update(postRef, {likeCount, dislikeCount});
    response = {liked, disliked, likeCount, dislikeCount};
  });
  return {ok: true, ...response};
});

exports.createComment_v2 = onCallV2({enforceAppCheck: false}, async (request) => {
  assertAppCheckV2(request);
  const auth = request.auth;
  if (!auth?.uid) throw new HttpsError("unauthenticated", "Sign-in required.");
  const data = request.data || {};
  const postId = String(data.postId || "").trim();
  const text = normalizeText(data.text || "", 2000);
  const parentIdRaw = String(data.parentId || "").trim();
  const parentId = parentIdRaw || null;
  const assetIds = Array.isArray(data.assetIds) ? data.assetIds.filter(Boolean).slice(0, 3) : [];
  if (!postId || !text) throw new HttpsError("invalid-argument", "postId and text are required.");
  logger.info("[createComment_v2]", {uid: auth.uid, hasApp: !!request.app, postId});
  await enforceRateLimit(auth.uid, `comment:${postId}:30s`, RATE_LIMITS.comments);

  const postRef = db.collection("posts").doc(postId);
  const postSnap = await postRef.get();
  if (!postSnap.exists) throw new HttpsError("not-found", "Post not found.");
  const postData = postSnap.data() || {};
  if (postData.visibility === "private" && postData.userId !== auth.uid) {
    throw new HttpsError("permission-denied", "You cannot comment on this post.");
  }

  let mediaAssetId = "";
  let mediaPath = "";
  if (assetIds.length) {
    const assetSnap = await db.collection("assets").doc(assetIds[0]).get();
    if (!assetSnap.exists) throw new HttpsError("not-found", "Asset not found.");
    const asset = assetSnap.data() || {};
    if (asset.ownerId !== auth.uid) throw new HttpsError("permission-denied", "Asset ownership mismatch.");
    mediaAssetId = assetIds[0];
    mediaPath = asset.storagePathOriginal || "";
  }
  const moderation = await moderateTextContent(text, "comment");
  let displayName = auth.token?.name || auth.token?.displayName || "";
  let photoURL = auth.token?.picture || auth.token?.photoURL || "";
  try {
    const userSnap = await db.collection("users").doc(auth.uid).get();
    if (userSnap.exists) {
      const userData = userSnap.data() || {};
      displayName = displayName || userData.name || userData.displayName || userData.username || "User";
      photoURL = photoURL || userData.photoURL || "";
    }
  } catch (error) {}
  if (!displayName) displayName = "User";
  const payload = {
    userId: auth.uid,
    text,
    parentId,
    displayName,
    photoURL,
    mediaAssetId,
    mediaPath,
    assets: assetIds,
    createdAt: FieldValue.serverTimestamp(),
    timestamp: FieldValue.serverTimestamp(),
    moderation: {
      status: moderation.status,
      labels: moderation.labels,
      scoreMap: moderation.scoreMap,
      modelVersion: moderation.modelVersion,
      reviewRequired: moderation.reviewRequired,
    },
  };

  const commentRef = postRef.collection("comments").doc();
  await db.runTransaction(async (tx) => {
    const postSnap = await tx.get(postRef);
    if (!postSnap.exists) throw new HttpsError("not-found", "Post not found.");
    tx.set(commentRef, payload);
    const updatePayload = {
      commentCount: FieldValue.increment(1),
    };
    if (moderation.status !== "blocked") {
      updatePayload.previewComment = {
        text: text.substring(0, 80) + (text.length > 80 ? "..." : ""),
        author: displayName || "User",
        likes: 0,
      };
    }
    tx.set(postRef, updatePayload, {merge: true});
  });
  return {ok: true, commentId: commentRef.id, createdAt: admin.firestore.Timestamp.now()};
});

exports.adminModerateContent = onCallV2({enforceAppCheck: true}, async (request) => {
  assertAppCheckV2(request);
  const auth = request.auth;
  if (!auth?.uid) throw new HttpsError("unauthenticated", "Sign-in required.");
  if (!isStaffClaims(auth)) throw new HttpsError("permission-denied", "Admin privileges required.");
  const data = request.data || {};
  const path = String(data.path || "").trim();
  const status = String(data.status || "").trim();
  if (!path || !["approved", "pending", "blocked"].includes(status)) {
    throw new HttpsError("invalid-argument", "Invalid moderation request.");
  }
  const docRef = db.doc(path);
  await docRef.set({
    moderation: {
      status,
      reviewedBy: auth.uid,
      reviewedAt: FieldValue.serverTimestamp(),
    },
  }, {merge: true});
  return {ok: true};
});

exports.adminBackfillPendingPosts = onCallV2({enforceAppCheck: true}, async (request) => {
  assertAppCheckV2(request);
  const auth = request.auth;
  if (!auth?.uid) throw new HttpsError("unauthenticated", "Sign-in required.");
  if (!isStaffClaims(auth)) throw new HttpsError("permission-denied", "Admin privileges required.");
  const data = request.data || {};
  const days = Math.min(Math.max(Number(data.days) || 14, 1), 90);
  const maxUpdates = Math.min(Math.max(Number(data.limit) || 200, 1), 500);
  const since = admin.firestore.Timestamp.fromMillis(Date.now() - days * 24 * 60 * 60 * 1000);

  const snapshot = await db.collection("posts")
    .where("moderation.status", "==", "pending")
    .where("createdAt", ">=", since)
    .orderBy("createdAt", "desc")
    .limit(maxUpdates)
    .get();

  if (snapshot.empty) return {ok: true, updated: 0};

  const batch = db.batch();
  let updated = 0;
  snapshot.forEach((docSnap) => {
    const postData = docSnap.data() || {};
    const moderation = postData.moderation || {};
    const labels = Array.isArray(moderation.labels) ? moderation.labels : [];
    const nextLabels = Array.from(new Set([...labels, "backfilled"]));
    batch.set(docSnap.ref, {
      moderation: {
        ...moderation,
        status: "approved",
        labels: nextLabels,
        reviewRequired: false,
        reviewedAt: FieldValue.serverTimestamp(),
        reviewedBy: auth.uid,
      },
    }, {merge: true});
    updated += 1;
  });

  await batch.commit();
  return {ok: true, updated};
});

exports.adminSetUserDisabled = onCallV2({enforceAppCheck: true}, async (request) => {
  assertAppCheckV2(request);
  const auth = request.auth;
  if (!auth?.uid) throw new HttpsError("unauthenticated", "Sign-in required.");
  if (!isStaffClaims(auth)) throw new HttpsError("permission-denied", "Admin privileges required.");
  const data = request.data || {};
  const targetUid = String(data.uid || "").trim();
  const disabled = data.disabled === true;
  if (!targetUid) throw new HttpsError("invalid-argument", "uid is required.");
  await admin.auth().updateUser(targetUid, {disabled});
  await db.collection("users").doc(targetUid).set({
    disabled,
    disabledAt: FieldValue.serverTimestamp(),
    disabledBy: auth.uid,
  }, {merge: true});
  return {ok: true};
});

exports.onAssetFinalize = onObjectFinalized(async (event) => {
    const object = event.data;
    const filePath = object.name || "";
    if (!filePath) return;
    const match = filePath.match(/^uploads\/([^/]+)\/([^/]+)\/original$/);
    if (!match) return;
    const [, uid, assetId] = match;
    const assetRef = db.collection("assets").doc(assetId);
    const assetSnap = await assetRef.get();
    if (!assetSnap.exists) {
      await admin.storage().bucket().file(filePath).delete().catch(() => {});
      return;
    }
    const asset = assetSnap.data() || {};
    if (asset.ownerId !== uid) {
      await admin.storage().bucket().file(filePath).delete().catch(() => {});
      return;
    }
    const contentType = object.contentType || "";
    const sizeBytes = object.size ? Number(object.size) : 0;
    const kind = asset.kind || "document";
    const policyCheck = validateAssetPolicy(kind, contentType, sizeBytes);
    if (!policyCheck.ok) {
      await assetRef.set({
        status: "blocked",
        contentType,
        sizeBytes,
        moderation: {
          status: "blocked",
          labels: [policyCheck.reason],
          scoreMap: {},
          modelVersion: "policy",
          reviewRequired: true,
        },
      }, {merge: true});
      await admin.storage().bucket().file(filePath).delete().catch(() => {});
      return;
    }

    await assetRef.set({
      status: "processing",
      contentType,
      sizeBytes,
      storagePathOriginal: filePath,
    }, {merge: true});

    const moderation = await moderateAssetContent({
      contentType,
      sizeBytes,
      path: filePath,
      kind,
    });
    if (moderation.status === "blocked") {
      await assetRef.set({
        status: "blocked",
        moderation,
      }, {merge: true});
      await admin.storage().bucket().file(filePath).delete().catch(() => {});
      return;
    }

    await assetRef.set({
      status: "ready",
      moderation,
      variants: asset.variants || {},
    }, {merge: true});
  }
);

exports.notifyMention = onCallV2({enforceAppCheck: true}, async (request) => {
  assertAppCheckV2(request);
  const data = request.data || {};
  const auth = request.auth;
  if (!auth || !auth.uid) throw new HttpsError("unauthenticated", "Sign-in required.");

  const actorId = auth.uid;
  const targetUserId = (data.targetUserId || "").toString();
  const postId = (data.postId || "").toString();
  const handle = (data.handle || "").toString();
  const postTitle = (data.postTitle || "").toString();
  const thumbnailUrl = (data.thumbnailUrl || "").toString();

  if (!targetUserId || !postId) throw new Error("invalid-argument");
  if (targetUserId === actorId) return {ok: true, skipped: "self"};

  let actorName = "Someone";
  let actorPhotoUrl = "";
  try {
    const actorDoc = await db.collection("users").doc(actorId).get();
    if (actorDoc.exists) {
      const userData = actorDoc.data() || {};
      actorName = userData.name || userData.displayName || userData.username || actorName;
      actorPhotoUrl = userData.photoURL || userData.photoUrl || "";
    }
  } catch (error) {}

  const docId = `mention_${actorId}_${postId}`;

  await db.collection("users")
      .doc(targetUserId)
      .collection("notifications")
      .doc(docId)
      .set({
        actorId,
        actorName,
        actorPhotoUrl,
        targetUserId,
        contentId: postId,
        contentType: "post",
        actionType: "mention",
        contentTitle: postTitle || "Post",
        contentThumbnailUrl: thumbnailUrl || "",
        previewText: handle ? `@${handle}` : "",
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        isRead: false,
      }, {merge: false});

  return {ok: true};
});

// DEV NOTE: Gen2 functions require the underlying Cloud Run service to be publicly invokable
// (allUsers roles/run.invoker) or the browser will fail with CORS before reaching the function.
exports.livekitCreateToken = onCallV2(
  {
    secrets: ["LIVEKIT_API_KEY", "LIVEKIT_API_SECRET", "LIVEKIT_URL"],
    cors: [
      "https://spike-streaming-service.web.app",
      "https://spike-streaming-service.firebaseapp.com",
    ],
    enforceAppCheck: true,
  },
  async (request) => {
    assertAppCheckV2(request);
    const auth = request.auth;
    if (!auth || !auth.uid) throw new HttpsError("unauthenticated", "Sign-in required.");
    const data = request.data || {};
    const roomName = String(data?.roomName || "").trim();
    const sessionId = String(data?.sessionId || "").trim();
    const canPublish = data?.canPublish !== false;
    let conversationId = String(data?.conversationId || "").trim();
    if (!conversationId && data?.metadata) {
      try {
        const parsed = JSON.parse(data.metadata);
        conversationId = String(parsed?.conversationId || "").trim();
      } catch (error) {}
    }
    if (!roomName || (!conversationId && !sessionId)) {
      throw new HttpsError("invalid-argument", "roomName and conversationId/sessionId are required.");
    }

    const apiKey = LIVEKIT_API_KEY.value();
    const apiSecret = LIVEKIT_API_SECRET.value();
    const livekitUrl = LIVEKIT_URL.value();
    if (!apiKey || !apiSecret || !livekitUrl) {
      throw new HttpsError("failed-precondition", "LiveKit is not configured.");
    }

    let displayName = auth.token?.name || auth.token?.displayName || "";
    if (!displayName) {
      const userSnap = await db.collection("users").doc(auth.uid).get();
      const userData = userSnap.exists ? userSnap.data() : {};
      displayName = userData?.displayName || userData?.name || userData?.username || auth.uid;
    }

    const token = new AccessToken(apiKey, apiSecret, {
      identity: auth.uid,
      name: displayName,
    });
    token.addGrant({room: roomName, roomJoin: true, canPublish, canSubscribe: true});

    return {
      url: livekitUrl,
      token: await token.toJwt(),
      roomName,
      conversationId: conversationId || null,
      sessionId: sessionId || null,
    };
  },
);
