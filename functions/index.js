/**
 * Import function triggers from their respective submodules:
 *
 * const {onCall} = require("firebase-functions/v2/https");
 * const {onDocumentWritten} = require("firebase-functions/v2/firestore");
 *
 * See a full list of supported triggers at https://firebase.google.com/docs/functions
 */

const {setGlobalOptions} = require("firebase-functions");
const {onCall: onCallV2} = require("firebase-functions/v2/https");
const {onCall, onRequest} = require("firebase-functions/https");
const {onDocumentCreated, onDocumentDeleted, onDocumentUpdated} = require("firebase-functions/v2/firestore");
const logger = require("firebase-functions/logger");
const admin = require("firebase-admin");

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
setGlobalOptions({ maxInstances: 10 });

// Create and deploy your first functions
// https://firebase.google.com/docs/functions/get-started

// exports.helloWorld = onRequest((request, response) => {
//   logger.info("Hello logs!", {structuredData: true});
//   response.send("Hello from Firebase!");
// });

const ivs = require("./ivs.js");
const uploads = require("./uploads.js");

exports.initializeUserChannel = ivs.initializeUserChannel;
exports.createEphemeralChannel = ivs.createEphemeralChannel;
exports.generatePlaybackToken = ivs.generatePlaybackToken;

if (!admin.apps.length) {
  admin.initializeApp();
}

const db = admin.firestore();
const {FieldValue} = admin.firestore;

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

function buildPostPreview(post = {}) {
  const title = post.title || post.content || post.body || post.text || "";
  const preview = typeof title === "string" ? title.slice(0, 80) : "";
  return {
    title: preview,
    thumbnail: post.mediaUrl || post.thumbnailUrl || post.thumbnail || "",
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

exports.onPostReaction = onDocumentUpdated("posts/{postId}", async (event) => {
  const before = event.data?.before?.data() || {};
  const after = event.data?.after?.data() || {};
  const ownerId = after.userId || before.userId;
  if (!ownerId) return;

  const beforeLiked = new Set(Array.isArray(before.likedBy) ? before.likedBy : []);
  const afterLiked = new Set(Array.isArray(after.likedBy) ? after.likedBy : []);
  const addedLikes = [...afterLiked].filter((uid) => !beforeLiked.has(uid));

  const beforeDisliked = new Set(Array.isArray(before.dislikedBy) ? before.dislikedBy : []);
  const afterDisliked = new Set(Array.isArray(after.dislikedBy) ? after.dislikedBy : []);
  const addedDislikes = [...afterDisliked].filter((uid) => !beforeDisliked.has(uid));

  if (!addedLikes.length && !addedDislikes.length) return;

  const preview = buildPostPreview(after);
  const tasks = [];
  addedLikes.forEach((actorId) => tasks.push(createContentNotification({
    actorId,
    targetUserId: ownerId,
    contentId: event.params.postId,
    contentType: "post",
    actionType: "like",
    contentTitle: preview.title,
    contentThumbnailUrl: preview.thumbnail,
  })));
  addedDislikes.forEach((actorId) => tasks.push(createContentNotification({
    actorId,
    targetUserId: ownerId,
    contentId: event.params.postId,
    contentType: "post",
    actionType: "dislike",
    contentTitle: preview.title,
    contentThumbnailUrl: preview.thumbnail,
  })));
  await Promise.all(tasks);
});

exports.onPostCommentNotification = onDocumentCreated("posts/{postId}/comments/{commentId}", async (event) => {
  const comment = event.data?.data() || {};
  const actorId = comment.userId;
  if (!actorId) return;
  const postId = event.params.postId;
  const postSnap = await db.collection("posts").doc(postId).get();
  const postData = postSnap.exists ? postSnap.data() : null;
  if (!postData || !postData.userId) return;
  const preview = buildPostPreview(postData);
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

exports.createUploadSession = onCall((data, context) => {
  if (!context.auth) {
    throw new Error("Unauthorized");
  }
  const uploadId = `${Date.now()}`;
  const storagePath = `videos/${context.auth.uid}/${uploadId}`;
  return {
    uploadId,
    storagePath,
    contentType: data?.type || null,
    size: data?.size || null,
  };
});

exports.notifyMention = onCallV2(async (request) => {
  const data = request.data || {};
  const auth = request.auth;
  if (!auth || !auth.uid) throw new Error("unauthenticated");

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
