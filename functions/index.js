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
const logger = require("firebase-functions/logger");
const admin = require("firebase-admin");
const {AccessToken} = require("livekit-server-sdk");

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
const LIVEKIT_API_KEY = defineSecret("LIVEKIT_API_KEY");
const LIVEKIT_API_SECRET = defineSecret("LIVEKIT_API_SECRET");
const LIVEKIT_URL = defineSecret("LIVEKIT_URL");

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

exports.createLiveKitToken = onCallV2(
  {secrets: ["LIVEKIT_API_KEY", "LIVEKIT_API_SECRET", "LIVEKIT_URL"], cors: true},
  async (request) => {
    const auth = request.auth;
    if (!auth || !auth.uid) throw new HttpsError("unauthenticated", "Sign-in required.");
    const roomName = String(request.data?.roomName || "").trim();
    if (!roomName) {
      throw new HttpsError("invalid-argument", "roomName required.");
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
    token.addGrant({room: roomName, roomJoin: true, canPublish: true, canSubscribe: true});

    return {
      token: await token.toJwt(),
      url: livekitUrl,
    };
  },
);
