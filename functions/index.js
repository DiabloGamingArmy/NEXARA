/**
 * Import function triggers from their respective submodules:
 *
 * const {onCall} = require("firebase-functions/v2/https");
 * const {onDocumentWritten} = require("firebase-functions/v2/firestore");
 *
 * See a full list of supported triggers at https://firebase.google.com/docs/functions
 */

const {setGlobalOptions} = require("firebase-functions");
const {onCall, onRequest} = require("firebase-functions/https");
const {onDocumentCreated} = require("firebase-functions/v2/firestore");
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

if (!admin.apps.length) {
  admin.initializeApp();
}

const db = admin.firestore();
const FieldValue = admin.firestore.FieldValue;

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

const TOPIC_STAT_BUCKETS = [
  "day",
  "week",
  "month",
  "halfYear",
  "year",
  "fiveYears",
  "lifetime",
];

function resolveTopicId(payload) {
  if (!payload) return null;
  return payload.categoryId || payload.topicId || payload.category || payload.topic || null;
}

function buildTopicStatCounts() {
  return TOPIC_STAT_BUCKETS.reduce((acc, key) => {
    acc[key] = FieldValue.increment(1);
    return acc;
  }, {});
}

async function bumpTopicStats(topicId) {
  if (!topicId) return null;
  const ref = db.collection("topicStats").doc(String(topicId));
  return ref.set(
    {
      counts: buildTopicStatCounts(),
      updatedAt: FieldValue.serverTimestamp(),
    },
    { merge: true },
  );
}

exports.trackPostTopicStats = onDocumentCreated("posts/{postId}", (event) => {
  const data = event.data?.data();
  const topicId = resolveTopicId(data);
  if (!topicId) return null;
  return bumpTopicStats(topicId);
});

exports.trackVideoTopicStats = onDocumentCreated("videos/{videoId}", (event) => {
  const data = event.data?.data();
  const topicId = resolveTopicId(data);
  if (!topicId) return null;
  return bumpTopicStats(topicId);
});

exports.trackLiveTopicStats = onDocumentCreated("liveStreams/{streamId}", (event) => {
  const data = event.data?.data();
  const topicId = resolveTopicId(data);
  if (!topicId) return null;
  return bumpTopicStats(topicId);
});
