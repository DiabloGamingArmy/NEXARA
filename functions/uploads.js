/**
 * Upload session backend for resumable video uploads.
 */

const { onRequest } = require("firebase-functions/v2/https");
const admin = require("firebase-admin");
const { randomUUID } = require("crypto");

if (!admin.apps.length) {
    admin.initializeApp({
        credential: admin.credential.applicationDefault(),
    });
}

const db = admin.firestore();
const FieldValue = admin.firestore.FieldValue;

const ALLOWED_ORIGINS = [
    "https://spike-streaming-service.web.app",
    "https://spike-streaming-service.firebaseapp.com",
    "https://diablogamingarmy.github.io",
];

function applyCors(req, res) {
    const origin = req.headers.origin;
    if (origin && ALLOWED_ORIGINS.includes(origin)) {
        res.set("Access-Control-Allow-Origin", origin);
        res.set("Vary", "Origin");
    }
    res.set("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
    res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
    res.set("Access-Control-Max-Age", "3600");

    if (req.method === "OPTIONS") {
        res.status(204).send("");
        return true;
    }
    return false;
}

async function requireAuthUid(req) {
    const authHeader = (req.headers.authorization || "").trim();
    const hasAuthHeader = authHeader.toLowerCase().startsWith("bearer ");

    if (!hasAuthHeader) {
        return { uid: null, error: { status: 401, message: "UNAUTHENTICATED" } };
    }

    const idToken = authHeader.split(" ")[1];
    if (!idToken) {
        return { uid: null, error: { status: 401, message: "UNAUTHENTICATED" } };
    }

    try {
        const decoded = await admin.auth().verifyIdToken(idToken);
        const uid = decoded?.uid;
        if (!uid) {
            return { uid: null, error: { status: 401, message: "UNAUTHENTICATED" } };
        }
        return { uid };
    } catch (error) {
        console.error("Failed to verify ID token", error);
        return { uid: null, error: { status: 401, message: "UNAUTHENTICATED" } };
    }
}

function respondWithError(res, status, message) {
    return res.status(status).json({ error: message });
}

function sanitizeFileName(fileName = "") {
    return fileName.replace(/[\\/]/g, "_");
}

exports.createUploadSession = onRequest({ region: "us-central1" }, async (req, res) => {
    if (applyCors(req, res)) return;

    if (req.method !== "POST") {
        return respondWithError(res, 405, "Method Not Allowed");
    }

    const { uid, error } = await requireAuthUid(req);
    if (error || !uid) {
        return respondWithError(res, error?.status || 401, error?.message || "UNAUTHENTICATED");
    }

    const { fileName, contentType, size } = req.body || {};
    if (!fileName) {
        return respondWithError(res, 400, "Missing fileName");
    }

    const uploadId = randomUUID();
    const safeName = sanitizeFileName(fileName);
    const storageKey = `videos/${uid}/${uploadId}/${safeName}`;

    try {
        const fileRef = admin.storage().bucket().file(storageKey);
        const [resumableUploadUrl] = await fileRef.createResumableUpload({
            metadata: {
                contentType: contentType || "application/octet-stream",
            },
        });

        await db.collection("videoUploads").doc(uploadId).set({
            uploadId,
            ownerId: uid,
            storageKey,
            status: "INITIATED",
            createdAt: FieldValue.serverTimestamp(),
        });

        return res.json({
            uploadId,
            ownerId: uid,
            storageKey,
            resumableUploadUrl,
        });
    } catch (err) {
        console.error("createUploadSession failed", err);
        return respondWithError(res, 500, "Failed to create upload session");
    }
});
