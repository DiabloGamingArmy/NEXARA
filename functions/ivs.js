/**
 * IVS Backend for Nexera – Gen-2 Compatible
 */

console.log("IVS backend version:", "2025-12-14T04:00Z-force-redeploy");

const { onRequest } = require("firebase-functions/v2/https");
const { defineString, defineSecret } = require("firebase-functions/params");

const admin = require("firebase-admin");

const jwt = require("jsonwebtoken");
const { v4: uuidv4 } = require("uuid");

const {
    IvsClient,
    CreateChannelCommand,
} = require("@aws-sdk/client-ivs");

// -------------------------------------------------------------
// Initialize Admin SDK (ESM-Compatible)
// -------------------------------------------------------------
if (!admin.apps.length) {
    admin.initializeApp({
        credential: admin.credential.applicationDefault(),
    });
}

const db = admin.firestore();
const FieldValue = admin.firestore.FieldValue;

// -------------------------------------------------------------
// PARAM DEFINITIONS
// -------------------------------------------------------------
const AWS_RECORDING_ARN = defineString("AWS_RECORDING_ARN");
const AWS_PLAYBACK_KEY_ARN = defineString("AWS_PLAYBACK_KEY_ARN");

const AWS_KEY = defineSecret("AWS_KEY");
const AWS_SECRET = defineSecret("AWS_SECRET");
const AWS_PLAYBACK_PRIVATE_KEY = defineSecret("AWS_PLAYBACK_PRIVATE_KEY");

// -------------------------------------------------------------
// CORS configuration
// -------------------------------------------------------------
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

// -------------------------------------------------------------
// Helper – Require UID from Firebase Auth ID token
// -------------------------------------------------------------
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

// -------------------------------------------------------------
// Helper – Create IVS client using loaded secrets
// -------------------------------------------------------------
function makeIVS(accessKey, secretKey) {
    return new IvsClient({
        region: "us-east-1",
        credentials: {
            accessKeyId: accessKey,
            secretAccessKey: secretKey,
        },
    });
}

function respondWithError(res, status, message) {
    return res.status(status).json({ error: message });
}

// =============================================================
// 1. initializeUserChannel  (Persistent Channel Per User)
// =============================================================
exports.initializeUserChannel = onRequest(
    { region: "us-central1", secrets: [AWS_KEY, AWS_SECRET, AWS_PLAYBACK_PRIVATE_KEY] },
    async (req, res) => {
        if (applyCors(req, res)) return;

        if (req.method !== "POST") {
            return respondWithError(res, 405, "Method Not Allowed");
        }

        const { uid, error } = await requireAuthUid(req);
        if (error || !uid) {
            return respondWithError(res, error?.status || 401, error?.message || "UNAUTHENTICATED");
        }

        try {
            // Load params
            const accessKey = AWS_KEY.value();
            const secretKey = AWS_SECRET.value();
            const recordingArn = AWS_RECORDING_ARN.value();

            const ivs = makeIVS(accessKey, secretKey);

            const ref = db.collection("users")
                .doc(uid)
                .collection("streamConfig")
                .doc("persistent");

            const existing = await ref.get();
            if (existing.exists) {
                return res.json(existing.data());
            }

            const channelName = `user_${uid}_persistent`;

            const channelCmd = new CreateChannelCommand({
                name: channelName,
                type: "STANDARD",
                latencyMode: "LOW",
                recordingConfigurationArn: recordingArn,
            });

            const channelRes = await ivs.send(channelCmd);
            const { arn: channelArn, playbackUrl } = channelRes.channel;
            const streamKeyValue = channelRes.streamKey?.value;
            if (!streamKeyValue) {
                console.error("CreateChannel response missing stream key", channelRes);
                return respondWithError(res, 500, "IVS channel created but stream key missing");
            }

            const payload = {
                uid,
                channelArn,
                streamKey: streamKeyValue,
                playbackUrl,
                createdAt: FieldValue.serverTimestamp(),
            };

            await ref.set(payload);
            return res.json(payload);
        } catch (error) {
            console.error("initializeUserChannel error", error);
            return respondWithError(res, 500, "Failed to initialize user channel");
        }
    }
);

// =============================================================
// 2. createEphemeralChannel  (Temporary Streams On-Demand)
// =============================================================
exports.createEphemeralChannel = onRequest(
    { region: "us-central1", secrets: [AWS_KEY, AWS_SECRET] },
    async (req, res) => {
        if (applyCors(req, res)) return;

        if (req.method !== "POST") {
            return respondWithError(res, 405, "Method Not Allowed");
        }

        const { uid, error } = await requireAuthUid(req);
        if (error || !uid) {
            return respondWithError(res, error?.status || 401, error?.message || "UNAUTHENTICATED");
        }

        try {
            // Load params
            const accessKey = AWS_KEY.value();
            const secretKey = AWS_SECRET.value();
            const recordingArn = AWS_RECORDING_ARN.value();

            const ivs = makeIVS(accessKey, secretKey);

            const visibility = req.body.visibility || "public";
            const sessionId = uuidv4();
            const channelName = `user_${uid}_session_${sessionId}`;

            const channelCmd = new CreateChannelCommand({
                name: channelName,
                type: "STANDARD",
                latencyMode: "LOW",
                recordingConfigurationArn: recordingArn,
            });

            const channelRes = await ivs.send(channelCmd);
            const { arn: channelArn, playbackUrl } = channelRes.channel;
            const streamKeyValue = channelRes.streamKey?.value;
            if (!streamKeyValue) {
                console.error("CreateChannel response missing stream key", channelRes);
                return respondWithError(res, 500, "IVS channel created but stream key missing");
            }

            const streamDoc = {
                sessionId,
                uid,
                channelArn,
                playbackUrl,
                streamKey: streamKeyValue,
                visibility,
                isLive: false,
                title: req.body.title || "",
                category: req.body.category || "",
                tags: req.body.tags || [],
                createdAt: FieldValue.serverTimestamp(),
            };

            await db.collection("liveStreams").doc(sessionId).set(streamDoc);
            return res.json(streamDoc);
        } catch (error) {
            console.error("createEphemeralChannel error", error);
            return respondWithError(res, 500, "Failed to create ephemeral channel");
        }
    }
);

// =============================================================
// 3. generatePlaybackToken  (Signed Token for Private Streams)
// =============================================================
exports.generatePlaybackToken = onRequest(
    { region: "us-central1", secrets: [AWS_PLAYBACK_PRIVATE_KEY] },
    async (req, res) => {
        if (applyCors(req, res)) return;

        if (req.method !== "POST") {
            return respondWithError(res, 405, "Method Not Allowed");
        }

        const { uid, error } = await requireAuthUid(req);
        if (error || !uid) {
            return respondWithError(res, error?.status || 401, error?.message || "UNAUTHENTICATED");
        }

        const channelArn = req.body?.channelArn;
        if (!channelArn) {
            return respondWithError(res, 400, "Missing required field: channelArn");
        }

        const visibility = req.body.visibility || "public";

        try {
            // Public streams need no token
            if (visibility === "public") {
                return res.json({ token: null });
            }

            const privateKey = AWS_PLAYBACK_PRIVATE_KEY.value();
            if (!privateKey) {
                return respondWithError(res, 500, "Missing playback private key.");
            }

            const token = jwt.sign(
                {
                    "aws:channel-arn": channelArn,
                    "aws:access-control:action": "ivs:Play",
                },
                privateKey,
                {
                    algorithm: "RS256",
                    expiresIn: "1h",
                }
            );

            return res.json({ token });
        } catch (error) {
            console.error("generatePlaybackToken error", error);
            return respondWithError(res, 500, "Failed to generate playback token");
        }
    }
);

