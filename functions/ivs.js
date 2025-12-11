/**
 * IVS Backend for Nexera – Gen-2 Compatible
 */

import { onCall } from "firebase-functions/v2/https";
import { defineString, defineSecret } from "firebase-functions/params";

import { initializeApp, applicationDefault } from "firebase-admin/app";
import { getFirestore, FieldValue } from "firebase-admin/firestore";

import jwt from "jsonwebtoken";
import { v4 as uuidv4 } from "uuid";

import {
    IvsClient,
    CreateChannelCommand,
    CreateStreamKeyCommand,
} from "@aws-sdk/client-ivs";

// -------------------------------------------------------------
// Initialize Admin SDK (ESM-Compatible)
// -------------------------------------------------------------
initializeApp({
    credential: applicationDefault()
});

const db = getFirestore();

// -------------------------------------------------------------
// PARAM DEFINITIONS
// -------------------------------------------------------------
export const AWS_RECORDING_ARN = defineString("AWS_RECORDING_ARN");
export const AWS_PLAYBACK_KEY_ARN = defineString("AWS_PLAYBACK_KEY_ARN");

export const AWS_KEY = defineSecret("AWS_KEY");
export const AWS_SECRET = defineSecret("AWS_SECRET");
export const AWS_PLAYBACK_PRIVATE_KEY = defineSecret("AWS_PLAYBACK_PRIVATE_KEY");

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


// -------------------------------------------------------------
// Helper: Create Stream Key
// -------------------------------------------------------------
async function createStreamKey(ivs, channelArn) {
    const cmd = new CreateStreamKeyCommand({ channelArn });
    const res = await ivs.send(cmd);
    return res.streamKey.value;
}

// =============================================================
// 1. initializeUserChannel  (Persistent Channel Per User)
// =============================================================
export const initializeUserChannel = onCall(
    { region: "us-east-1", secrets: [AWS_KEY, AWS_SECRET, AWS_PLAYBACK_PRIVATE_KEY] },
    async (request) => {

        const uid = request.auth?.uid;
        if (!uid) {
            throw new Error("User must be authenticated.");
        }

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
            return existing.data();
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

        const streamKeyValue = await createStreamKey(ivs, channelArn);

        const payload = {
            uid,
            channelArn,
            streamKey: streamKeyValue,
            playbackUrl,
            createdAt: FieldValue.serverTimestamp()
,
        };

        await ref.set(payload);
        return payload;
    }
);

// =============================================================
// 2. createEphemeralChannel  (Temporary Streams On-Demand)
// =============================================================
export const createEphemeralChannel = onCall(
    { region: "us-east-1", secrets: [AWS_KEY, AWS_SECRET] },
    async (request) => {

        const uid = request.auth?.uid;
        if (!uid) {
            throw new Error("User must be authenticated.");
        }

        // Load params
        const accessKey = AWS_KEY.value();
        const secretKey = AWS_SECRET.value();
        const recordingArn = AWS_RECORDING_ARN.value();

        const ivs = makeIVS(accessKey, secretKey);

        const visibility = request.data.visibility || "public";
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

        const streamKeyValue = await createStreamKey(ivs, channelArn);

        const streamDoc = {
            sessionId,
            uid,
            channelArn,
            playbackUrl,
            streamKey: streamKeyValue,
            visibility,
            isLive: false,
            title: request.data.title || "",
            category: request.data.category || "",
            tags: request.data.tags || [],
            createdAt: FieldValue.serverTimestamp()
,
        };

        await db.collection("liveStreams").doc(sessionId).set(streamDoc);
        return streamDoc;
    }
);

// =============================================================
// 3. generatePlaybackToken  (Signed Token for Private Streams)
// =============================================================
export const generatePlaybackToken = onCall(
    { region: "us-east-1", secrets: [AWS_PLAYBACK_PRIVATE_KEY] },
    async (request) => {

        const channelArn = request.data.channelArn;
        const visibility = request.data.visibility || "public";

        // Public streams need no token
        if (visibility === "public") {
            return { token: null };
        }

        const privateKey = AWS_PLAYBACK_PRIVATE_KEY.value();
        if (!privateKey) {
            throw new Error("Missing playback private key.");
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

        return { token };
    }
);
