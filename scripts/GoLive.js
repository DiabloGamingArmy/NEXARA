// scripts/GoLive.js
// Nexera Go Live Controller – Browser First

import { getAuth } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-auth.js";
import { getFirestore, doc, onSnapshot, updateDoc, serverTimestamp } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";

// --------------------------------------------------
// Load IVS Web Broadcast SDK
// --------------------------------------------------
const IVS_BROADCAST_SRC =
    "https://web-broadcast.live-video.net/1.0.0/amazon-ivs-web-broadcast.min.js";

function loadBroadcastSdk() {
    if (window.IVSBroadcastClient) return Promise.resolve();
    return new Promise((resolve, reject) => {
        const script = document.createElement("script");
        script.src = IVS_BROADCAST_SRC;
        script.async = true;
        script.onload = resolve;
        script.onerror = reject;
        document.head.appendChild(script);
    });
}

// --------------------------------------------------
// Controller
// --------------------------------------------------
export class NexeraGoLiveController {
    constructor() {
        this.auth = getAuth();
        this.db = getFirestore();

        this.state = "idle";
        this.inputMode = "camera"; // camera | screen | external
        this.audioMode = "mic";    // mic | system | mixed | external
        this.latencyMode = "normal"; // normal | low

        this.session = null;
        this.client = null;
        this.stream = null;
        this.previewVideo = null;
        this.unsubscribeLiveDoc = null;
    }

    // ----------------------------------------------
    // UI Bootstrapping
    // ----------------------------------------------
    initializeUI() {
        const root = document.getElementById("go-live-root");
        if (!root) return;

        root.innerHTML = `
            <div class="go-live-interface">
                <div class="preview-area">
                    <video id="live-preview" autoplay muted playsinline></video>
                    <div id="obs-panel" style="display:none;"></div>
                </div>

                <div class="config-area">
                    <input id="stream-title" placeholder="Stream title" />
                    <input id="stream-category" placeholder="Category" />
                    <input id="stream-tags" placeholder="Tags (comma separated)" />

                    <select id="input-mode">
                        <option value="camera">Camera</option>
                        <option value="screen">Screen</option>
                        <option value="external">Streaming Software (OBS)</option>
                    </select>

                    <select id="latency-mode">
                        <option value="normal">Normal Latency</option>
                        <option value="low">Low Latency</option>
                    </select>
                </div>

                <div class="control-area">
                    <button id="start-stream">Start Stream</button>
                    <button id="end-stream" disabled>End Stream</button>
                </div>
            </div>
        `;

        this.previewVideo = document.getElementById("live-preview");

        document.getElementById("input-mode").onchange = e => {
            this.inputMode = e.target.value;
        };

        document.getElementById("latency-mode").onchange = e => {
            this.latencyMode = e.target.value;
        };

        document.getElementById("start-stream").onclick = () => this.start();
        document.getElementById("end-stream").onclick = () => this.stop();
    }

    // ----------------------------------------------
    // Start Streaming
    // ----------------------------------------------
    async start() {
        if (this.state !== "idle") return;
        this.state = "initializing";

        const title = document.getElementById("stream-title").value || "";
        const category = document.getElementById("stream-category").value || "";
        const tags = document.getElementById("stream-tags").value
            .split(",")
            .map(t => t.trim())
            .filter(Boolean);

        const latencyMode =
            this.latencyMode === "low" ? "LOW" : "NORMAL";

        const user = this.auth?.currentUser;
        if (!user) {
            throw new Error("User must be signed in to start streaming");
        }

        const idToken = await user.getIdToken();

        const response = await fetch(
            "https://us-central1-spike-streaming-service.cloudfunctions.net/createEphemeralChannel",
            {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    Authorization: `Bearer ${idToken}`,
                },
                body: JSON.stringify({
                    title,
                    category,
                    tags,
                    latencyMode,
                    visibility: "public",
                }),
            }
        );

        if (!response.ok) {
            const errorPayload = await response.json().catch(() => ({}));
            throw new Error(errorPayload?.error || "Failed to create channel");
        }

        this.session = await response.json();

        if (this.inputMode === "external") {
            this.enterOBSMode();
            return;
        }

        await this.startBrowserBroadcast();
    }

    // ----------------------------------------------
    // Browser Broadcast (Camera / Screen)
    // ----------------------------------------------
    async startBrowserBroadcast() {
        await loadBroadcastSdk();

        this.client = IVSBroadcastClient.create({
            ingestEndpoint: this.session.channelArn,
            streamKey: this.session.streamKey,
        });

        this.stream =
            this.inputMode === "screen"
                ? await navigator.mediaDevices.getDisplayMedia({ video: true, audio: true })
                : await navigator.mediaDevices.getUserMedia({ video: true, audio: true });

        this.previewVideo.srcObject = this.stream;

        this.client.addVideoInputDevice(this.stream.getVideoTracks()[0], "camera");
        if (this.stream.getAudioTracks()[0]) {
            this.client.addAudioInputDevice(this.stream.getAudioTracks()[0]);
        }

        await this.client.startBroadcast(this.session.streamKey, this.session.channelArn);

        await updateDoc(
  doc(this.db, "liveStreams", this.session.sessionId),
  {
    isLive: true,
    startedAt: serverTimestamp(),
    endedAt: null
  }
);


        this.state = "live";
        document.getElementById("end-stream").disabled = false;
    }

    // ----------------------------------------------
    // OBS Mode
    // ----------------------------------------------
    enterOBSMode() {
        const panel = document.getElementById("obs-panel");
        panel.style.display = "block";
        panel.innerHTML = `
            <h3>Stream with OBS</h3>
            <p><strong>Server:</strong><br>${this.session.ingestEndpoint}</p>
            <p><strong>Stream Key:</strong><br>${this.session.streamKey}</p>
            <p>Waiting for stream to go live…</p>
        `;

        this.unsubscribeLiveDoc = onSnapshot(
            doc(this.db, "liveStreams", this.session.sessionId),
            snap => {
                if (snap.exists() && snap.data().isLive) {
                    this.state = "live";
                }
            }
        );
    }

    // ----------------------------------------------
    // Stop Stream
    // ----------------------------------------------
    async stop() {
        if (this.client) {
            await this.client.stopBroadcast();
            this.client = null;
        }

        if (this.stream) {
            this.stream.getTracks().forEach(t => t.stop());
            this.stream = null;
        }

        if (this.unsubscribeLiveDoc) {
            this.unsubscribeLiveDoc();
            this.unsubscribeLiveDoc = null;
        }

        await updateDoc(
  doc(this.db, "liveStreams", this.session.sessionId),
  {
    isLive: false,
    endedAt: serverTimestamp()
  }
);


        this.state = "idle";
        document.getElementById("end-stream").disabled = true;
    }
}

// ----------------------------------------------
// SPA Entry
// ----------------------------------------------
export function initialize() {
    const controller = new NexeraGoLiveController();
    controller.initializeUI();
    window.__goLiveController = controller;
}

export function teardown() {
    if (window.__goLiveController) {
        window.__goLiveController.stop();
        window.__goLiveController = null;
    }
}
