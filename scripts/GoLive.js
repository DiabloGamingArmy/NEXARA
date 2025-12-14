// scripts/GoLive.js
// Nexera Go Live Controller – Browser First

import { getAuth } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-auth.js";
import { getFirestore, doc, onSnapshot, updateDoc, serverTimestamp } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";

// --------------------------------------------------
// Load IVS Web Broadcast SDK
// --------------------------------------------------
const IVS_BROADCAST_SOURCES = [
    "https://web-broadcast.live-video.net/1.31.1/amazon-ivs-web-broadcast.js",
    "https://web-broadcast.live-video.net/1.31.0/amazon-ivs-web-broadcast.js",
    "https://web-broadcast.live-video.net/1.13.0/amazon-ivs-web-broadcast.js",
];

function loadBroadcastSdk() {
    if (window.IVSBroadcastClient) return Promise.resolve();
    const loadFromSource = (src) =>
        new Promise((resolve, reject) => {
            const script = document.createElement("script");
            script.src = src;
            script.async = true;
            script.onload = () => {
                if (window.IVSBroadcastClient) {
                    resolve();
                } else {
                    const err = new Error(`IVS Broadcast SDK loaded from ${src} but IVSBroadcastClient is missing`);
                    console.error("[GoLive]", err);
                    reject(err);
                }
            };
            script.onerror = (err) => {
                console.error("[GoLive]", `Failed to load IVS Broadcast SDK from ${src}`, err);
                reject(err || new Error(`Failed to load IVS Broadcast SDK from ${src}`));
            };
            document.head.appendChild(script);
        });

    return IVS_BROADCAST_SOURCES.reduce(
        (chain, src) => chain.catch(() => loadFromSource(src)),
        Promise.reject()
    ).then(() => {
        if (!window.IVSBroadcastClient) {
            throw new Error("IVS Broadcast SDK did not initialize after script load attempts");
        }
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

        const raw = await response.text();
        let json = null;
        try {
            json = raw ? JSON.parse(raw) : null;
        } catch (_) {
            json = null;
        }

        if (!response.ok) {
            const messageCandidate = json?.error?.message || json?.error || json?.message || raw || `HTTP ${response.status}`;
            const message = typeof messageCandidate === "string" ? messageCandidate : JSON.stringify(messageCandidate);
            console.error("[GoLive] createEphemeralChannel failed", { status: response.status, raw, json });
            throw new Error(message);
        }

        const data = json ?? {};
        this.session = {
            sessionId: data.sessionId,
            channelArn: data.channelArn,
            playbackUrl: data.playbackUrl,
            streamKey: data.streamKey,
            visibility: data.visibility,
            title,
            category,
            tags,
            ingestEndpoint: data.ingestEndpoint,
            rtmpsIngestUrl: data.rtmpsIngestUrl || (data.ingestEndpoint ? `rtmps://${data.ingestEndpoint}:443/app/` : ""),
        };

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

        const ingestHostname =
            this.session.ingestEndpoint ||
            (this.session.rtmpsIngestUrl
                ? (() => {
                      try {
                          return new URL(this.session.rtmpsIngestUrl).hostname;
                      } catch (_) {
                          return null;
                      }
                  })()
                : null);
        if (!ingestHostname) {
            throw new Error("Missing ingest endpoint from backend response");
        }

        this.client = IVSBroadcastClient.create({
            ingestEndpoint: ingestHostname,
            streamConfig: IVSBroadcastClient.BASIC_LANDSCAPE,
        });

        this.stream =
            this.inputMode === "screen"
                ? await navigator.mediaDevices.getDisplayMedia({ video: true, audio: true })
                : await navigator.mediaDevices.getUserMedia({ video: true, audio: true });

                this.previewVideo.srcObject = this.stream;

        // Split tracks into clean MediaStreams for IVS SDK
        const vTrack = this.stream.getVideoTracks()[0] || null;
        const aTrack = this.stream.getAudioTracks()[0] || null;

        if (!vTrack) {
            throw new Error("No video track available from capture source");
        }

        const videoStream = new MediaStream([vTrack]);
        const audioStream = aTrack ? new MediaStream([aTrack]) : null;

        // IMPORTANT: provide a name AND a VideoComposition
        await this.client.addVideoInputDevice(videoStream, "video1", { index: 0 });

        if (audioStream) {
            await this.client.addAudioInputDevice(audioStream, "audio1");
        }

        await this.client.startBroadcast(this.session.streamKey);

        try {
            await updateDoc(
                doc(this.db, "liveStreams", this.session.sessionId),
                {
                    isLive: true,
                    startedAt: serverTimestamp(),
                    endedAt: null,
                }
            );
        } catch (error) {
            console.error("[GoLive] failed to mark session live", error);
        }


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

        try {
            await updateDoc(
                doc(this.db, "liveStreams", this.session.sessionId),
                {
                    isLive: false,
                    endedAt: serverTimestamp(),
                }
            );
        } catch (error) {
            console.error("[GoLive] failed to mark session ended", error);
        }


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
