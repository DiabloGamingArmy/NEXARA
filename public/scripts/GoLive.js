// scripts/GoLive.js
// Nexera Go Live Controller – Browser First

import { getAuth } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-auth.js";
import { getFirestore, doc, onSnapshot, updateDoc, serverTimestamp, setDoc, deleteField } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";

const GO_LIVE_MODE_STORAGE_KEY = "nexera-go-live-mode";
const GO_LIVE_AUDIO_STORAGE_KEY = "nexera-go-live-audio-gains";

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
        this.audioMode = "mic"; // mic | system | mixed | external
        this.latencyMode = "NORMAL"; // NORMAL | LOW
        this.autoRecord = false;

        this.session = null;
        this.client = null;
        this.stream = null;
        this.previewVideo = null;
        this.unsubscribeLiveDoc = null;

        this.uiMode = localStorage.getItem(GO_LIVE_MODE_STORAGE_KEY) === "advanced" ? "advanced" : "basic";
        this.logEntries = [];
        this.audioGains = this.loadAudioGains();
    }

    // ----------------------------------------------
    // UI Bootstrapping
    // ----------------------------------------------
    initializeUI() {
        this.renderUI();
    }

    captureFormState() {
        return {
            title: document.getElementById("stream-title")?.value || "",
            category: document.getElementById("stream-category")?.value || "",
            tags: document.getElementById("stream-tags")?.value || "",
            visibility: document.getElementById("stream-visibility")?.value || "public",
            inputMode: this.inputMode,
            latencyMode: this.latencyMode,
            autoRecord: this.autoRecord,
        };
    }

    renderUI() {
        const root = document.getElementById("go-live-root");
        if (!root) return;

        const preserved = this.captureFormState();
        const toggleLabel = this.uiMode === "advanced" ? "Back to Basic" : "Switch to Advanced Studio";

        root.innerHTML = `
            <div class="go-live-shell ${this.uiMode}-mode">
                <div class="go-live-topbar">
                    <div class="status-chip" id="go-live-state-chip">Idle</div>
                    <div class="go-live-mode-toggle">
                        <button class="icon-pill" id="go-live-mode-toggle-btn">${toggleLabel}</button>
                    </div>
                </div>
                <div class="go-live-main">
                    <div class="go-live-left">
                        <div class="go-live-card preview-card glass-panel">
                            <div class="preview-header">
                                <div class="status-badge" id="go-live-status">Idle</div>
                                <div class="preview-meta" id="go-live-visibility-pill">Preview</div>
                            </div>
                            <div class="preview-frame">
                                <video id="live-preview" autoplay muted playsinline></video>
                                <div id="obs-panel" class="obs-panel" style="display:none;"></div>
                            </div>
                            <div class="control-row">
                                <button class="create-btn-sidebar" id="start-stream">Start Stream</button>
                                <button class="icon-pill" id="end-stream" disabled>End Stream</button>
                            </div>
                        </div>
                        ${
                            this.uiMode === "advanced"
                                ? `<div class="go-live-card glass-panel log-card">
                                        <div class="panel-title">Session Logs</div>
                                        <div id="go-live-log" class="log-viewer" aria-live="polite"></div>
                                   </div>`
                                : ""
                        }
                    </div>
                    <div class="go-live-right">
                        <div class="go-live-card config-card glass-panel">
                            <div class="config-header">
                                <div>
                                    <div class="panel-title">Stream Setup</div>
                                    <div class="config-subtitle">Configure your stream before going live.</div>
                                </div>
                                <div class="status-dot-row">
                                    <span class="status-dot" id="status-dot-indicator"></span>
                                    <span id="status-dot-label">Idle</span>
                                </div>
                            </div>
                            <label class="config-label" for="stream-title">Title</label>
                            <input id="stream-title" class="form-input" placeholder="Stream title" />
                            <label class="config-label" for="stream-category">Category</label>
                            <input id="stream-category" class="form-input" placeholder="Choose a category" />
                            <label class="config-label" for="stream-tags">Tags</label>
                            <input id="stream-tags" class="form-input" placeholder="Tags (comma separated)" />
                            <label class="config-label" for="stream-visibility">Visibility</label>
                            <select id="stream-visibility" class="form-input">
                                <option value="public">Public</option>
                            </select>
                            <label class="config-label" for="input-mode">Input Mode</label>
                            <select id="input-mode" class="form-input">
                                <option value="camera">Camera</option>
                                <option value="screen">Screen</option>
                                <option value="external">Streaming Software (OBS)</option>
                            </select>
                            <label class="config-label" for="latency-mode">Latency</label>
                            <select id="latency-mode" class="form-input">
                                <option value="NORMAL">Normal Latency</option>
                                <option value="LOW">Low Latency</option>
                            </select>
                            <label class="config-toggle">
                                <input type="checkbox" id="auto-record" />
                                <span>Enable Auto-record</span>
                            </label>
                        </div>
                        ${
                            this.uiMode === "advanced"
                                ? `<div class="go-live-card studio-card glass-panel">
                                        <div class="panel-title">Advanced Studio</div>
                                        <div class="studio-grid">
                                            <div class="studio-panel">
                                                <div class="panel-heading">Scenes</div>
                                                <div class="panel-body muted">Add and arrange scene presets.</div>
                                            </div>
                                            <div class="studio-panel">
                                                <div class="panel-heading">Sources</div>
                                                <div class="panel-body muted">Manage screens, cameras, and overlays.</div>
                                            </div>
                                            <div class="studio-panel">
                                                <div class="panel-heading">Audio Mixer</div>
                                                <div class="panel-body">
                                                    <label class="mixer-label">Mic Gain
                                                        <input type="range" id="mixer-mic" min="0" max="150" />
                                                    </label>
                                                    <label class="mixer-label">System Gain
                                                        <input type="range" id="mixer-system" min="0" max="150" />
                                                    </label>
                                                    <div class="muted small-text">Mix levels are UI-only for now and saved locally.</div>
                                                </div>
                                            </div>
                                            <div class="studio-panel">
                                                <div class="panel-heading">Graphics</div>
                                                <div class="panel-body muted">Lower thirds and overlays placeholder.</div>
                                            </div>
                                            <div class="studio-panel">
                                                <div class="panel-heading">Stream Health</div>
                                                <div class="panel-body">
                                                    <div class="health-row"><span>Bitrate</span><span class="muted">N/A</span></div>
                                                    <div class="health-row"><span>FPS</span><span class="muted">N/A</span></div>
                                                    <div class="health-row"><span>Dropped Frames</span><span class="muted">N/A</span></div>
                                                </div>
                                            </div>
                                            <div class="studio-panel" id="logs-panel">
                                                <div class="panel-heading">Logs</div>
                                                <div class="panel-body muted">Session logs are displayed below the preview.</div>
                                            </div>
                                        </div>
                                   </div>`
                                : ""
                        }
                    </div>
                </div>
            </div>
        `;

        this.previewVideo = document.getElementById("live-preview");
        this.restoreFormState(preserved);
        this.bindEvents();
        this.applyAudioGains();
        this.renderLogEntries();
        this.setStatus(this.state);
    }

    restoreFormState(state) {
        document.getElementById("stream-title").value = state.title || "";
        document.getElementById("stream-category").value = state.category || "";
        document.getElementById("stream-tags").value = state.tags || "";
        document.getElementById("stream-visibility").value = state.visibility || "public";

        const inputMode = document.getElementById("input-mode");
        if (inputMode) inputMode.value = state.inputMode || "camera";

        const latencyMode = document.getElementById("latency-mode");
        if (latencyMode) latencyMode.value = state.latencyMode || "NORMAL";

        const autoRecord = document.getElementById("auto-record");
        if (autoRecord) autoRecord.checked = !!state.autoRecord;
    }

    bindEvents() {
        const modeToggle = document.getElementById("go-live-mode-toggle-btn");
        if (modeToggle) {
            modeToggle.onclick = () => {
                this.uiMode = this.uiMode === "advanced" ? "basic" : "advanced";
                localStorage.setItem(GO_LIVE_MODE_STORAGE_KEY, this.uiMode);
                this.persistUiSnapshot();
                this.renderUI();
            };
        }

        const inputMode = document.getElementById("input-mode");
        if (inputMode) inputMode.onchange = (e) => {
            this.inputMode = e.target.value;
            this.persistSettingsSnapshot();
        };

        const latencyMode = document.getElementById("latency-mode");
        if (latencyMode) latencyMode.onchange = (e) => {
            this.latencyMode = e.target.value || "NORMAL";
            this.persistSettingsSnapshot();
        };

        const autoRecord = document.getElementById("auto-record");
        if (autoRecord) autoRecord.onchange = (e) => {
            this.autoRecord = !!e.target.checked;
            this.persistSettingsSnapshot();
        };

        const startBtn = document.getElementById("start-stream");
        if (startBtn) startBtn.onclick = () => this.safeStart();

        const endBtn = document.getElementById("end-stream");
        if (endBtn) endBtn.onclick = () => this.safeStop();

        const micGain = document.getElementById("mixer-mic");
        if (micGain) {
            micGain.value = this.audioGains.mic;
            micGain.onchange = (e) => {
                const mic = Number(e.target.value);
                this.persistAudioGains({ mic });
                this.persistStudioSnapshot({ audio: { micGain: mic, systemGain: this.audioGains.system } });
            };
        }

        const systemGain = document.getElementById("mixer-system");
        if (systemGain) {
            systemGain.value = this.audioGains.system;
            systemGain.onchange = (e) => {
                const system = Number(e.target.value);
                this.persistAudioGains({ system });
                this.persistStudioSnapshot({ audio: { micGain: this.audioGains.mic, systemGain: system } });
            };
        }
    }

    setStatus(state, message = "") {
        this.state = state;
        const chip = document.getElementById("go-live-state-chip");
        const status = document.getElementById("go-live-status");
        const pill = document.getElementById("go-live-visibility-pill");
        const dot = document.getElementById("status-dot-indicator");
        const dotLabel = document.getElementById("status-dot-label");

        const labels = {
            idle: "Idle",
            previewing: "Preview",
            starting: "Starting…",
            live: "Live",
            error: "Error",
        };

        const label = labels[state] || "Idle";
        const detail = message ? `${label} – ${message}` : label;

        if (chip) chip.textContent = label;
        if (status) status.textContent = detail;
        if (pill) pill.textContent = this.inputMode === "external" ? "External Software" : "Preview";
        if (dotLabel) dotLabel.textContent = label;

        [chip, status, dot].forEach((el) => {
            if (!el) return;
            el.classList.remove("state-idle", "state-preview", "state-starting", "state-live", "state-error");
            const cls =
                state === "live"
                    ? "state-live"
                    : state === "starting"
                    ? "state-starting"
                    : state === "error"
                    ? "state-error"
                    : state === "previewing"
                    ? "state-preview"
                    : "state-idle";
            el.classList.add(cls);
        });

        this.syncControls();
    }

    syncControls() {
        const startBtn = document.getElementById("start-stream");
        const endBtn = document.getElementById("end-stream");
        if (startBtn) startBtn.disabled = this.state === "starting" || this.state === "live";
        if (endBtn) endBtn.disabled = this.state === "idle" || this.state === "error" || this.state === "previewing";
    }

    log(message) {
        const entry = `${new Date().toLocaleTimeString()} – ${message}`;
        this.logEntries.push(entry);
        if (this.logEntries.length > 200) this.logEntries.shift();
        console.log("[GoLive]", message);
        this.renderLogEntries();
    }

    renderLogEntries() {
        const logEl = document.getElementById("go-live-log");
        if (!logEl) return;
        logEl.innerHTML = this.logEntries
            .slice(-50)
            .map((line) => `<div class=\"log-line\">${line}</div>`)
            .join("");
        logEl.scrollTop = logEl.scrollHeight;
    }

    persistAudioGains(updates) {
        this.audioGains = { ...this.audioGains, ...updates };
        try {
            localStorage.setItem(GO_LIVE_AUDIO_STORAGE_KEY, JSON.stringify(this.audioGains));
        } catch (err) {
            console.warn("[GoLive] failed to persist audio gains", err);
        }
    }

    loadAudioGains() {
        try {
            const stored = localStorage.getItem(GO_LIVE_AUDIO_STORAGE_KEY);
            if (stored) return { mic: 100, system: 100, ...JSON.parse(stored) };
        } catch (err) {
            console.warn("[GoLive] failed to load audio gains", err);
        }
        return { mic: 100, system: 100 };
    }

    applyAudioGains() {
        const micGain = document.getElementById("mixer-mic");
        const systemGain = document.getElementById("mixer-system");
        if (micGain) micGain.value = this.audioGains.mic;
        if (systemGain) systemGain.value = this.audioGains.system;
    }

    settingsPayload(overrides = {}) {
        const tags = Array.isArray(overrides.tags)
            ? overrides.tags
            : overrides.tags
            ? String(overrides.tags)
                  .split(",")
                  .map((t) => t.trim())
                  .filter(Boolean)
            : Array.isArray(this.session?.tags)
            ? this.session.tags
            : [];

        const title = overrides.title ?? this.session?.title ?? document.getElementById("stream-title")?.value ?? "";
        const category = overrides.category ?? this.session?.category ?? document.getElementById("stream-category")?.value ?? "";
        const visibility = overrides.visibility ?? this.session?.visibility ?? document.getElementById("stream-visibility")?.value ?? "public";

        return {
            inputMode: this.inputMode,
            audioMode: this.audioMode,
            latencyMode: this.latencyMode === "LOW" ? "LOW" : "NORMAL",
            autoRecord: !!this.autoRecord,
            visibility,
            title,
            category,
            tags,
        };
    }

    async persistLiveSnapshot(overrides = {}) {
        if (!this.session?.sessionId) return;

        const settings = this.settingsPayload(overrides);
        const payload = {
            uid: this.auth?.currentUser?.uid || null,
            playbackUrl: this.session.playbackUrl || null,
            visibility: settings.visibility,
            title: settings.title,
            category: settings.category,
            tags: settings.tags,
            channelArn: this.session.channelArn || null,
            ingestEndpoint: this.session.ingestEndpoint || null,
            rtmpsIngestUrl: this.session.rtmpsIngestUrl || null,
            settings,
            ui: { mode: this.uiMode, updatedAt: serverTimestamp() },
        };

        try {
            await setDoc(doc(this.db, "liveStreams", this.session.sessionId), payload, { merge: true });
        } catch (error) {
            console.error("[GoLive] failed to persist live settings", error);
            this.log(`Persist settings failed: ${error.message || error}`);
        }
    }

    async persistSettingsSnapshot(overrides = {}) {
        await this.persistLiveSnapshot(overrides);
    }

    async persistUiSnapshot() {
        if (!this.session?.sessionId) return;
        try {
            await setDoc(
                doc(this.db, "liveStreams", this.session.sessionId),
                { ui: { mode: this.uiMode, updatedAt: serverTimestamp() } },
                { merge: true }
            );
        } catch (error) {
            console.error("[GoLive] failed to persist UI snapshot", error);
        }
    }

    async persistStudioSnapshot(partial = {}) {
        if (!this.session?.sessionId) return;
        const audio = partial.audio || { micGain: this.audioGains.mic, systemGain: this.audioGains.system };
        const studio = {
            activeSceneId: partial.activeSceneId ?? null,
            scenes: partial.scenes ?? [],
            sources: partial.sources ?? [],
            graphics: partial.graphics ?? [],
            audio,
        };
        try {
            await setDoc(doc(this.db, "liveStreams", this.session.sessionId), { studio }, { merge: true });
        } catch (error) {
            console.error("[GoLive] failed to persist studio snapshot", error);
        }
    }

    async persistPrivateStreamKey() {
        if (!this.session?.sessionId || !this.session.streamKey) return;
        const uid = this.auth?.currentUser?.uid || null;
        if (!uid) return;
        try {
            await setDoc(
                doc(this.db, "liveStreams", this.session.sessionId, "private", "keys"),
                { uid, streamKey: this.session.streamKey, updatedAt: serverTimestamp() },
                { merge: true }
            );
            await updateDoc(doc(this.db, "liveStreams", this.session.sessionId), { streamKey: deleteField() });
        } catch (error) {
            console.error("[GoLive] failed to persist private stream key", error);
            this.log(`Persist key failed: ${error.message || error}`);
        }
    }

    // ----------------------------------------------
    // Start Stream
    // ----------------------------------------------
    async safeStart() {
        this.setStatus("starting");
        this.log("Starting stream request");
        try {
            await this.start();
            this.setStatus(this.state);
            this.log("Stream started");
        } catch (error) {
            console.error("[GoLive] start failed", error);
            this.log(`Start failed: ${error.message || error}`);
            this.setStatus("error", error.message || "Start failed");
        }
    }

    async start() {
        const title = document.getElementById("stream-title").value || "";
        const category = document.getElementById("stream-category").value || "";
        const tags = (document.getElementById("stream-tags").value || "")
            .split(",")
            .map((t) => t.trim())
            .filter(Boolean);
        const visibility = document.getElementById("stream-visibility").value || "public";

        const latencyMode = this.latencyMode === "LOW" ? "LOW" : "NORMAL";
        this.latencyMode = latencyMode;

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
                    autoRecord: !!this.autoRecord,
                    visibility,
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

        await this.persistLiveSnapshot({
            title,
            category,
            tags,
            visibility,
        });

        await this.persistPrivateStreamKey();

        if (this.inputMode === "external") {
            this.setStatus("starting", "Waiting for external encoder");
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
            await updateDoc(doc(this.db, "liveStreams", this.session.sessionId), {
                isLive: true,
                startedAt: serverTimestamp(),
                endedAt: null,
            });
        } catch (error) {
            console.error("[GoLive] failed to mark session live", error);
            this.log(`Failed to mark live: ${error.message || error}`);
        }

        this.setStatus("live");
        this.log("Browser broadcast active");
    }

    // ----------------------------------------------
    // OBS Mode
    // ----------------------------------------------
    enterOBSMode() {
        const panel = document.getElementById("obs-panel");
        if (!panel) return;
        panel.style.display = "flex";
        panel.innerHTML = `
            <div class="panel-heading">External Encoder</div>
            <div class="panel-body">
                <div class="ingest-row"><span>Server</span><code>${this.session.ingestEndpoint}</code></div>
                <div class="ingest-row"><span>Stream Key</span><code>${this.session.streamKey}</code></div>
                <div class="muted">Start streaming from OBS to go live.</div>
            </div>
        `;

        this.unsubscribeLiveDoc = onSnapshot(doc(this.db, "liveStreams", this.session.sessionId), (snap) => {
            if (snap.exists() && snap.data().isLive) {
                this.setStatus("live");
            }
        });
    }

    // ----------------------------------------------
    // Stop Stream
    // ----------------------------------------------
    async safeStop() {
        this.log("Stopping stream");
        try {
            await this.stop();
            this.setStatus("idle");
            this.log("Stream ended");
        } catch (error) {
            console.error("[GoLive] stop failed", error);
            this.log(`Stop failed: ${error.message || error}`);
            this.setStatus("error", error.message || "Stop failed");
        }
    }

    async stop() {
        if (this.client) {
            await this.client.stopBroadcast();
            this.client = null;
        }

        if (this.stream) {
            this.stream.getTracks().forEach((t) => t.stop());
            this.stream = null;
        }

        if (this.unsubscribeLiveDoc) {
            this.unsubscribeLiveDoc();
            this.unsubscribeLiveDoc = null;
        }

        if (this.session) {
            try {
                await updateDoc(doc(this.db, "liveStreams", this.session.sessionId), {
                    isLive: false,
                    endedAt: serverTimestamp(),
                });
            } catch (error) {
                console.error("[GoLive] failed to mark session ended", error);
                this.log(`Failed to mark ended: ${error.message || error}`);
            }
        }

        this.session = null;
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
