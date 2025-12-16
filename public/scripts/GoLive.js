// scripts/GoLive.js
// Nexera Go Live Controller – Browser First

import { getAuth } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-auth.js";
import { getFirestore, doc, onSnapshot, updateDoc, serverTimestamp, setDoc, deleteField } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";

const UI_MODE_STORAGE_KEY = "nexera_go_live_ui_mode";
const LEGACY_UI_MODE_STORAGE_KEY = "nexera-go-live-mode";
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

        this.uiBound = false;
        this.liveStartTime = null;
        this.statsInterval = null;

        this.formState = {
            title: "",
            category: "",
            tags: [],
            visibility: "public",
            inputMode: "camera",
            latencyMode: "NORMAL",
            autoRecord: false,
        };

        this.visibilityChoice = this.formState.visibility;

        this.studioRoot = null;

        this.scenes = [
            { id: "scene-main", name: "Main", sources: ["source-camera"] },
            { id: "scene-screen", name: "Screen Share", sources: ["source-screen"] },
            { id: "scene-external", name: "External Encoder", sources: ["source-external"] },
        ];
        this.sources = [
            { id: "source-camera", name: "Camera", type: "camera" },
            { id: "source-screen", name: "Screen Capture", type: "screen" },
            { id: "source-external", name: "External Encoder", type: "external" },
        ];
        this.activePreviewSceneId = this.scenes[0].id;
        this.activeProgramSceneId = this.scenes[0].id;
        this.selectedSceneId = this.scenes[0].id;
        this.selectedSourceId = this.sources[0].id;
        this.activeProgramSourceId = this.sources[0].id;
        this.advancedPreferences = { latencyMode: "NORMAL", autoRecord: false };
        this.mixerState = {
            mic: { muted: false, gain: 100 },
            system: { muted: false, gain: 100 },
            music: { muted: false, gain: 70 },
            aux: { muted: false, gain: 70 },
        };
        this.meterRaf = null;
        this.audioContext = null;
        this.meterAnalyser = null;
        this.meterSourceNode = null;
        this.meterGainNode = null;
        this.meterDestination = null;
        this.processedAudioStream = null;
        this.activeMeterChannel = null;

        this.session = null;
        this.client = null;
        this.stream = null;
        this.previewVideo = null;
        this.previewShell = null;
        this.programVideo = null;
        this.previewSlots = { basic: null, advanced: null };
        this.obsSlots = { basic: null, advanced: null };
        this.programStream = null;
        this.unsubscribeLiveDoc = null;

        const storedMode =
            localStorage.getItem(UI_MODE_STORAGE_KEY) || localStorage.getItem(LEGACY_UI_MODE_STORAGE_KEY) || "basic";
        this.uiMode = storedMode === "advanced" ? "advanced" : "basic";
        this.logEntries = [];
        this.audioGains = this.loadAudioGains();

        this.mobileMedia = window.matchMedia ? window.matchMedia("(max-width: 820px)") : null;
        this.isMobile = !!this.mobileMedia?.matches;
        if (this.mobileMedia?.addEventListener) {
            this.mobileMedia.addEventListener("change", (e) => {
                this.isMobile = e.matches;
                if (this.isMobile && this.uiMode === "advanced") {
                    this.applyUIMode("basic", { skipPersist: false });
                }
                this.filterInputOptions();
            });
        }
    }

    normalizeInputMode(value) {
        const mode = (value || "").toString().toLowerCase();
        if (mode === "screen") return "screen";
        if (mode === "external") return "external";
        if (mode === "program") return "program";
        return "camera";
    }

    setVisibility(nextVisibility) {
        const requested = nextVisibility === "private" ? "followers" : nextVisibility;
        const choice = ["public", "followers", "unlisted"].includes(requested) ? requested : "public";
        this.visibilityChoice = choice;
        const visibility = choice === "public" ? "public" : "private";
        this.formState.visibility = visibility;
        this.updateVisibilityButtons();
    }

    handleInputModeChange(nextMode) {
        const normalized = this.normalizeInputMode(nextMode);
        const previous = this.inputMode;
        this.formState.inputMode = normalized;
        this.inputMode = normalized;
        this.activeMeterChannel = this.getActiveAudioChannel();

        const matchingSource = this.sources.find((src) => src.type === normalized);
        if (matchingSource) {
            this.selectedSourceId = matchingSource.id;
        }

        if (previous !== normalized && this.stream) {
            this.stream.getTracks().forEach((t) => t.stop());
            this.stream = null;
            this.teardownAudioGraph();
            this.renderMeterLevels();
            if (this.previewVideo) this.previewVideo.srcObject = null;
        }

        this.writeStateIntoBasicForm();
        this.writeStateIntoAdvancedForm();
        this.syncSceneForInput();
        this.renderSources();
        this.updateEncoderTab();
        this.updateMonitorBadges();

        if (this.stream) {
            this.setupAudioPipeline(this.stream);
        } else {
            this.renderMeterLevels();
        }
    }

    // ----------------------------------------------
    // UI Bootstrapping
    // ----------------------------------------------
    initializeUI() {
        const root = document.getElementById("go-live-root");
        if (!root) return;

        this.root = root;
        const studio = root.classList.contains("go-live-studio") ? root : root.querySelector(".go-live-studio");

        if (studio) {
            this.studioRoot = studio;
            this.bindExistingUI();
            this.filterInputOptions();
            this.applyUIMode(this.uiMode, { skipPersist: true });
            this.setStatus(this.state);
            this.applyAudioGains();
            this.renderLogEntries();
            return;
        }

        this.renderLegacyUI();
    }

    renderLegacyUI() {
        const root = document.getElementById("go-live-root");
        if (!root) return;

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

        this.studioRoot = root.querySelector(".go-live-shell") || root;
        this.previewVideo = document.getElementById("live-preview");
        this.bindExistingUI();
        this.applyAudioGains();
        this.renderLogEntries();
        this.setStatus(this.state);
    }

    parseTags(value) {
        if (Array.isArray(value)) return value.filter(Boolean);
        return String(value || "")
            .split(",")
            .map((t) => t.trim())
            .filter(Boolean);
    }

    hydrateFormStateFromDom() {
        const loadedBasic = this.readBasicFormIntoState({ quiet: true });
        const loadedAdvanced = !loadedBasic && this.readAdvancedFormIntoState({ quiet: true });

        if (!loadedBasic && !loadedAdvanced) {
            this.formState = {
                title: "",
                category: "",
                tags: [],
                visibility: "public",
                inputMode: "camera",
                latencyMode: "NORMAL",
                autoRecord: false,
            };
        }

        this.inputMode = this.formState.inputMode || "camera";
        this.latencyMode = (this.formState.latencyMode || "NORMAL").toUpperCase() === "LOW" ? "LOW" : "NORMAL";
        this.autoRecord = !!this.formState.autoRecord;
    }

    readBasicFormIntoState(options = {}) {
        const quiet = options.quiet;
        const titleEl = document.getElementById("stream-title");
        const categoryEl = document.getElementById("stream-category");
        const tagsEl = document.getElementById("stream-tags");
        const inputModeEl = document.getElementById("input-mode");
        const latencyEl = document.getElementById("latency-mode");
        const autoRecordEl = document.getElementById("auto-record-toggle") || document.getElementById("auto-record");

        if (!titleEl && !categoryEl && !tagsEl) return false;

        let latencyMode = (latencyEl?.value || "NORMAL").toUpperCase();
        const inputMode = this.normalizeInputMode(inputModeEl?.value || "camera");
        const visibility = this.resolveVisibilityFromDom(this.formState.visibility || "public");
        let autoRecord = !!autoRecordEl?.checked;

        if (this.uiMode === "basic") {
            latencyMode = "NORMAL";
            autoRecord = false;
        }

        this.formState = {
            ...this.formState,
            title: titleEl?.value || "",
            category: categoryEl?.value || "",
            tags: this.parseTags(tagsEl?.value || []),
            visibility,
            inputMode,
            latencyMode: latencyMode === "LOW" ? "LOW" : "NORMAL",
            autoRecord,
        };

        this.inputMode = this.formState.inputMode;
        this.latencyMode = this.formState.latencyMode;
        this.autoRecord = this.formState.autoRecord;

        if (!quiet) {
            this.writeStateIntoAdvancedForm();
        }

        return true;
    }

    readAdvancedFormIntoState(options = {}) {
        const quiet = options.quiet;
        const titleEl = document.getElementById("adv-stream-title");
        const categoryEl = document.getElementById("adv-stream-category");
        const tagsEl = document.getElementById("adv-stream-tags");
        const inputModeEl = document.getElementById("adv-input-mode");
        const latencyEl = document.getElementById("adv-latency-mode");
        const autoRecordEl = document.getElementById("adv-auto-record");

        if (!titleEl && !categoryEl && !tagsEl && !inputModeEl && !latencyEl) return false;

        const latencyMode = (latencyEl?.value || "NORMAL").toUpperCase();
        const inputMode = this.normalizeInputMode(inputModeEl?.value || "camera");
        const visibility = this.resolveVisibilityFromDom(this.formState.visibility || "public");

        this.formState = {
            ...this.formState,
            title: titleEl?.value || "",
            category: categoryEl?.value || "",
            tags: this.parseTags(tagsEl?.value || []),
            visibility,
            inputMode,
            latencyMode: latencyMode === "LOW" ? "LOW" : "NORMAL",
            autoRecord: !!autoRecordEl?.checked,
        };

        this.inputMode = this.formState.inputMode;
        this.latencyMode = this.formState.latencyMode;
        this.autoRecord = this.formState.autoRecord;

        this.advancedPreferences = {
            latencyMode: this.formState.latencyMode,
            autoRecord: this.formState.autoRecord,
        };

        if (!quiet) {
            this.writeStateIntoBasicForm();
        }

        return true;
    }

    writeStateIntoBasicForm() {
        const titleEl = document.getElementById("stream-title");
        const categoryEl = document.getElementById("stream-category");
        const tagsEl = document.getElementById("stream-tags");
        const inputModeEl = document.getElementById("input-mode");
        const latencyEl = document.getElementById("latency-mode");
        const autoRecordEl = document.getElementById("auto-record-toggle") || document.getElementById("auto-record");

        if (titleEl) titleEl.value = this.formState.title || "";
        if (categoryEl) categoryEl.value = this.formState.category || "";
        if (tagsEl) tagsEl.value = Array.isArray(this.formState.tags) ? this.formState.tags.join(", ") : this.formState.tags || "";
        if (inputModeEl) {
            const nextMode = this.formState.inputMode === "external" ? "camera" : this.formState.inputMode;
            inputModeEl.value = nextMode || "camera";
        }
        if (latencyEl) latencyEl.value = (this.formState.latencyMode || "NORMAL").toUpperCase();
        if (autoRecordEl) autoRecordEl.checked = !!this.formState.autoRecord;

        this.updateVisibilityButtons();
    }

    writeStateIntoAdvancedForm() {
        const titleEl = document.getElementById("adv-stream-title");
        const categoryEl = document.getElementById("adv-stream-category");
        const tagsEl = document.getElementById("adv-stream-tags");
        const inputModeEl = document.getElementById("adv-input-mode");
        const latencyEl = document.getElementById("adv-latency-mode");
        const autoRecordEl = document.getElementById("adv-auto-record");

        if (titleEl) titleEl.value = this.formState.title || "";
        if (categoryEl) categoryEl.value = this.formState.category || "";
        if (tagsEl) tagsEl.value = Array.isArray(this.formState.tags) ? this.formState.tags.join(", ") : this.formState.tags || "";
        if (inputModeEl) inputModeEl.value = this.formState.inputMode || "camera";
        if (latencyEl) latencyEl.value = (this.formState.latencyMode || "NORMAL").toUpperCase();
        if (autoRecordEl) autoRecordEl.checked = !!this.formState.autoRecord;

        this.updateVisibilityButtons();
    }

    filterInputOptions() {
        const allowExternal = !this.isMobile;
        ["input-mode", "adv-input-mode"].forEach((id) => {
            const select = document.getElementById(id);
            if (!select) return;
            Array.from(select.options || []).forEach((opt) => {
                if (opt.value === "external") {
                    opt.disabled = !allowExternal;
                    opt.title = allowExternal ? "" : "External encoder is desktop-only";
                }
            });
            if (!allowExternal && select.value === "external") {
                select.value = "camera";
                this.formState.inputMode = "camera";
                this.inputMode = "camera";
                this.syncSceneForInput();
            }
        });
        const gate = document.getElementById("go-advanced-external");
        if (gate) gate.disabled = !allowExternal;
        const configSelect = document.getElementById("go-live-ui-config");
        if (configSelect) {
            const adv = configSelect.querySelector('option[value="advanced"]');
            if (adv) adv.disabled = !allowExternal;
            if (!allowExternal) configSelect.value = "basic";
        }
    }

    async refreshWebPermissions() {
        try {
            const stream = await navigator.mediaDevices.getUserMedia({ audio: true, video: true });
            stream.getTracks().forEach((track) => track.stop());
            await navigator.mediaDevices.enumerateDevices();
            this.log("Permissions refreshed; devices reloaded.");
        } catch (error) {
            this.log(`Permissions refresh failed: ${error?.message || error}`);
        }
    }

    resolveVisibilityFromDom(fallback = "public") {
        const active = document.querySelector("[data-go-live-visibility].active");
        const choice = active?.dataset?.goLiveVisibility || fallback;
        const normalizedChoice = choice === "private" ? "followers" : choice;
        this.visibilityChoice = normalizedChoice;
        return normalizedChoice === "public" ? "public" : "private";
    }

    updateVisibilityButtons() {
        const fallback = this.formState.visibility === "public" ? "public" : "followers";
        const target = this.visibilityChoice || fallback;
        document.querySelectorAll("[data-go-live-visibility]").forEach((btn) => {
            const isActive = btn.dataset?.goLiveVisibility === target;
            btn.classList.toggle("active", isActive);
            btn.setAttribute("aria-pressed", isActive ? "true" : "false");
        });
    }

    applyUIMode(mode, options = {}) {
        const requestedMode = mode === "advanced" ? "advanced" : "basic";
        const nextMode = this.isMobile && requestedMode === "advanced" ? "basic" : requestedMode;
        this.uiMode = nextMode;

        const basicView = document.getElementById("go-live-basic-view");
        const advancedView = document.getElementById("go-live-advanced-view");

        if (this.studioRoot) {
            this.studioRoot.classList.remove("ui-basic", "ui-advanced");
            this.studioRoot.classList.add(nextMode === "advanced" ? "ui-advanced" : "ui-basic");
        }

        if (basicView) basicView.classList.toggle("is-active", nextMode === "basic");
        if (advancedView) advancedView.classList.toggle("is-active", nextMode === "advanced");

        this.movePreviewIntoActiveSlot(nextMode);
        this.moveObsPanelIntoActiveSlot(nextMode);

        const modeSelect = document.getElementById("go-live-ui-config");
        if (modeSelect) modeSelect.value = nextMode;

        if (nextMode === "basic") {
            this.advancedPreferences = {
                latencyMode: this.formState.latencyMode || this.advancedPreferences.latencyMode,
                autoRecord:
                    typeof this.formState.autoRecord === "boolean"
                        ? this.formState.autoRecord
                        : this.advancedPreferences.autoRecord,
            };
            this.formState.latencyMode = "NORMAL";
            this.formState.autoRecord = false;
            this.latencyMode = "NORMAL";
            this.autoRecord = false;
        } else {
            this.formState.latencyMode = this.advancedPreferences.latencyMode || "NORMAL";
            this.formState.autoRecord = !!this.advancedPreferences.autoRecord;
            this.latencyMode = this.formState.latencyMode;
            this.autoRecord = this.formState.autoRecord;
        }

        if (!options.skipPersist) {
            try {
                localStorage.setItem(UI_MODE_STORAGE_KEY, nextMode);
                localStorage.setItem(LEGACY_UI_MODE_STORAGE_KEY, nextMode);
            } catch (err) {
                console.warn("[GoLive] failed to persist UI mode", err);
            }
        }

        if (nextMode === "advanced") {
            this.writeStateIntoAdvancedForm();
        } else {
            this.writeStateIntoBasicForm();
        }

        this.syncSceneForInput();
        this.renderSources();

        if (!options.skipLog) {
            this.log(`UI mode: ${nextMode}`);
        }
    }

    movePreviewIntoActiveSlot(mode) {
        if (!this.previewShell) return;
        const target = mode === "advanced" ? this.previewSlots.advanced : this.previewSlots.basic;
        if (target && this.previewShell.parentElement !== target) {
            target.appendChild(this.previewShell);
        }
    }

    moveObsPanelIntoActiveSlot(mode) {
        const obsPanel = document.getElementById("obs-panel");
        const target = mode === "advanced" ? this.obsSlots.advanced : this.obsSlots.basic;
        if (obsPanel && target && obsPanel.parentElement !== target) {
            target.appendChild(obsPanel);
        }
    }

    bindExistingUI() {
        if (this.uiBound) return;
        this.previewVideo = document.getElementById("live-preview");
        this.previewShell = document.getElementById("live-preview-shell");
        this.programVideo = document.getElementById("program-output");
        this.previewSlots = {
            basic: document.getElementById("live-preview-slot-basic"),
            advanced: document.getElementById("live-preview-slot-advanced"),
        };
        this.obsSlots = {
            basic: document.getElementById("obs-panel-slot-basic"),
            advanced: document.getElementById("obs-panel-slot-advanced"),
        };
        this.previewBadge = document.getElementById("preview-source-label");
        this.programBadge = document.getElementById("program-source-label");
        this.studioRoot = this.studioRoot || (this.root?.classList?.contains("go-live-studio") ? this.root : document.querySelector(".go-live-studio"));

        this.hydrateFormStateFromDom();
        this.writeStateIntoBasicForm();
        this.writeStateIntoAdvancedForm();
        this.renderSessionDetails();
        this.renderStats({ note: "Idle" });

        const modeSelect = document.getElementById("go-live-ui-config");
        if (modeSelect) {
            modeSelect.value = this.uiMode;
            modeSelect.addEventListener("change", (e) => {
                this.applyUIMode(e.target.value);
                this.persistUiSnapshot();
            });
        }

        const refreshPermissionsBtn = document.getElementById("refresh-permissions-btn");
        if (refreshPermissionsBtn) {
            refreshPermissionsBtn.addEventListener("click", () => this.refreshWebPermissions());
        }

        const legacyToggle = document.getElementById("go-live-mode-toggle-btn");
        if (legacyToggle) {
            legacyToggle.addEventListener("click", () => {
                const next = this.uiMode === "advanced" ? "basic" : "advanced";
                this.applyUIMode(next);
                this.persistUiSnapshot();
            });
        }

        const basicTitle = document.getElementById("stream-title");
        const basicCategory = document.getElementById("stream-category");
        const basicTags = document.getElementById("stream-tags");
        const basicInputMode = document.getElementById("input-mode");
        const basicLatency = document.getElementById("latency-mode");
        const basicAutoRecord = document.getElementById("auto-record-toggle") || document.getElementById("auto-record");
        const basicStart = document.getElementById("start-stream");
        const basicEnd = document.getElementById("end-stream");
        const basicPublic = document.getElementById("basic-visibility-public");
        const basicPrivate = document.getElementById("basic-visibility-private");
        const backButton = document.getElementById("go-live-back-button");

        [basicTitle, basicCategory, basicTags].forEach((el) => {
            if (!el) return;
            el.addEventListener("input", () => {
                this.readBasicFormIntoState();
                this.writeStateIntoAdvancedForm();
            });
        });

        if (basicInputMode)
            basicInputMode.addEventListener("change", (e) => {
                this.handleInputModeChange(e.target.value || "camera");
            });

        if (basicLatency)
            basicLatency.addEventListener("change", (e) => {
                this.formState.latencyMode = (e.target.value || "NORMAL").toUpperCase() === "LOW" ? "LOW" : "NORMAL";
                this.latencyMode = this.formState.latencyMode;
                this.writeStateIntoAdvancedForm();
            });

        if (basicAutoRecord)
            basicAutoRecord.addEventListener("change", (e) => {
                this.formState.autoRecord = !!e.target.checked;
                this.autoRecord = this.formState.autoRecord;
                this.writeStateIntoAdvancedForm();
            });

        if (basicPublic)
            basicPublic.addEventListener("click", () => {
                this.setVisibility("public");
                this.writeStateIntoAdvancedForm();
            });

        if (basicPrivate)
            basicPrivate.addEventListener("click", () => {
                this.setVisibility("private");
                this.writeStateIntoAdvancedForm();
            });

        if (basicStart)
            basicStart.addEventListener("click", async () => {
                this.readBasicFormIntoState();
                this.writeStateIntoAdvancedForm();
                try {
                    await this.primeMediaCaptureFromUserGesture();
                    await this.safeStart();
                } catch (error) {
                    console.error("[GoLive] pre-start capture failed", error);
                    this.log(`Pre-start capture failed: ${error.message || error}`);
                    this.setStatus("error", error.message || "Capture failed");
                }
            });

        if (basicEnd)
            basicEnd.addEventListener("click", () => {
                this.readBasicFormIntoState();
                this.writeStateIntoAdvancedForm();
                this.safeStop();
            });

        const advTitle = document.getElementById("adv-stream-title");
        const advCategory = document.getElementById("adv-stream-category");
        const advTags = document.getElementById("adv-stream-tags");
        const advInputMode = document.getElementById("adv-input-mode");
        const advLatency = document.getElementById("adv-latency-mode");
        const advAutoRecord = document.getElementById("adv-auto-record");
        const advStart = document.getElementById("adv-start-stream");
        const advEnd = document.getElementById("adv-end-stream");
        const advPublic = document.getElementById("adv-visibility-public");
        const advFollowers = document.getElementById("adv-visibility-followers");
        const advUnlisted = document.getElementById("adv-visibility-unlisted");

        [advTitle, advCategory, advTags].forEach((el) => {
            if (!el) return;
            el.addEventListener("input", () => {
                this.readAdvancedFormIntoState();
                this.writeStateIntoBasicForm();
            });
        });

        if (advInputMode)
            advInputMode.addEventListener("change", (e) => {
                this.handleInputModeChange(e.target.value || "camera");
            });

        if (advLatency)
            advLatency.addEventListener("change", (e) => {
                this.formState.latencyMode = (e.target.value || "NORMAL").toUpperCase() === "LOW" ? "LOW" : "NORMAL";
                this.latencyMode = this.formState.latencyMode;
                this.writeStateIntoBasicForm();
            });

        if (advAutoRecord)
            advAutoRecord.addEventListener("change", (e) => {
                this.formState.autoRecord = !!e.target.checked;
                this.autoRecord = this.formState.autoRecord;
                this.writeStateIntoBasicForm();
            });

        if (advPublic)
            advPublic.addEventListener("click", () => {
                this.setVisibility("public");
                this.writeStateIntoBasicForm();
            });

        if (advFollowers)
            advFollowers.addEventListener("click", () => {
                this.setVisibility("followers");
                this.writeStateIntoBasicForm();
            });

        if (advUnlisted)
            advUnlisted.addEventListener("click", () => {
                this.setVisibility("unlisted");
                this.writeStateIntoBasicForm();
            });

        if (advStart)
            advStart.addEventListener("click", async () => {
                this.readAdvancedFormIntoState();
                this.writeStateIntoBasicForm();
                try {
                    await this.primeMediaCaptureFromUserGesture();
                    await this.safeStart();
                } catch (error) {
                    console.error("[GoLive] pre-start capture failed", error);
                    this.log(`Pre-start capture failed: ${error.message || error}`);
                    this.setStatus("error", error.message || "Capture failed");
                }
            });

        if (advEnd)
            advEnd.addEventListener("click", () => {
                this.readAdvancedFormIntoState();
                this.writeStateIntoBasicForm();
                this.safeStop();
            });

        if (backButton)
            backButton.addEventListener("click", async () => {
                const isActive = this.state === "starting" || this.state === "live";
                if (isActive) {
                    const shouldEnd = confirm("End stream and go back?");
                    if (!shouldEnd) return;
                    try {
                        await this.safeStop();
                    } finally {
                        document.body.classList.remove("go-live-open");
                        if (window.goBack) window.goBack();
                        else if (window.history?.length > 1) window.history.back();
                        else if (window.navigateTo) window.navigateTo("feed");
                    }
                    return;
                }

                document.body.classList.remove("go-live-open");
                if (window.goBack) window.goBack();
                else if (window.history?.length > 1) window.history.back();
                else if (window.navigateTo) window.navigateTo("feed");
                else window.location.hash = "#feed";
            });

        this.bindTabs();
        this.bindSceneAndSources();
        this.bindMixerControls();
        this.bindExternalBridge();

        this.applyUIMode(this.uiMode, { skipPersist: true });
        this.syncControls();
        this.uiBound = true;
    }

    setStatus(state, message = "") {
        const previous = this.state;
        this.state = state;
        const monitorChip = document.getElementById("go-live-state-chip");
        const liveChip = document.getElementById("go-live-status");
        const previewChip = document.getElementById("go-live-visibility-pill");
        const helper = document.getElementById("go-live-status-text-secondary");
        const helperAdv = document.getElementById("go-live-status-text-secondary-adv");
        const liveIndicator = document.getElementById("program-live-indicator");
        const liveLabel = document.getElementById("program-live-label");
        const programNotes = document.getElementById("program-notes");

        const labels = {
            idle: "Idle",
            previewing: "Preview",
            starting: "Starting…",
            live: "Live",
            error: "Error",
        };

        const label = labels[state] || "Idle";
        const detail = message ? `${label} – ${message}` : label;
        const isLive = state === "live";
        const isStarting = state === "starting";

        if (monitorChip) {
            monitorChip.textContent = isLive ? "LIVE" : isStarting ? "Starting" : "Idle";
            monitorChip.classList.toggle("is-live", isLive);
            monitorChip.classList.toggle("is-idle", !isLive);
        }
        if (liveChip) {
            liveChip.textContent = "Live";
            liveChip.classList.toggle("is-live", isLive);
            liveChip.classList.toggle("is-idle", !isLive);
            liveChip.classList.toggle("active-live", isLive);
        }
        if (previewChip) {
            previewChip.textContent = this.inputMode === "external" ? "External" : "Preview";
            const previewActive = !isLive;
            previewChip.classList.toggle("is-active", previewActive);
            previewChip.classList.toggle("is-idle", previewActive);
            previewChip.classList.toggle("active-live", false);
        }
        if (helper) helper.textContent = message || (state === "live" ? "Streaming to your audience." : "Ready to preview.");
        if (helperAdv) helperAdv.textContent = detail;

        if (liveIndicator) {
            liveIndicator.classList.toggle("is-live", isLive);
            liveIndicator.classList.toggle("is-idle", !isLive);
        }
        if (liveLabel) {
            liveLabel.textContent = isLive ? "LIVE" : isStarting ? "Starting" : "Idle";
        }
        if (programNotes) {
            programNotes.textContent = detail;
        }

        if (previous !== state || message) {
            this.log(`State: ${detail}`);
        }

        if (state === "live") {
            if (!this.liveStartTime) this.liveStartTime = Date.now();
            this.startStatsPolling();
        } else if (state !== "starting") {
            this.stopStatsPolling();
            if (state === "idle" || state === "error") {
                this.liveStartTime = null;
                this.renderStats({ note: "Idle" });
            }
        }

        this.syncControls();
    }

    syncControls() {
        const startBtn = document.getElementById("start-stream");
        const endBtn = document.getElementById("end-stream");
        const advStart = document.getElementById("adv-start-stream");
        const advEnd = document.getElementById("adv-end-stream");

        const needsProgram = this.inputMode === "program";
        const hasProgram = !!(this.programStream?.getVideoTracks?.().length);
        const startDisabled = this.state === "starting" || this.state === "live" || (needsProgram && !hasProgram);
        const endDisabled =
            this.state === "idle" || this.state === "error" || this.state === "previewing" || this.state === "starting";

        if (startBtn) startBtn.disabled = startDisabled;
        if (advStart) advStart.disabled = startDisabled;
        if (endBtn) endBtn.disabled = endDisabled;
        if (advEnd) advEnd.disabled = endDisabled;

        const helperAdv = document.getElementById("go-live-status-text-secondary-adv");
        if (helperAdv && needsProgram && !hasProgram && this.uiMode === "advanced") {
            helperAdv.textContent = "Program has no active video source.";
        }

        const decorate = (btn, activeClass) => {
            if (!btn) return;
            btn.classList.add("go-live-action-btn");
            btn.classList.remove("btn-primary", "btn-danger", "btn-disabled");
            if (btn.disabled) {
                btn.classList.add("btn-disabled");
            } else if (activeClass) {
                btn.classList.add(activeClass);
            }
        };

        decorate(startBtn, "btn-primary");
        decorate(advStart, "btn-primary");
        decorate(endBtn, "btn-danger");
        decorate(advEnd, "btn-danger");
    }

    log(message) {
        const entry = `${new Date().toLocaleTimeString()} – ${message}`;
        this.logEntries.push(entry);
        if (this.logEntries.length > 200) this.logEntries.shift();
        console.log("[GoLive]", message);
        this.renderLogEntries();
    }

    describeTracks(stream) {
        if (!stream) return { video: "none", audio: "none" };
        const videoLabel = stream.getVideoTracks?.()[0]?.label || "none";
        const audioLabel = stream.getAudioTracks?.()[0]?.label || "none";
        return { video: videoLabel, audio: audioLabel };
    }

    renderLogEntries() {
        const logEl = document.getElementById("go-live-log");
        const advLogEl = document.getElementById("go-live-log-advanced");
        const recent = this.logEntries.slice(-50);

        if (logEl) {
            logEl.innerHTML = recent.map((line) => `<div class="log-line">${line}</div>`).join("");
            logEl.scrollTop = logEl.scrollHeight;
        }

        if (advLogEl) {
            advLogEl.textContent = recent.join("\n");
        }
    }

    renderSessionDetails() {
        const defaults = {
            id: "—",
            arn: "—",
            ingest: "—",
            playback: "—",
            latency: (this.formState.latencyMode || "NORMAL").toUpperCase(),
            autoRecord: this.formState.autoRecord ? "On" : "Off",
        };

        const details = {
            id: this.session?.sessionId || defaults.id,
            arn: this.session?.channelArn || defaults.arn,
            ingest: this.session?.ingestEndpoint || defaults.ingest,
            playback: this.session?.playbackUrl || defaults.playback,
            latency: this.session?.latencyMode || defaults.latency,
            autoRecord: this.session?.autoRecord ?? defaults.autoRecord,
        };

        const targets = [
            ["session-id-basic", details.id],
            ["session-arn-basic", details.arn],
            ["session-ingest-basic", details.ingest],
            ["session-playback-basic", details.playback],
            ["session-latency-basic", details.latency],
            ["session-auto-basic", details.autoRecord === true ? "On" : details.autoRecord === false ? "Off" : details.autoRecord],
            ["session-id-advanced", details.id],
            ["session-arn-advanced", details.arn],
            ["session-ingest-advanced", details.ingest],
            ["session-playback-advanced", details.playback],
            ["session-latency-advanced", details.latency],
            ["session-auto-advanced", details.autoRecord === true ? "On" : details.autoRecord === false ? "Off" : details.autoRecord],
        ];

        targets.forEach(([id, value]) => {
            const el = document.getElementById(id);
            if (el) el.textContent = value ?? "—";
        });
    }

    renderStats(stats = {}) {
        const formatter = (val, suffix = "") => {
            if (val === undefined || val === null || Number.isNaN(val)) return "—";
            if (typeof val === "number") {
                return `${val}${suffix}`;
            }
            return String(val);
        };

        const durationMs = this.liveStartTime ? Date.now() - this.liveStartTime : 0;
        const duration = new Date(durationMs).toISOString().substring(11, 19);
        const audioTracks = this.stream?.getAudioTracks()?.length || 0;
        const videoTracks = this.stream?.getVideoTracks()?.length || 0;

        const fields = {
            "adv-stat-ingest": stats.ingestEndpoint || this.session?.ingestEndpoint || "—",
            "adv-stat-bitrate": formatter(stats.bitrate, " kbps"),
            "adv-stat-rtt": formatter(stats.rtt, " ms"),
            "adv-stat-dropped": formatter(stats.droppedFrames),
            "adv-stat-cpu": formatter(stats.cpuPercentage, "%"),
            "adv-stat-duration": duration,
            "adv-stat-tracks": `${videoTracks} video / ${audioTracks} audio`,
            "adv-stat-mode": this.formState.inputMode || this.inputMode,
            "adv-stat-health": stats.note || (stats.available === false ? "Stats unavailable in this SDK build" : "Monitoring"),
        };

        Object.entries(fields).forEach(([id, value]) => {
            const el = document.getElementById(id);
            if (el) el.textContent = value ?? "—";
        });

        const programTimer = document.getElementById("program-timer");
        if (programTimer) programTimer.textContent = duration;
    }

    startStatsPolling() {
        if (this.statsInterval) return;
        this.statsInterval = window.setInterval(() => this.collectStats(), 1500);
    }

    stopStatsPolling() {
        if (this.statsInterval) {
            clearInterval(this.statsInterval);
            this.statsInterval = null;
        }
    }

    collectStats() {
        if (!this.client) {
            this.renderStats({ available: false, note: "Client not initialized" });
            return;
        }

        if (typeof this.client.getStats === "function") {
            try {
                const stats = this.client.getStats();
                this.renderStats({
                    available: true,
                    bitrate: stats?.bitrates?.audio || stats?.bitrates?.video || stats?.bitrate,
                    rtt: stats?.rtt,
                    droppedFrames: stats?.droppedFrames,
                    cpuPercentage: stats?.cpuPercentage,
                    ingestEndpoint: stats?.ingestEndpoint || this.session?.ingestEndpoint,
                });
                return;
            } catch (err) {
                console.warn("[GoLive] stats unavailable", err);
                this.log(`Stats unavailable: ${err.message || err}`);
            }
        }

        this.renderStats({ available: false });
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
        const micGain = document.getElementById("mixer-mic-fader");
        const systemGain = document.getElementById("mixer-system-fader");
        this.mixerState.mic.gain = this.audioGains.mic ?? this.mixerState.mic.gain;
        this.mixerState.system.gain = this.audioGains.system ?? this.mixerState.system.gain;
        if (micGain) micGain.value = this.mixerState.mic.gain;
        if (systemGain) systemGain.value = this.mixerState.system.gain;
        this.updateMixerUi();
        this.applyAudioGainToGraph();
    }

    async setupAudioPipeline(stream) {
        this.teardownAudioGraph();
        const track = stream?.getAudioTracks?.()[0];
        this.activeMeterChannel = this.getActiveAudioChannel();
        if (!track || !this.activeMeterChannel) {
            this.renderMeterLevels();
            return;
        }

        this.audioContext = this.audioContext || new AudioContext();
        if (this.audioContext.state === "suspended") {
            try {
                await this.audioContext.resume();
            } catch (_) {
                // ignore resume failures; meters will remain idle
            }
        }

        const source = this.audioContext.createMediaStreamSource(new MediaStream([track]));
        const gainNode = this.audioContext.createGain();
        const analyser = this.audioContext.createAnalyser();
        analyser.fftSize = 512;
        const destination = this.audioContext.createMediaStreamDestination();

        source.connect(gainNode);
        gainNode.connect(analyser);
        analyser.connect(destination);

        this.meterSourceNode = source;
        this.meterGainNode = gainNode;
        this.meterAnalyser = analyser;
        this.meterDestination = destination;
        this.processedAudioStream = destination.stream;

        this.applyAudioGainToGraph();
        this.startMeterAnimation();
    }

    teardownAudioGraph() {
        this.stopMeterAnimation();
        [this.meterSourceNode, this.meterGainNode, this.meterAnalyser].forEach((node) => {
            try {
                node?.disconnect?.();
            } catch (_) {
                /* noop */
            }
        });
        if (this.meterDestination?.stream) {
            this.meterDestination.stream.getTracks().forEach((t) => t.stop());
        }
        this.meterSourceNode = null;
        this.meterGainNode = null;
        this.meterAnalyser = null;
        this.meterDestination = null;
        this.processedAudioStream = null;
        this.activeMeterChannel = null;
    }

    getBroadcastAudioTrack(fallbackTrack) {
        const processed = this.processedAudioStream?.getAudioTracks?.()[0];
        return processed || fallbackTrack || null;
    }

    bindExternalBridge() {
        const externalLink = document.getElementById("go-advanced-external");
        if (externalLink) {
            externalLink.addEventListener("click", () => {
                this.applyUIMode("advanced");
                this.handleInputModeChange("external");
                this.writeStateIntoAdvancedForm();
                this.persistUiSnapshot();
                this.log("Switched to advanced for external encoder");
                const advancedView = document.getElementById("go-live-advanced-view");
                if (advancedView && typeof advancedView.scrollIntoView === "function") {
                    advancedView.scrollIntoView({ behavior: "smooth", block: "start" });
                }
            });
        }
    }

    bindTabs() {
        const tabs = Array.from(document.querySelectorAll("[data-tab-target]"));
        const panels = Array.from(document.querySelectorAll("[data-tab-panel]"));
        if (!tabs.length || !panels.length) return;

        const activateTab = (target) => {
            tabs.forEach((tab) => {
                const isActive = tab.dataset.tabTarget === target;
                tab.classList.toggle("active", isActive);
                tab.setAttribute("aria-selected", isActive ? "true" : "false");
            });
            panels.forEach((panel) => {
                const match = panel.dataset.tabPanel === target;
                panel.classList.toggle("show", match);
                panel.toggleAttribute("hidden", !match);
                panel.setAttribute("aria-hidden", match ? "false" : "true");
            });
        };

        tabs.forEach((tab) => {
            tab.addEventListener("click", () => activateTab(tab.dataset.tabTarget));
            tab.addEventListener("keydown", (e) => {
                if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    activateTab(tab.dataset.tabTarget);
                }
            });
        });

        const defaultTab = tabs.find((t) => t.classList.contains("active"))?.dataset.tabTarget || tabs[0]?.dataset.tabTarget;
        if (defaultTab) activateTab(defaultTab);
    }

    bindSceneAndSources() {
        this.sceneListEl = document.getElementById("scene-list");
        this.sourceListEl = document.getElementById("source-list");
        const addSceneBtn = document.getElementById("add-scene-btn");
        const removeSceneBtn = document.getElementById("remove-scene-btn");
        const addSourceBtn = document.getElementById("add-source-btn");
        const removeSourceBtn = document.getElementById("remove-source-btn");
        const cutBtn = document.getElementById("transition-cut");
        const fadeBtn = document.getElementById("transition-fade");
        const autoBtn = document.getElementById("transition-auto");

        if (addSceneBtn) addSceneBtn.addEventListener("click", () => this.addScene());
        if (removeSceneBtn) removeSceneBtn.addEventListener("click", () => this.removeScene());
        if (addSourceBtn) addSourceBtn.addEventListener("click", () => this.addSource());
        if (removeSourceBtn) removeSourceBtn.addEventListener("click", () => this.removeSource());
        if (cutBtn)
            cutBtn.addEventListener("click", () => {
                this.applyTransition("cut");
            });
        if (fadeBtn)
            fadeBtn.addEventListener("click", () => {
                this.applyTransition("fade");
            });
        if (autoBtn)
            autoBtn.addEventListener("click", () => {
                this.applyTransition("auto");
            });

        if (!this.sceneMenuOutsideHandler) {
            this.sceneMenuOutsideHandler = (event) => {
                if (!event.target.closest(".scene-menu") && !event.target.closest(".scene-menu-trigger")) {
                    this.closeSceneMenus();
                }
            };
            document.addEventListener("click", this.sceneMenuOutsideHandler);
        }

        this.selectedSceneId = this.activePreviewSceneId;
        this.syncSceneForInput();
        this.renderScenes();
        this.renderSources();
    }

    bindMixerControls() {
        const faders = document.querySelectorAll(".mixer-fader");
        faders.forEach((fader) => {
            const channel = fader.dataset.channel;
            if (!channel || !this.mixerState[channel]) return;
            fader.value = this.mixerState[channel].gain;
            fader.addEventListener("input", (e) => {
                const next = Number(e.target.value);
                this.mixerState[channel].gain = next;
                if (channel === "mic" || channel === "system") {
                    this.persistAudioGains({ [channel]: next });
                }
                this.updateMixerUi();
                this.applyAudioGainToGraph();
            });
        });

        const muteButtons = document.querySelectorAll(".mixer-mute");
        muteButtons.forEach((btn) => {
            const channel = btn.dataset.channel;
            if (!channel || !this.mixerState[channel]) return;
            btn.addEventListener("click", () => {
                this.mixerState[channel].muted = !this.mixerState[channel].muted;
                this.updateMixerUi();
                this.applyAudioGainToGraph();
            });
        });

        this.updateMixerUi();
        this.applyAudioGainToGraph();
        this.startMeterAnimation();
    }

    getActiveAudioChannel() {
        if (this.inputMode === "screen") return "system";
        if (this.inputMode === "external") return null;
        return "mic";
    }

    renderMeterLevels(levels = {}) {
        const defaults = { mic: 0, system: 0, music: 0, aux: 0 };
        const merged = { ...defaults, ...levels };
        Object.entries(merged).forEach(([channel, value]) => {
            const fill = document.querySelector(`.mixer-strip[data-channel="${channel}"] .meter-fill`);
            const state = this.mixerState[channel] || {};
            if (!fill) return;
            const clamped = Math.max(0, Math.min(100, value));
            const effective = state.muted ? 0 : clamped;
            fill.style.setProperty("--meter-fill", `${effective}%`);
            fill.style.opacity = state.muted ? "0.3" : "1";
        });
    }

    updateMixerUi() {
        Object.entries(this.mixerState).forEach(([channel, state]) => {
            const strip = document.querySelector(`.mixer-strip[data-channel="${channel}"]`);
            if (!strip) return;
            const fader = strip.querySelector(".mixer-fader");
            const muteBtn = strip.querySelector(".mixer-mute");
            if (fader) {
                fader.value = state.gain;
            }
            if (muteBtn) {
                muteBtn.classList.toggle("active", state.muted);
                muteBtn.textContent = state.muted ? "Unmute" : "Mute";
            }
        });
        this.renderMeterLevels();
    }

    applyAudioGainToGraph() {
        const channel = this.activeMeterChannel || this.getActiveAudioChannel();
        const state = channel ? this.mixerState[channel] : null;
        if (this.meterGainNode && state) {
            this.meterGainNode.gain.value = state.muted ? 0 : (state.gain ?? 100) / 100;
        }
    }

    startMeterAnimation() {
        this.stopMeterAnimation();
        const analyser = this.meterAnalyser;
        const activeChannel = this.getActiveAudioChannel();
        if (!analyser || !activeChannel) {
            this.renderMeterLevels();
            return;
        }

        const buffer = new Uint8Array(analyser.fftSize || 512);
        const animate = () => {
            if (!this.meterAnalyser) {
                this.renderMeterLevels();
                return;
            }
            this.meterAnalyser.getByteTimeDomainData(buffer);
            let sumSquares = 0;
            buffer.forEach((v) => {
                const normalized = (v - 128) / 128;
                sumSquares += normalized * normalized;
            });
            const rms = Math.sqrt(sumSquares / buffer.length);
            const level = Math.min(100, Math.max(0, rms * 140));
            const state = this.mixerState[activeChannel] || {};
            const adjusted = state.muted ? 0 : level * (state.gain ?? 100) / 100;

            this.renderMeterLevels({
                mic: activeChannel === "mic" ? adjusted : 0,
                system: activeChannel === "system" ? adjusted : 0,
                music: 0,
                aux: 0,
            });

            this.meterRaf = window.requestAnimationFrame(animate);
        };

        animate();
    }

    stopMeterAnimation() {
        if (this.meterRaf) {
            window.cancelAnimationFrame(this.meterRaf);
            this.meterRaf = null;
        }
        this.renderMeterLevels();
    }

    syncSceneForInput() {
        const mode = this.inputMode;
        if (mode === "external") {
            this.activePreviewSceneId = "scene-external";
        } else if (mode === "screen") {
            this.activePreviewSceneId = "scene-screen";
        } else {
            this.activePreviewSceneId = "scene-main";
        }
        this.selectedSceneId = this.activePreviewSceneId;
        this.renderScenes();
    }

    renderScenes() {
        if (!this.sceneListEl) return;
        this.sceneListEl.innerHTML = "";
        if (!this.selectedSceneId && this.scenes.length) {
            this.selectedSceneId = this.scenes[0].id;
        }

        this.scenes.forEach((scene, index) => {
            const row = document.createElement("div");
            row.className = "scene-row";
            row.dataset.sceneId = scene.id;
            row.setAttribute("role", "button");
            row.tabIndex = 0;
            const statusLabel =
                scene.id === this.activeProgramSceneId
                    ? "Program"
                    : scene.id === this.activePreviewSceneId
                    ? "Preview"
                    : "Standby";
            if (scene.id === this.activePreviewSceneId) row.classList.add("active");
            if (scene.id === this.activeProgramSceneId) row.classList.add("program");
            if (scene.id === this.selectedSceneId) row.classList.add("selected");

            const labels = document.createElement("div");
            labels.className = "scene-labels";
            labels.innerHTML = `<span>${scene.name}</span><span class="muted">${statusLabel}</span>`;

            const menuTrigger = document.createElement("button");
            menuTrigger.type = "button";
            menuTrigger.className = "scene-menu-trigger";
            menuTrigger.setAttribute("aria-label", `Scene ${index + 1} options`);
            menuTrigger.innerHTML = "⋯";

            const menu = document.createElement("div");
            menu.className = "scene-menu";
            const renameBtn = document.createElement("button");
            renameBtn.type = "button";
            renameBtn.textContent = "Rename";
            renameBtn.addEventListener("click", (e) => {
                e.stopPropagation();
                this.renameScene(scene.id);
                this.closeSceneMenus();
            });
            const deleteBtn = document.createElement("button");
            deleteBtn.type = "button";
            deleteBtn.textContent = "Delete";
            deleteBtn.addEventListener("click", (e) => {
                e.stopPropagation();
                this.removeScene(scene.id);
                this.closeSceneMenus();
            });
            menu.append(renameBtn, deleteBtn);

            menuTrigger.addEventListener("click", (e) => {
                e.stopPropagation();
                this.toggleSceneMenu(menu);
            });

            row.addEventListener("click", () => {
                this.selectedSceneId = scene.id;
                this.setPreviewScene(scene.id);
                this.closeSceneMenus();
            });

            row.addEventListener("keydown", (event) => {
                if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    this.selectedSceneId = scene.id;
                    this.setPreviewScene(scene.id);
                    this.closeSceneMenus();
                }
            });

            row.append(labels, menuTrigger, menu);
            this.sceneListEl.appendChild(row);
        });
        this.updateSceneBadges();
    }

    toggleSceneMenu(menu) {
        if (!menu) return;
        const isOpen = menu.classList.contains("open");
        this.closeSceneMenus();
        menu.classList.toggle("open", !isOpen);
    }

    closeSceneMenus() {
        document.querySelectorAll(".scene-menu.open").forEach((menu) => menu.classList.remove("open"));
    }

    renameScene(sceneId) {
        const scene = this.scenes.find((s) => s.id === sceneId);
        if (!scene) return;
        const nextName = prompt("Rename scene", scene.name);
        if (!nextName) return;
        scene.name = nextName.trim();
        this.renderScenes();
        this.updateSceneBadges();
    }

    addScene() {
        const nextIndex = this.scenes.length + 1;
        const id = `scene-${Date.now()}`;
        this.scenes.push({ id, name: `Scene ${nextIndex}`, sources: [] });
        this.activePreviewSceneId = this.activePreviewSceneId || id;
        this.selectedSceneId = id;
        this.renderScenes();
        this.updateSceneBadges();
    }

    removeScene(sceneId = this.selectedSceneId || this.activePreviewSceneId) {
        if (!sceneId) return;
        if (this.scenes.length <= 1) {
            alert("Keep at least one scene available.");
            return;
        }
        const scene = this.scenes.find((s) => s.id === sceneId);
        if (!scene) return;
        const shouldDelete = confirm("Are you sure?");
        if (!shouldDelete) return;
        this.scenes = this.scenes.filter((s) => s.id !== sceneId);
        if (this.activePreviewSceneId === sceneId) this.activePreviewSceneId = this.scenes[0]?.id || null;
        if (this.activeProgramSceneId === sceneId) this.activeProgramSceneId = this.scenes[0]?.id || null;
        if (this.selectedSceneId === sceneId) this.selectedSceneId = this.scenes[0]?.id || null;
        this.renderScenes();
        this.updateSceneBadges();
    }

    renderSources() {
        if (!this.sourceListEl) return;
        this.sourceListEl.innerHTML = "";
        if (!this.selectedSourceId && this.sources.length) {
            this.selectedSourceId = this.sources[0].id;
        }
        this.sources.forEach((source) => {
            const row = document.createElement("button");
            row.type = "button";
            row.className = "source-row";
            row.dataset.sourceId = source.id;
            const isActive = this.selectedSourceId === source.id;
            row.classList.toggle("active", isActive);
            row.textContent = source.name;
            row.addEventListener("click", () => this.handleSourceSelect(source.id));
            this.sourceListEl.appendChild(row);
        });
    }

    addSource() {
        const nextIndex = this.sources.length + 1;
        const id = `source-${Date.now()}`;
        this.sources.push({ id, name: `Source ${nextIndex}`, type: "camera" });
        this.selectedSourceId = id;
        this.handleSourceSelect(id);
    }

    removeSource(sourceId = this.selectedSourceId) {
        if (!sourceId || this.sources.length <= 1) {
            alert("Keep at least one source available.");
            return;
        }
        const source = this.sources.find((s) => s.id === sourceId);
        if (!source) return;
        const shouldDelete = confirm("Are you sure?");
        if (!shouldDelete) return;
        this.sources = this.sources.filter((s) => s.id !== sourceId);
        if (this.selectedSourceId === sourceId) {
            const fallback = this.sources[0];
            this.selectedSourceId = fallback?.id || null;
            if (fallback) {
                this.handleSourceSelect(fallback.id);
                return;
            }
        }
        this.renderSources();
    }

    setPreviewScene(sceneId) {
        const exists = this.scenes.some((s) => s.id === sceneId);
        if (!exists) return;
        this.activePreviewSceneId = sceneId;
        this.renderScenes();
    }

    updateSceneBadges() {
        const previewLabel = document.getElementById("preview-scene-label");
        const programLabel = document.getElementById("program-scene-label");
        const previewScene = this.scenes.find((s) => s.id === this.activePreviewSceneId);
        const programScene = this.scenes.find((s) => s.id === this.activeProgramSceneId);
        if (previewLabel) previewLabel.textContent = previewScene?.name || "Preview";
        if (programLabel) programLabel.textContent = programScene?.name || "Program";
        this.updateMonitorBadges();
    }

    updateMonitorBadges() {
        const previewSource = this.sources.find((s) => s.id === this.selectedSourceId) || this.sources[0];
        const programSource = this.sources.find((s) => s.id === this.activeProgramSourceId) || previewSource;
        if (this.previewBadge) {
            this.previewBadge.textContent = previewSource?.name || "Source";
        }
        if (this.programBadge) {
            this.programBadge.textContent = programSource?.name || "Program Source";
        }
    }

    applyTransition(mode = "cut") {
        this.activeProgramSceneId = this.activePreviewSceneId;
        this.activeProgramSourceId = this.selectedSourceId;
        this.programStream = this.stream || this.programStream;
        if (this.programVideo && this.programStream) {
            this.programVideo.srcObject = this.programStream;
        }
        this.renderScenes();
        const notes = document.getElementById("program-notes");
        if (notes) notes.textContent = mode === "fade" ? "Fade transition applied." : "Scene pushed live.";
        this.updateMonitorBadges();
    }

    handleSourceSelect(sourceId) {
        const source = this.sources.find((s) => s.id === sourceId);
        if (!source) return;
        if (this.state === "live") {
            this.log("Stop stream to change source while live.");
            const notes = document.getElementById("program-notes");
            if (notes) notes.textContent = "Stop stream to change source.";
            return;
        }
        this.selectedSourceId = sourceId;
        this.handleInputModeChange(source.type);
        this.syncSceneForInput();
        if (!this.stream || this.stream.getTracks().every((track) => track.readyState === "ended")) {
            this.primeMediaCaptureFromUserGesture().catch((err) => this.log(err?.message || String(err)));
        }
        this.updateMonitorBadges();
    }

    updateEncoderTab() {
        const encoderPlaceholder = document.getElementById("encoder-tab-placeholder");
        if (!encoderPlaceholder) return;
        if (this.inputMode === "external") {
            const ingest = this.session?.ingestEndpoint || "Ingest pending…";
            const keyState = this.session?.streamKey ? "Stream key ready" : "Stream key pending";
            encoderPlaceholder.textContent = `RTMP ingest: ${ingest} · ${keyState}`;
        } else {
            encoderPlaceholder.textContent = "Encoder configuration placeholder.";
        }
    }

    settingsPayload(overrides = {}) {
        const state = { ...this.formState, ...overrides };
        const tags = Array.isArray(state.tags)
            ? state.tags
            : state.tags
            ? String(state.tags)
                  .split(",")
                  .map((t) => t.trim())
                  .filter(Boolean)
            : Array.isArray(this.session?.tags)
            ? this.session.tags
            : [];

        const title = state.title ?? this.session?.title ?? "";
        const category = state.category ?? this.session?.category ?? "";
        const visibility = state.visibility ?? this.session?.visibility ?? "public";

        return {
            inputMode: state.inputMode || this.inputMode,
            audioMode: this.audioMode,
            latencyMode: (state.latencyMode || this.latencyMode) === "LOW" ? "LOW" : "NORMAL",
            autoRecord: !!state.autoRecord,
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
        if ((this.formState.visibility || this.session.visibility) !== "private") return;
        try {
            await setDoc(
                doc(this.db, "liveStreams", this.session.sessionId, "private", "keys"),
                { uid, streamKey: this.session.streamKey, updatedAt: serverTimestamp() },
                { merge: true }
            );
            await updateDoc(doc(this.db, "liveStreams", this.session.sessionId), { streamKey: deleteField() });
        } catch (error) {
            console.warn("[GoLive] failed to persist private stream key", error);
            this.log(`Persist key failed (non-blocking): ${error.message || error}`);
        }
    }

    // ----------------------------------------------
    // Start Stream
    // ----------------------------------------------
    async primeMediaCaptureFromUserGesture() {
        const mode = this.formState.inputMode || "camera";
        this.inputMode = mode;

        if (mode === "external") return;

        if (this.stream) {
            this.stream.getTracks().forEach((t) => t.stop());
            this.stream = null;
        }

        const api = mode === "screen" ? "getDisplayMedia" : "getUserMedia";
        this.log(`Preparing media capture for inputMode=${mode} using ${api} (pre-flight)`);
        console.info("[GoLive] capture selection", { mode, api });

        const stream =
            mode === "screen"
                ? await navigator.mediaDevices.getDisplayMedia({ video: true, audio: true })
                : await navigator.mediaDevices.getUserMedia({ video: true, audio: true });

        this.stream = stream;
        const summary = this.describeTracks(stream);
        this.log(`Capture summary: mode=${mode} api=${api} video=${summary.video} audio=${summary.audio}`);
        if (this.previewVideo) {
            this.previewVideo.srcObject = stream;
        }

        await this.setupAudioPipeline(stream);
    }

    async safeStart() {
        if (this.state === "starting" || this.state === "live") {
            this.log("Start ignored: already starting or live");
            return;
        }
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
        if (this.uiMode === "advanced") {
            this.readAdvancedFormIntoState();
        } else {
            this.readBasicFormIntoState();
        }

        const effectiveLatency =
            this.uiMode === "basic"
                ? "NORMAL"
                : (this.formState.latencyMode || this.advancedPreferences.latencyMode || "NORMAL").toUpperCase() === "LOW"
                ? "LOW"
                : "NORMAL";
        const effectiveAutoRecord = this.uiMode === "basic" ? false : !!this.formState.autoRecord;

        if (this.uiMode === "basic") {
            this.log("[GoLive] Basic mode start: forcing latency=NORMAL autoRecord=false");
        }

        const state = {
            ...this.formState,
            latencyMode: effectiveLatency,
            inputMode: this.formState.inputMode || "camera",
            autoRecord: effectiveAutoRecord,
            tags: Array.isArray(this.formState.tags) ? this.formState.tags : this.parseTags(this.formState.tags),
        };

        const title = state.title || "";
        const category = state.category || "";
        const tags = state.tags || [];
        const visibility = state.visibility || "public";

        this.inputMode = state.inputMode;
        this.latencyMode = state.latencyMode;
        this.autoRecord = !!state.autoRecord;

        const user = this.auth?.currentUser;
        if (!user) {
            throw new Error("User must be signed in to start streaming");
        }

        const idToken = await user.getIdToken();

        const payload = {
            title,
            category,
            tags,
            latencyMode: state.latencyMode,
            autoRecord: !!state.autoRecord,
            visibility,
        };

        if (this.inputMode === "program" && (!this.programStream || !this.programStream.getVideoTracks().length)) {
            throw new Error("Program has no active video source");
        }

        this.log(
            `Start config -> visibility:${visibility}, latency:${state.latencyMode}, autoRecord:${!!state.autoRecord}, inputMode:${state.inputMode}`
        );
        this.log(`Calling createEphemeralChannel with ${JSON.stringify(payload)}`);

        const response = await fetch(
            "https://us-central1-spike-streaming-service.cloudfunctions.net/createEphemeralChannel",
            {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    Authorization: `Bearer ${idToken}`,
                },
                body: JSON.stringify(payload),
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
            latencyMode: state.latencyMode,
            autoRecord: !!state.autoRecord,
        };

        this.log("createEphemeralChannel response received");

        this.renderSessionDetails();
        this.updateEncoderTab();

        await this.persistLiveSnapshot({
            title,
            category,
            tags,
            visibility,
        });

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
        this.log("IVS Broadcast SDK loaded");

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

        if (this.stream) {
            this.log(`Using pre-captured media for inputMode=${this.inputMode}`);
        } else {
            const api = this.inputMode === "screen" ? "getDisplayMedia" : "getUserMedia";
            this.log(`Preparing media capture for inputMode=${this.inputMode} using ${api}`);
            console.info("[GoLive] broadcast capture", { mode: this.inputMode, api });
            this.stream =
                this.inputMode === "screen"
                    ? await navigator.mediaDevices.getDisplayMedia({ video: true, audio: true })
                    : await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
            const summary = this.describeTracks(this.stream);
            this.log(`Capture summary: mode=${this.inputMode} api=${api} video=${summary.video} audio=${summary.audio}`);
        }

        this.previewVideo.srcObject = this.stream;

        await this.setupAudioPipeline(this.stream);

        // Split tracks into clean MediaStreams for IVS SDK
        const outboundVideoStream = this.inputMode === "program" ? this.programStream : this.stream;
        const vTrack = outboundVideoStream?.getVideoTracks?.()[0] || null;
        const aTrack = this.stream.getAudioTracks()[0] || null;

        if (!vTrack) {
            throw new Error("No video track available from capture source");
        }

        const videoStream = new MediaStream([vTrack]);
        const broadcastAudio = this.getBroadcastAudioTrack(aTrack);
        const audioStream = broadcastAudio ? new MediaStream([broadcastAudio]) : null;

        // IMPORTANT: provide a name AND a VideoComposition
        await this.client.addVideoInputDevice(videoStream, "video1", { index: 0 });

        if (audioStream) {
            await this.client.addAudioInputDevice(audioStream, "audio1");
        }

        await this.client.startBroadcast(this.session.streamKey);

        this.liveStartTime = this.liveStartTime || Date.now();

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
        this.startStatsPolling();
        this.collectStats();
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

        this.updateEncoderTab();

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
        if (this.state === "idle" || this.state === "error") {
            this.log("Stop ignored: already idle");
            return;
        }
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

        if (this.programStream) {
            this.programStream.getTracks().forEach((t) => t.stop());
            this.programStream = null;
        }

        if (this.programVideo) {
            this.programVideo.srcObject = null;
        }

        this.teardownAudioGraph();

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
        this.liveStartTime = null;
        this.stopStatsPolling();
        this.stopMeterAnimation();
        this.renderSessionDetails();
        this.renderStats({ note: "Idle" });
        this.updateEncoderTab();
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
