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

        this.studioRoot = null;

        this.session = null;
        this.client = null;
        this.stream = null;
        this.previewVideo = null;
        this.previewShell = null;
        this.previewSlots = { basic: null, advanced: null };
        this.obsSlots = { basic: null, advanced: null };
        this.unsubscribeLiveDoc = null;

        const storedMode =
            localStorage.getItem(UI_MODE_STORAGE_KEY) || localStorage.getItem(LEGACY_UI_MODE_STORAGE_KEY) || "basic";
        this.uiMode = storedMode === "advanced" ? "advanced" : "basic";
        this.logEntries = [];
        this.audioGains = this.loadAudioGains();
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

        const latencyMode = (latencyEl?.value || "NORMAL").toUpperCase();
        const inputMode = inputModeEl?.value || "camera";
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
        const inputMode = inputModeEl?.value || "camera";
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
        if (inputModeEl) inputModeEl.value = this.formState.inputMode || "camera";
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

    resolveVisibilityFromDom(fallback = "public") {
        const active = document.querySelector("[data-go-live-visibility].active");
        return active?.dataset?.goLiveVisibility || fallback;
    }

    updateVisibilityButtons() {
        const isPublic = (this.formState.visibility || "public") === "public";
        const target = isPublic ? "public" : "private";
        document.querySelectorAll("[data-go-live-visibility]").forEach((btn) => {
            const isActive = btn.dataset?.goLiveVisibility === target;
            btn.classList.toggle("active", isActive);
            btn.setAttribute("aria-pressed", isActive ? "true" : "false");
        });
    }

    applyUIMode(mode, options = {}) {
        const nextMode = mode === "advanced" ? "advanced" : "basic";
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
        this.previewSlots = {
            basic: document.getElementById("live-preview-slot-basic"),
            advanced: document.getElementById("live-preview-slot-advanced"),
        };
        this.obsSlots = {
            basic: document.getElementById("obs-panel-slot-basic"),
            advanced: document.getElementById("obs-panel-slot-advanced"),
        };
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
                this.formState.inputMode = e.target.value || "camera";
                this.inputMode = this.formState.inputMode;
                this.writeStateIntoAdvancedForm();
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
                this.formState.visibility = "public";
                this.updateVisibilityButtons();
                this.writeStateIntoAdvancedForm();
            });

        if (basicPrivate)
            basicPrivate.addEventListener("click", () => {
                this.formState.visibility = "private";
                this.updateVisibilityButtons();
                this.writeStateIntoAdvancedForm();
            });

        if (basicStart)
            basicStart.addEventListener("click", () => {
                this.readBasicFormIntoState();
                this.writeStateIntoAdvancedForm();
                this.safeStart();
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
        const advPrivate = document.getElementById("adv-visibility-private");

        [advTitle, advCategory, advTags].forEach((el) => {
            if (!el) return;
            el.addEventListener("input", () => {
                this.readAdvancedFormIntoState();
                this.writeStateIntoBasicForm();
            });
        });

        if (advInputMode)
            advInputMode.addEventListener("change", (e) => {
                this.formState.inputMode = e.target.value || "camera";
                this.inputMode = this.formState.inputMode;
                this.writeStateIntoBasicForm();
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
                this.formState.visibility = "public";
                this.updateVisibilityButtons();
                this.writeStateIntoBasicForm();
            });

        if (advPrivate)
            advPrivate.addEventListener("click", () => {
                this.formState.visibility = "private";
                this.updateVisibilityButtons();
                this.writeStateIntoBasicForm();
            });

        if (advStart)
            advStart.addEventListener("click", () => {
                this.readAdvancedFormIntoState();
                this.writeStateIntoBasicForm();
                this.safeStart();
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
                        else if (window.navigateTo) window.navigateTo("feed");
                    }
                    return;
                }

                document.body.classList.remove("go-live-open");
                if (window.goBack) window.goBack();
                else if (window.navigateTo) window.navigateTo("feed");
            });

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

        this.applyUIMode(this.uiMode, { skipPersist: true });
        this.syncControls();
        this.uiBound = true;
    }

    setStatus(state, message = "") {
        const previous = this.state;
        this.state = state;
        const chip = document.getElementById("go-live-state-chip");
        const status = document.getElementById("go-live-status");
        const pill = document.getElementById("go-live-visibility-pill");
        const dot = document.getElementById("status-dot-indicator");
        const dotLabel = document.getElementById("status-dot-label");
        const overlayText = document.getElementById("go-live-status-text");
        const helper = document.getElementById("go-live-status-text-secondary");
        const helperAdv = document.getElementById("go-live-status-text-secondary-adv");
        const topStatus = document.getElementById("go-live-top-status");

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
        if (overlayText) overlayText.textContent = detail;
        if (pill) pill.textContent = this.inputMode === "external" ? "External Software" : "Preview";
        if (dotLabel) dotLabel.textContent = label;
        if (helper) helper.textContent = message || (state === "live" ? "Streaming to your audience." : "Ready to preview.");
        if (helperAdv) helperAdv.textContent = detail;
        if (topStatus) topStatus.textContent = detail;

        [chip, status, dot, topStatus].forEach((el) => {
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

        const startDisabled = this.state === "starting" || this.state === "live";
        const endDisabled = this.state === "idle" || this.state === "error" || this.state === "previewing";

        if (startBtn) startBtn.disabled = startDisabled;
        if (advStart) advStart.disabled = startDisabled;
        if (endBtn) endBtn.disabled = endDisabled;
        if (advEnd) advEnd.disabled = endDisabled;
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
        const micGain = document.getElementById("mixer-mic");
        const systemGain = document.getElementById("mixer-system");
        if (micGain) micGain.value = this.audioGains.mic;
        if (systemGain) systemGain.value = this.audioGains.system;
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
        if (this.uiMode === "advanced") {
            this.readAdvancedFormIntoState();
        } else {
            this.readBasicFormIntoState();
        }

        const state = {
            ...this.formState,
            latencyMode: (this.formState.latencyMode || "NORMAL").toUpperCase() === "LOW" ? "LOW" : "NORMAL",
            inputMode: this.formState.inputMode || "camera",
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

        this.log(`Start config -> visibility:${visibility}, latency:${state.latencyMode}, autoRecord:${!!state.autoRecord}`);
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

        this.log(`Preparing media capture for inputMode=${this.inputMode}`);
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
        this.liveStartTime = null;
        this.stopStatsPolling();
        this.renderSessionDetails();
        this.renderStats({ note: "Idle" });
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
