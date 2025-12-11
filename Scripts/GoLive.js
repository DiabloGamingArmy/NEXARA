import { initializeApp, getApps } from 'https://www.gstatic.com/firebasejs/11.6.1/firebase-app.js';
import { getAuth } from 'https://www.gstatic.com/firebasejs/11.6.1/firebase-auth.js';
import { getFunctions, httpsCallable } from 'https://www.gstatic.com/firebasejs/11.6.1/firebase-functions.js';
import {
    getFirestore,
    doc,
    setDoc,
    serverTimestamp
} from 'https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js';

const firebaseConfig = {
    apiKey: 'AIzaSyDg9Duz3xicI3pvvOtLCrV1DJRWDI0NtYA',
    authDomain: 'spike-streaming-service.firebaseapp.com',
    projectId: 'spike-streaming-service',
    storageBucket: 'spike-streaming-service.firebasestorage.app',
    messagingSenderId: '592955741032',
    appId: '1:592955741032:web:dbd629cc957b67fc69bcdd',
    measurementId: 'G-BF3GFFY3D6'
};

function getFirebase() {
    if (!getApps().length) {
        initializeApp(firebaseConfig);
    }
    return {
        auth: getAuth(),
        db: getFirestore(),
        functions: getFunctions()
    };
}

function createOverlay() {
    let overlay = document.getElementById('go-live-overlay');
    if (!overlay) {
        overlay = document.createElement('div');
        overlay.id = 'go-live-overlay';
        overlay.innerHTML = `
            <div class="loader"></div>
            <div class="message">Loading Live Interface…</div>
        `;
        document.body.appendChild(overlay);
    }
    return overlay;
}

function ensureStyles() {
    if (document.getElementById('go-live-style')) return;
    const style = document.createElement('style');
    style.id = 'go-live-style';
    style.textContent = `
        #go-live-overlay {
            position: fixed;
            inset: 0;
            background: rgba(6, 10, 25, 0.82);
            display: none;
            align-items: center;
            justify-content: center;
            flex-direction: column;
            z-index: 9999;
            color: #fff;
            backdrop-filter: blur(8px);
            transition: opacity 0.4s ease;
        }
        #go-live-overlay.active { display: flex; opacity: 1; }
        #go-live-overlay.fade-out { opacity: 0; pointer-events: none; }
        #go-live-overlay .loader {
            width: 64px;
            height: 64px;
            border-radius: 50%;
            border: 6px solid rgba(255,255,255,0.2);
            border-top-color: #7dd3fc;
            animation: spin 1s linear infinite;
            margin-bottom: 18px;
        }
        #go-live-overlay .message { font-size: 1.1rem; letter-spacing: 0.01em; }
        @keyframes spin { to { transform: rotate(360deg); } }
        .go-live-interface { display: grid; grid-template-columns: 2fr 1fr; gap: 16px; background: rgba(255,255,255,0.04); padding: 16px; border-radius: 16px; border: 1px solid rgba(255,255,255,0.08); }
        .go-live-interface .preview-area { background: #0c1224; border-radius: 12px; min-height: 320px; display: flex; align-items: center; justify-content: center; overflow: hidden; position: relative; }
        .go-live-interface video { width: 100%; height: 100%; object-fit: contain; background: #050814; }
        .go-live-interface .config-area { background: #0c1224; padding: 12px; border-radius: 12px; color: #e6e8ef; display: grid; gap: 10px; }
        .go-live-interface .config-group { display: flex; flex-direction: column; gap: 4px; }
        .go-live-interface .config-group label { font-size: 0.9rem; color: #aab2cc; }
        .go-live-interface .config-group input,
        .go-live-interface .config-group select,
        .go-live-interface .config-group textarea { background: #0a0f1f; border: 1px solid #1d2742; color: #e6e8ef; border-radius: 8px; padding: 8px 10px; }
        .go-live-interface .control-area { grid-column: span 2; display: flex; justify-content: flex-end; gap: 10px; }
        .go-live-interface .control-area button { padding: 12px 18px; border-radius: 12px; border: none; cursor: pointer; font-weight: 600; }
        #start-stream { background: linear-gradient(135deg, #22d3ee, #2563eb); color: #fff; }
        #end-stream { background: #ef4444; color: #fff; }
    `;
    document.head.appendChild(style);
}

export class NexeraGoLiveController {
    constructor(rootEl = document.getElementById('go-live-root')) {
        ensureStyles();
        this.root = rootEl || this.createRoot();
        const { auth, db, functions } = getFirebase();
        this.auth = auth;
        this.db = db;
        this.functions = functions;
        this.state = 'pre-live';
        this.currentSessionId = null;
        this.channelInfo = null;
        this.broadcastClient = null;
        this.mediaStream = null;
        this.hasPersistentChannel = false;
        this.overlay = createOverlay();
        this.buildUI();
    }

    createRoot() {
        const el = document.createElement('div');
        el.id = 'go-live-root';
        document.body.appendChild(el);
        return el;
    }

    buildUI() {
        this.root.style.display = 'block';
        this.root.innerHTML = `
            <div class="go-live-interface">
                <div class="preview-area" id="go-live-preview">Waiting for stream source…</div>
                <div class="config-area">
                    <div class="config-group">
                        <label for="golive-title">Title</label>
                        <input id="golive-title" placeholder="Give your stream a name" />
                    </div>
                    <div class="config-group">
                        <label for="golive-category">Category</label>
                        <input id="golive-category" placeholder="Category" />
                    </div>
                    <div class="config-group">
                        <label for="golive-tags">Tags</label>
                        <input id="golive-tags" placeholder="Comma separated tags" />
                    </div>
                    <div class="config-group">
                        <label for="golive-visibility">Visibility</label>
                        <select id="golive-visibility">
                            <option value="public">Public</option>
                            <option value="unlisted">Unlisted</option>
                            <option value="private">Private</option>
                        </select>
                    </div>
                    <div class="config-group">
                        <label for="golive-source">Source</label>
                        <select id="golive-source">
                            <option value="screen">Share Screen</option>
                            <option value="camera">Camera</option>
                        </select>
                    </div>
                    <div class="config-group">
                        <label for="golive-channel-mode">Channel Type</label>
                        <select id="golive-channel-mode">
                            <option value="persistent">Persistent Channel</option>
                            <option value="ephemeral">Ephemeral Session</option>
                        </select>
                    </div>
                </div>
                <div class="control-area">
                    <button id="start-stream">Start Stream</button>
                    <button id="end-stream">End Stream</button>
                </div>
            </div>
        `;
        this.previewArea = this.root.querySelector('#go-live-preview');
        this.startBtn = this.root.querySelector('#start-stream');
        this.endBtn = this.root.querySelector('#end-stream');
        this.startBtn.addEventListener('click', () => this.startBroadcast());
        this.endBtn.addEventListener('click', () => this.stopBroadcast());
    }

    showOverlay(message = 'Loading Live Interface…') {
        if (!this.overlay) return;
        this.overlay.querySelector('.message').textContent = message;
        this.overlay.classList.remove('fade-out');
        this.overlay.classList.add('active');
    }

    hideOverlay() {
        if (!this.overlay) return;
        this.overlay.classList.add('fade-out');
        setTimeout(() => this.overlay.classList.remove('active'), 400);
    }

    setState(newState) {
        this.state = newState;
    }

    async ensureIVSSDK() {
        if (window.IVSBroadcastClient) return window.IVSBroadcastClient;
        await new Promise((resolve, reject) => {
            const script = document.createElement('script');
            script.src = 'https://web-broadcast.live-video.net/1.0.0/amazon-ivs-web-broadcast.min.js';
            script.onload = () => resolve();
            script.onerror = (err) => reject(err);
            document.head.appendChild(script);
        });
        return window.IVSBroadcastClient;
    }

    async provisionChannel(mode = 'persistent') {
        const initializeUserChannel = httpsCallable(this.functions, 'initializeUserChannel');
        const createEphemeralChannel = httpsCallable(this.functions, 'createEphemeralChannel');
        if (mode === 'persistent') {
            this.showOverlay('Provisioning persistent channel…');
            const response = await initializeUserChannel();
            this.hasPersistentChannel = true;
            return response?.data || {};
        }
        this.showOverlay('Creating ephemeral channel…');
        const response = await createEphemeralChannel();
        return response?.data || {};
    }

    getMetadata() {
        const title = document.getElementById('golive-title')?.value?.trim() || 'Untitled Stream';
        const category = document.getElementById('golive-category')?.value?.trim() || 'General';
        const tagsRaw = document.getElementById('golive-tags')?.value || '';
        const tags = tagsRaw.split(',').map(t => t.trim()).filter(Boolean);
        const visibility = document.getElementById('golive-visibility')?.value || 'public';
        const source = document.getElementById('golive-source')?.value || 'screen';
        const channelMode = document.getElementById('golive-channel-mode')?.value || 'persistent';
        return { title, category, tags, visibility, source, channelMode };
    }

    attachPreview(stream) {
        let video = this.previewArea.querySelector('video');
        if (!video) {
            this.previewArea.innerHTML = '';
            video = document.createElement('video');
            video.autoplay = true;
            video.muted = true;
            video.playsInline = true;
            this.previewArea.appendChild(video);
        }
        video.srcObject = stream;
    }

    async startBroadcast() {
        if (this.state === 'live' || this.state === 'initializing') return;
        this.setState('initializing');
        const metadata = this.getMetadata();
        try {
            const channelInfo = await this.provisionChannel(metadata.channelMode);
            this.channelInfo = channelInfo;
            this.currentSessionId = channelInfo.sessionId || crypto.randomUUID();
            this.showOverlay('Retrieving stream key…');
            const streamKey = channelInfo.streamKey;
            const ingestEndpoint = channelInfo.ingestEndpoint || channelInfo.ingestServer;
            if (!streamKey || !ingestEndpoint) {
                throw new Error('Missing stream key or ingest endpoint');
            }
            const IVSBroadcastClient = await this.ensureIVSSDK();
            this.showOverlay('Initializing WebRTC pipeline…');
            const mediaStream = metadata.source === 'screen'
                ? await navigator.mediaDevices.getDisplayMedia({ video: true, audio: true })
                : await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
            this.mediaStream = mediaStream;
            this.attachPreview(mediaStream);
            const videoTrack = mediaStream.getVideoTracks()[0];
            const audioTrack = mediaStream.getAudioTracks()[0];
            const streamConfig = IVSBroadcastClient?.StreamConfig?.STANDARD_PORTRAIT || undefined;
            const client = IVSBroadcastClient.create({ streamConfig });
            if (videoTrack) client.addVideoInput(videoTrack);
            if (audioTrack) client.addAudioInput(audioTrack);
            client.on('connectionSuccess', () => console.log('IVS connection success'));
            client.on('connectionError', (err) => console.error('IVS connection error', err));
            client.on('connectionRetry', () => console.warn('IVS retrying connection'));
            await client.startBroadcast(streamKey, ingestEndpoint);
            this.broadcastClient = client;
            await this.syncFirestoreState({
                isLive: true,
                playbackUrl: channelInfo.playbackUrl || '',
                title: metadata.title,
                category: metadata.category,
                visibility: metadata.visibility,
                tags: metadata.tags,
                startedAt: serverTimestamp(),
                endedAt: null
            });
            this.setState('live');
        } catch (error) {
            console.error('Failed to start broadcast', error);
        } finally {
            this.hideOverlay();
        }
    }

    async stopBroadcast() {
        if (this.state !== 'live' && this.state !== 'initializing') return;
        try {
            if (this.broadcastClient) {
                await this.broadcastClient.stopBroadcast();
                this.broadcastClient.destroy();
            }
        } catch (err) {
            console.warn('Error stopping broadcast', err);
        }
        if (this.mediaStream) {
            this.mediaStream.getTracks().forEach(t => t.stop());
        }
        await this.syncFirestoreState({
            isLive: false,
            endedAt: serverTimestamp()
        });
        this.setState('pre-live');
    }

    async syncFirestoreState(payload) {
        try {
            const user = this.auth.currentUser;
            const docId = this.currentSessionId || crypto.randomUUID();
            const liveDoc = doc(this.db, 'liveStreams', docId);
            const broadcaster = user ? {
                uid: user.uid,
                displayName: user.displayName || 'Creator',
                photoURL: user.photoURL || ''
            } : {};
            await setDoc(liveDoc, {
                ...payload,
                sessionId: docId,
                broadcaster,
            }, { merge: true });
        } catch (err) {
            console.error('Failed to sync live state', err);
        }
    }
}

function handleRoute() {
    const path = window.location.pathname;
    if (path.endsWith('/live/create') || path === '/live/create') {
        const root = document.getElementById('go-live-root');
        if (root) root.style.display = 'block';
        if (!window.__nexeraGoLiveController) {
            window.__nexeraGoLiveController = new NexeraGoLiveController(root);
        }
    }
}

export function initialize() {
    handleRoute();
    window.addEventListener('popstate', handleRoute);
}

export function teardown() {
    window.removeEventListener('popstate', handleRoute);
}

if (!window.__nexeraGoLiveInit) {
    window.__nexeraGoLiveInit = true;
    initialize();
}
