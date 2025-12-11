import {
    initializeApp,
    getApps
} from 'https://www.gstatic.com/firebasejs/11.6.1/firebase-app.js';
import {
    getFirestore,
    collection,
    query,
    where,
    orderBy,
    onSnapshot,
    doc
} from 'https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js';
import { getAuth, onAuthStateChanged } from 'https://www.gstatic.com/firebasejs/11.6.1/firebase-auth.js';
import { NexeraVideoPlayer } from './VideoPlayer.js';
import { LiveInteractions } from './LiveInteractions.js';

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
    const app = getApps()[0];
    return {
        auth: getAuth(app),
        db: getFirestore(app)
    };
}

function buildLiveDiscoverShell(root) {
    root.innerHTML = `
        <section class="live-hero">
            <div class="live-hero-content">
                <p class="live-hero-kicker">Broadcast</p>
                <h1 class="live-hero-title">Live</h1>
                <p class="live-hero-subtitle">Catch creators in real time with the same polished look as Discover.</p>
            </div>
        </section>
        <section class="live-discover-controls">
            <div class="live-search">
                <i class="ph ph-magnifying-glass"></i>
                <input type="search" id="live-search" placeholder="Search live streams" aria-label="Search live" />
            </div>
            <div class="live-filter-row" role="tablist" aria-label="Live Filters">
                <button class="live-filter-pill active" data-filter="featured">Featured</button>
                <button class="live-filter-pill" data-filter="trending">Trending</button>
                <button class="live-filter-pill" data-filter="following">Following</button>
                <button class="live-filter-pill" data-filter="new">New</button>
            </div>
        </section>
        <div class="live-grid" id="live-grid" aria-live="polite"></div>
    `;
}

function formatViewerCount(count = 0) {
    if (count >= 1_000_000) return `${(count / 1_000_000).toFixed(1)}M viewers`;
    if (count >= 1_000) return `${(count / 1_000).toFixed(1)}K viewers`;
    return `${count} viewers`;
}

class LiveListingController {
    constructor(root, db, auth) {
        this.root = root;
        this.db = db;
        this.auth = auth;
        this.unsubscribe = null;
        this.cachedStreams = [];
        buildLiveDiscoverShell(root);
        this.gridEl = root.querySelector('#live-grid');
        this.searchInput = root.querySelector('#live-search');
        this.filterPills = Array.from(root.querySelectorAll('.live-filter-pill'));
        this.bindEvents();
        this.listenLiveStreams();
    }

    bindEvents() {
        this.searchInput.addEventListener('input', () => this.render());
        this.filterPills.forEach((pill) => {
            pill.addEventListener('click', () => {
                this.filterPills.forEach(p => p.classList.remove('active'));
                pill.classList.add('active');
                this.render();
            });
        });
    }

    listenLiveStreams() {
        const liveRef = collection(this.db, 'liveStreams');
        const qRef = query(liveRef, where('isLive', '==', true), orderBy('startedAt', 'desc'));
        this.unsubscribe = onSnapshot(qRef, (snapshot) => {
            this.cachedStreams = snapshot.docs.map((d) => ({ id: d.id, ...d.data() }));
            this.render();
        });
    }

    getFilter() {
        const active = this.filterPills.find(p => p.classList.contains('active'));
        return active ? active.dataset.filter : 'featured';
    }

    filterStreams() {
        const term = (this.searchInput.value || '').toLowerCase();
        const filter = this.getFilter();
        return this.cachedStreams.filter((stream) => {
            const matchesTerm = !term || stream.title?.toLowerCase().includes(term) || stream.category?.toLowerCase().includes(term);
            let matchesFilter = true;
            if (filter === 'new') {
                matchesFilter = true;
            } else if (filter === 'trending') {
                matchesFilter = (stream.viewerCount || 0) > 10 || (stream.likes || 0) > 5;
            } else if (filter === 'following') {
                const current = this.auth.currentUser;
                const followerList = stream.followers || [];
                matchesFilter = !!current && (followerList.includes(current.uid) || stream.broadcaster?.uid === current.uid);
            }
            return matchesTerm && matchesFilter;
        });
    }

    render() {
        const streams = this.filterStreams();
        this.gridEl.innerHTML = '';
        if (!streams.length) {
            this.gridEl.innerHTML = '<div class="live-empty">No live streams right now.</div>';
            return;
        }
        streams.forEach((stream) => {
            const card = document.createElement('article');
            card.className = 'live-card';
            card.setAttribute('role', 'button');
            card.setAttribute('tabindex', '0');
            card.dataset.id = stream.id;
            card.innerHTML = `
                <div class="live-card-media">
                    <div class="live-pill">LIVE</div>
                    <div class="live-viewers">${formatViewerCount(stream.viewerCount || 0)}</div>
                </div>
                <div class="live-card-body">
                    <div class="live-card-header">
                        <img class="live-avatar" src="${stream.broadcaster?.photoURL || 'https://placehold.co/48x48'}" alt="${stream.broadcaster?.displayName || 'Creator'} avatar" />
                        <div>
                            <div class="live-title">${stream.title || 'Untitled Stream'}</div>
                            <div class="live-category">${stream.category || 'General'}</div>
                        </div>
                    </div>
                    <div class="live-card-meta">
                        <span class="live-broadcaster">${stream.broadcaster?.displayName || 'Creator'}</span>
                        <span class="live-meta-spacer">•</span>
                        <span class="live-viewer-chip">${formatViewerCount(stream.viewerCount || 0)}</span>
                    </div>
                </div>
            `;
            card.addEventListener('click', () => this.navigateTo(stream.id));
            card.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') this.navigateTo(stream.id);
            });
            this.gridEl.appendChild(card);
        });
    }

    navigateTo(streamId) {
        const path = `/live/watch/${streamId}`;
        window.history.pushState({}, '', path);
        handleRoute();
    }

    destroy() {
        if (this.unsubscribe) this.unsubscribe();
    }
}

class LiveWatchController {
    constructor(root, db, auth) {
        this.root = root;
        this.db = db;
        this.auth = auth;
        this.player = new NexeraVideoPlayer();
        this.interactions = new LiveInteractions(db, auth);
        this.streamUnsub = null;
        this.currentId = null;
        this.interactionsBound = false;
    }

    mountShell() {
        this.root.innerHTML = `
            <div class="live-player-shell">
                <div class="live-video-panel">
                    <div id="live-video-container" class="live-video-container"></div>
                    <div class="live-interaction-bar">
                        <button class="live-like-btn" id="live-like-btn">❤️ Like</button>
                        <button class="live-follow-btn" id="live-follow-btn">Follow</button>
                        <span class="live-viewer-count" id="live-viewer-count">Viewers: --</span>
                        <button class="live-share-btn" id="live-share-btn">Share</button>
                    </div>
                </div>
                <aside class="live-chat-panel">
                    <div class="live-chat-header">Live Chat</div>
                    <div class="live-chat-messages" id="live-chat-messages"></div>
                    <form class="live-chat-input" id="live-chat-form">
                        <input type="text" id="live-chat-text" placeholder="Send a message" autocomplete="off" />
                        <button type="submit">Send</button>
                    </form>
                </aside>
            </div>
            <div class="live-toast-stack" id="live-toast-stack"></div>
        `;
    }

    showOffline() {
        this.interactions.destroy();
        this.player.destroy();
        this.interactionsBound = false;
        this.root.innerHTML = `<div class="offline-banner">This stream has ended.</div>`;
    }

    loadStream(streamId) {
        this.currentId = streamId;
        this.interactionsBound = false;
        this.mountShell();
        const streamRef = doc(this.db, 'liveStreams', streamId);
        if (this.streamUnsub) this.streamUnsub();
        this.streamUnsub = onSnapshot(streamRef, async (snap) => {
            if (!snap.exists()) {
                this.showOffline();
                return;
            }
            const data = snap.data();
            this.interactions.updateStreamData(data);
            if (!data.isLive) {
                this.showOffline();
                return;
            }
            this.player.mount(document.getElementById('live-video-container'));
            if (data.playbackUrl) {
                await this.player.play(data.playbackUrl);
            }
            if (!this.interactionsBound) {
                const controls = {
                    likeBtn: document.getElementById('live-like-btn'),
                    followBtn: document.getElementById('live-follow-btn'),
                    viewerLabel: document.getElementById('live-viewer-count'),
                    shareBtn: document.getElementById('live-share-btn'),
                    toastStack: document.getElementById('live-toast-stack')
                };
                const chat = {
                    messagesEl: document.getElementById('live-chat-messages'),
                    formEl: document.getElementById('live-chat-form'),
                    inputEl: document.getElementById('live-chat-text')
                };
                this.interactions.bind(streamId, data, controls, chat);
                this.interactionsBound = true;
            } else {
                this.interactions.updateStreamData(data);
            }
        });
    }

    destroy() {
        if (this.streamUnsub) this.streamUnsub();
        this.interactions.destroy();
        this.player.destroy();
    }
}

let liveListingController = null;
let liveWatchController = null;
const { auth, db } = getFirebase();

function ensureAuthReady(callback) {
    if (auth.currentUser) {
        callback();
    } else {
        onAuthStateChanged(auth, () => callback());
    }
}

function handleRoute() {
    const path = window.location.pathname;
    if (path.startsWith('/live/create')) return;
    const discoverRoot = document.getElementById('live-discover-root');
    const playerRoot = document.getElementById('live-player-root');
    if (path.startsWith('/live/watch/')) {
        const streamId = path.split('/live/watch/')[1];
        discoverRoot.style.display = 'none';
        playerRoot.style.display = 'block';
        if (!liveWatchController) {
            liveWatchController = new LiveWatchController(playerRoot, db, auth);
        }
        liveWatchController.loadStream(streamId);
        return;
    }
    if (path.startsWith('/live')) {
        playerRoot.style.display = 'none';
        discoverRoot.style.display = 'block';
        if (!liveListingController) {
            liveListingController = new LiveListingController(discoverRoot, db, auth);
        }
        return;
    }
}

export function initialize() {
    ensureAuthReady(handleRoute);
    window.addEventListener('popstate', handleRoute);
}

export function teardown() {
    window.removeEventListener('popstate', handleRoute);
    if (liveListingController) liveListingController.destroy();
    if (liveWatchController) liveWatchController.destroy();
}

if (!window.__nexeraLiveInit) {
    window.__nexeraLiveInit = true;
    initialize();
}
