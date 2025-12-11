import { getApp } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-app.js";
import { getFirestore, doc, getDoc } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";
import { NexeraLivePlayer } from './VideoPlayer.js';

let currentPlayer = null;
let currentStreamId = null;
const OFFLINE_BANNER = '<div class="live-offline-banner">This stream is currently offline.</div>';

function getDb() {
    try {
        const app = getApp();
        return getFirestore(app);
    } catch (error) {
        console.error('Firestore unavailable', error);
        return null;
    }
}

function renderOfflineState(root) {
    if (!root) return;
    root.style.display = 'block';
    root.innerHTML = OFFLINE_BANNER;
}

async function hydrateOwner(ownerId) {
    const db = getDb();
    if (!db || !ownerId) return null;
    try {
        const ownerRef = doc(db, 'users', ownerId);
        const ownerSnap = await getDoc(ownerRef);
        return ownerSnap.exists() ? { id: ownerSnap.id, ...ownerSnap.data() } : null;
    } catch (error) {
        console.error('Failed to fetch owner profile', error);
        return null;
    }
}

function renderMetadata(shell, streamData, owner) {
    const bar = shell.querySelector('.live-interaction-bar');
    if (!bar) return;
    const avatarUrl = owner && owner.photoURL ? owner.photoURL : streamData.avatarUrl;
    const ownerName = owner && (owner.displayName || owner.username || owner.name) ? (owner.displayName || owner.username || owner.name) : streamData.ownerName;
    const title = streamData.title || 'Live Stream';
    bar.innerHTML = `
        <div class="live-player-meta">
            <div class="live-player-title">${title}</div>
            <div class="live-player-owner">${ownerName ? `Hosted by ${ownerName}` : 'Broadcast'}</div>
            <div class="live-player-viewers">Viewers: --</div>
        </div>
        <div class="live-player-avatar">${avatarUrl ? `<img src="${avatarUrl}" alt="${ownerName || 'Host'}">` : ''}</div>
    `;
}

export async function loadLiveStream(streamId) {
    const root = document.getElementById('live-player-root');
    if (!root) return;
    const db = getDb();
    if (!db || !streamId) {
        renderOfflineState(root);
        return;
    }

    currentStreamId = streamId;

    try {
        const streamRef = doc(db, 'liveStreams', streamId);
        const streamSnap = await getDoc(streamRef);
        if (!streamSnap.exists()) {
            renderOfflineState(root);
            return;
        }
        const data = streamSnap.data() || {};
        if (!data.playbackUrl) {
            renderOfflineState(root);
            return;
        }
        const owner = await hydrateOwner(data.ownerId);
        if (currentPlayer) {
            currentPlayer.destroy();
        }
        currentPlayer = new NexeraLivePlayer({
            playbackUrl: data.playbackUrl,
            visibility: data.visibility,
            title: data.title,
            ownerName: owner && (owner.displayName || owner.username || owner.name),
            avatarUrl: owner && owner.photoURL
        });
        await currentPlayer.load();

        const shell = root.querySelector('.live-player-shell');
        if (shell) {
            renderMetadata(shell, data, owner);
        }
    } catch (error) {
        console.error('Failed to load live stream', error);
        renderOfflineState(root);
    }
}

export function unloadLiveStream() {
    if (currentPlayer) {
        currentPlayer.destroy();
        currentPlayer = null;
    }
    currentStreamId = null;
}

export function initialize() {}
export function teardown() {}
