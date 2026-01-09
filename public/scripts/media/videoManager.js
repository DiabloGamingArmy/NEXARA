import { ref, getDownloadURL } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-storage.js";
import { storage } from "../core/firebase.js";
import { buildCdnUrl } from "../config/mediaConfig.js";

const hlsControllers = new WeakMap();
let hlsModulePromise = null;

function normalizePath(path = '') {
    return (path || '').replace(/^\//, '');
}

function getStoragePath(doc = {}) {
    return doc.storage?.sourcePath || doc.storagePath || '';
}

export function getThumbnailUrl(doc = {}) {
    const thumbPath = doc.storage?.thumbPath || doc.storage?.customThumbPath || '';
    const cdnThumb = buildCdnUrl(normalizePath(thumbPath));
    if (cdnThumb) return cdnThumb;
    return doc.thumbnailUrl || doc.mediaThumbUrl || doc.thumbURL || doc.thumbnail || '';
}

export function getHlsUrl(doc = {}) {
    const hlsPath = doc.storage?.hlsMasterPath || '';
    const cdnHls = buildCdnUrl(normalizePath(hlsPath));
    if (cdnHls) return cdnHls;
    return doc.hlsUrl || '';
}

export function getLegacyMp4Url(doc = {}) {
    return doc.videoURL || doc.mediaUrl || doc.mediaURL || doc.url || '';
}

async function resolveStorageDownloadUrl(path = '') {
    if (!path) return '';
    try {
        return await getDownloadURL(ref(storage, path));
    } catch (err) {
        return '';
    }
}

async function loadHlsModule() {
    if (hlsModulePromise) return hlsModulePromise;
    hlsModulePromise = import('https://cdn.jsdelivr.net/npm/hls.js@1.5.8/dist/hls.mjs');
    return hlsModulePromise;
}

function getStartLevel() {
    const connection = navigator.connection || navigator.mozConnection || navigator.webkitConnection;
    const effectiveType = connection?.effectiveType || '';
    const downlink = Number(connection?.downlink || 0);
    const saveData = connection?.saveData === true;
    if (saveData) return 0;
    if (effectiveType.includes('2g')) return 0;
    if (effectiveType.includes('3g') || downlink && downlink < 2) return 1;
    return -1;
}

export function destroyPlayback(videoEl) {
    if (!videoEl) return;
    const controller = hlsControllers.get(videoEl);
    if (controller) {
        try {
            controller.destroy();
        } catch (err) {
            // swallow
        }
        hlsControllers.delete(videoEl);
    }
    videoEl.pause?.();
    videoEl.removeAttribute('src');
    videoEl.load?.();
}

function showPlaybackError(videoEl, message) {
    if (!videoEl) return;
    const container = videoEl.closest('.video-player-frame, .video-modal-player, .video-preview-shell, .media-modal-player') || videoEl.parentElement;
    if (!container) return;
    let errorEl = container.querySelector('.video-player-error');
    if (!errorEl) {
        errorEl = document.createElement('div');
        errorEl.className = 'video-player-error';
        container.appendChild(errorEl);
    }
    errorEl.textContent = message || 'Video unavailable';
    errorEl.style.display = 'flex';
}

function clearPlaybackError(videoEl) {
    if (!videoEl) return;
    const container = videoEl.closest('.video-player-frame, .video-modal-player, .video-preview-shell, .media-modal-player') || videoEl.parentElement;
    const errorEl = container?.querySelector('.video-player-error');
    if (errorEl) errorEl.style.display = 'none';
}

export async function attachPlayback(videoEl, doc = {}, options = {}) {
    if (!videoEl) return { ok: false, reason: 'missing-element' };
    destroyPlayback(videoEl);
    clearPlaybackError(videoEl);

    const processingStatus = (doc.processing?.status || '').toUpperCase();
    const hlsUrl = getHlsUrl(doc);
    let mp4Url = getLegacyMp4Url(doc);

    if (processingStatus === 'PROCESSING' && !hlsUrl && !mp4Url) {
        showPlaybackError(videoEl, 'Processing…');
        return { ok: false, reason: 'processing' };
    }

    if (hlsUrl) {
        const canNative = videoEl.canPlayType('application/vnd.apple.mpegurl');
        if (canNative) {
            videoEl.src = hlsUrl;
            if (options.autoplay) videoEl.autoplay = true;
            videoEl.playsInline = true;
            return { ok: true, mode: 'hls-native', url: hlsUrl };
        }
        try {
            const module = await loadHlsModule();
            const Hls = module.default || module.Hls;
            if (Hls && Hls.isSupported()) {
                const hls = new Hls({ startLevel: getStartLevel() });
                hls.attachMedia(videoEl);
                hls.on(Hls.Events.MEDIA_ATTACHED, function () {
                    hls.loadSource(hlsUrl);
                });
                hlsControllers.set(videoEl, hls);
                if (options.autoplay) videoEl.autoplay = true;
                return { ok: true, mode: 'hls', url: hlsUrl };
            }
        } catch (err) {
            console.warn('HLS load failed', err);
        }
    }

    if (!mp4Url && doc.storage?.sourcePath) {
        mp4Url = await resolveStorageDownloadUrl(doc.storage.sourcePath);
    }

    if (mp4Url) {
        videoEl.src = mp4Url;
        if (options.autoplay) videoEl.autoplay = true;
        videoEl.playsInline = true;
        return { ok: true, mode: 'mp4', url: mp4Url };
    }

    showPlaybackError(videoEl, 'Video unavailable');
    return { ok: false, reason: 'no-source' };
}

export function getSourcePath(doc = {}) {
    return getStoragePath(doc);
}
