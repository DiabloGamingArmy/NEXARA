import { ref, uploadBytesResumable } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-storage.js";

const UPLOAD_STORAGE_PREFIX = 'nexera-video-uploads';
let taskViewerBound = false;
let preferredRetryId = null;

function getStorageKey(uid) {
    return `${UPLOAD_STORAGE_PREFIX}:${uid}`;
}

function parseUploads(raw) {
    if (!raw) return [];
    try {
        const parsed = JSON.parse(raw);
        return Array.isArray(parsed) ? parsed : [];
    } catch (err) {
        console.warn('[UploadManager] Failed to parse uploads', err);
        return [];
    }
}

function serializeUploads(uploads) {
    return JSON.stringify(Array.isArray(uploads) ? uploads : []);
}

function sanitizeUploads(uploads) {
    return (uploads || []).filter(function (upload) {
        return upload && upload.uploadId && upload.storagePath;
    });
}

function getUploads(uid) {
    if (!uid) return [];
    const raw = localStorage.getItem(getStorageKey(uid));
    return sanitizeUploads(parseUploads(raw));
}

function persistUploads(uid, uploads) {
    if (!uid) return [];
    const sanitized = sanitizeUploads(uploads);
    localStorage.setItem(getStorageKey(uid), serializeUploads(sanitized));
    return sanitized;
}

function updateUpload(uid, uploadId, patch) {
    const uploads = getUploads(uid);
    const idx = uploads.findIndex(function (entry) { return entry.uploadId === uploadId; });
    if (idx === -1) return null;
    uploads[idx] = { ...uploads[idx], ...patch };
    persistUploads(uid, uploads);
    return uploads[idx];
}

function upsertUpload(uid, upload) {
    const uploads = getUploads(uid);
    const idx = uploads.findIndex(function (entry) { return entry.uploadId === upload.uploadId; });
    if (idx === -1) {
        uploads.unshift(upload);
    } else {
        uploads[idx] = { ...uploads[idx], ...upload };
    }
    persistUploads(uid, uploads);
    return uploads;
}

function removeUpload(uid, uploadId) {
    const uploads = getUploads(uid).filter(function (entry) { return entry.uploadId !== uploadId; });
    persistUploads(uid, uploads);
    return uploads;
}

function formatStatusLabel(upload) {
    const status = (upload.status || '').toUpperCase();
    if (status === 'READY') return 'Ready';
    if (status === 'FAILED') return 'Failed';
    if (status === 'UPLOADED') return 'Processing';
    if (status === 'UPLOADING') return 'Uploading';
    if (status === 'PAUSED') return 'Paused';
    return status || 'Pending';
}

function renderTaskViewer(uploads) {
    const container = document.getElementById('upload-task-viewer');
    if (!container) return;

    if (!uploads.length) {
        container.innerHTML = '<div class="upload-task-empty">No upload tasks yet.</div>';
        return;
    }

    container.innerHTML = uploads.map(function (upload) {
        const progress = Math.max(0, Math.min(100, Number(upload.lastProgress) || 0));
        const statusLabel = formatStatusLabel(upload);
        const canRetry = upload.status !== 'READY' && upload.status !== 'UPLOADING';
        return `
            <div class="upload-task-item" data-upload-id="${upload.uploadId}">
                <div class="upload-task-meta">
                    <div class="upload-task-title">${upload.fileName || upload.storagePath}</div>
                    <div class="upload-task-status">${statusLabel} • ${progress}%</div>
                </div>
                <div class="upload-task-actions">
                    ${canRetry ? `<button class="upload-task-btn" data-upload-retry="${upload.uploadId}">Retry</button>` : ''}
                    <button class="upload-task-btn secondary" data-upload-clear="${upload.uploadId}">Dismiss</button>
                </div>
                <div class="upload-task-progress">
                    <div class="upload-task-progress-bar" style="width:${progress}%;"></div>
                </div>
            </div>
        `;
    }).join('');
}

function ensureTaskViewerListener({ onRetry, onClear }) {
    const container = document.getElementById('upload-task-viewer');
    if (!container || taskViewerBound) return;
    taskViewerBound = true;
    container.addEventListener('click', function (event) {
        const retryBtn = event.target.closest('[data-upload-retry]');
        if (retryBtn && onRetry) {
            onRetry(retryBtn.getAttribute('data-upload-retry'));
            return;
        }
        const clearBtn = event.target.closest('[data-upload-clear]');
        if (clearBtn && onClear) {
            onClear(clearBtn.getAttribute('data-upload-clear'));
        }
    });
}

function normalizeResumableUploads(uploads) {
    return uploads.map(function (upload) {
        if (upload.status === 'UPLOADING') {
            return { ...upload, status: 'PAUSED' };
        }
        return upload;
    });
}

function findMatchingUpload(uploads, file) {
    if (!file) return null;
    if (preferredRetryId) {
        const preferred = uploads.find(function (entry) { return entry.uploadId === preferredRetryId; });
        if (preferred && preferred.fileName === file.name && Number(preferred.size) === Number(file.size)) {
            preferredRetryId = null;
            return preferred;
        }
    }
    return uploads.find(function (entry) {
        return entry.fileName === file.name && Number(entry.size) === Number(file.size) && entry.status !== 'READY';
    }) || null;
}

function runUploadTask({ uid, storage, file, upload, onProgress, onComplete, onError, onStateChange }) {
    return new Promise(function (resolve, reject) {
        const storageRef = ref(storage, upload.storagePath);
        const task = uploadBytesResumable(storageRef, file, {
            contentType: file.type || upload.contentType || undefined
        });

        task.on('state_changed', function (snapshot) {
            const progress = snapshot.totalBytes ? Math.round((snapshot.bytesTransferred / snapshot.totalBytes) * 100) : 0;
            updateUpload(uid, upload.uploadId, { lastProgress: progress, status: 'UPLOADING' });
            renderTaskViewer(getUploads(uid));
            if (typeof onStateChange === 'function') onStateChange(getUploads(uid));
            if (typeof onProgress === 'function') onProgress(progress, snapshot);
        }, function (error) {
            updateUpload(uid, upload.uploadId, { status: 'FAILED' });
            renderTaskViewer(getUploads(uid));
            if (typeof onStateChange === 'function') onStateChange(getUploads(uid));
            if (typeof onError === 'function') onError(error, upload);
            reject(error);
        }, function () {
            updateUpload(uid, upload.uploadId, { status: 'UPLOADED', lastProgress: 100 });
            renderTaskViewer(getUploads(uid));
            if (typeof onStateChange === 'function') onStateChange(getUploads(uid));
            if (typeof onComplete === 'function') onComplete(task.snapshot, upload);
            resolve({ snapshot: task.snapshot, upload });
        });
    });
}

export function createUploadManager({ storage, onStateChange } = {}) {
    return {
        initTaskViewer: function ({ onRetry, onClear } = {}) {
            ensureTaskViewerListener({
                onRetry: function (uploadId) {
                    preferredRetryId = uploadId;
                    if (typeof onRetry === 'function') onRetry(uploadId);
                },
                onClear: function (uploadId) {
                    if (typeof onClear === 'function') onClear(uploadId);
                }
            });
        },
        restorePendingUploads: function (uid) {
            const uploads = normalizeResumableUploads(getUploads(uid));
            persistUploads(uid, uploads);
            renderTaskViewer(uploads);
            if (typeof onStateChange === 'function') onStateChange(uploads);
            return uploads;
        },
        startUpload: async function ({ uid, file, session, onProgress, onComplete, onError } = {}) {
            if (!uid || !file || !session) return null;
            const upload = {
                uploadId: session.uploadId,
                storagePath: session.storageKey,
                fileName: file.name,
                size: file.size,
                contentType: file.type || session.contentType || '',
                lastProgress: 0,
                startedAt: Date.now(),
                status: 'UPLOADING'
            };
            upsertUpload(uid, upload);
            renderTaskViewer(getUploads(uid));
            if (typeof onStateChange === 'function') onStateChange(getUploads(uid));
            return runUploadTask({ uid, storage, file, upload, onProgress, onComplete, onError, onStateChange });
        },
        resumeUpload: async function ({ uid, file, upload, onProgress, onComplete, onError } = {}) {
            if (!uid || !file || !upload) return null;
            updateUpload(uid, upload.uploadId, { status: 'UPLOADING' });
            renderTaskViewer(getUploads(uid));
            if (typeof onStateChange === 'function') onStateChange(getUploads(uid));
            return runUploadTask({ uid, storage, file, upload, onProgress, onComplete, onError, onStateChange });
        },
        getUploads: function (uid) {
            return getUploads(uid);
        },
        findPendingUpload: function (uid, file) {
            const uploads = getUploads(uid);
            return findMatchingUpload(uploads, file);
        },
        markReady: function (uid, uploadId) {
            const updated = updateUpload(uid, uploadId, { status: 'READY', lastProgress: 100 });
            renderTaskViewer(getUploads(uid));
            if (typeof onStateChange === 'function') onStateChange(getUploads(uid));
            return updated;
        },
        markFailed: function (uid, uploadId) {
            const updated = updateUpload(uid, uploadId, { status: 'FAILED' });
            renderTaskViewer(getUploads(uid));
            if (typeof onStateChange === 'function') onStateChange(getUploads(uid));
            return updated;
        },
        prepareRetry: function (uploadId) {
            if (uploadId) preferredRetryId = uploadId;
        },
        clearUpload: function (uid, uploadId) {
            const remaining = removeUpload(uid, uploadId);
            renderTaskViewer(remaining);
            if (typeof onStateChange === 'function') onStateChange(remaining);
            return remaining;
        }
    };
}

export function renderUploadTaskViewer(uploads = []) {
    renderTaskViewer(uploads);
}
