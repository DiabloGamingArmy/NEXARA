export const CHAT_IMAGE_MAX_BYTES = 10 * 1024 * 1024;
export const CHAT_VIDEO_MAX_BYTES = 25 * 1024 * 1024;
export const CHAT_ALLOWED_MIME_PREFIXES = ['image/', 'video/'];

export function sanitizeFileName(name = '') {
    const trimmed = (name || '').trim();
    if (!trimmed) return 'attachment';
    return trimmed
        .replace(/\s+/g, '_')
        .replace(/[^a-zA-Z0-9._-]/g, '')
        .replace(/_+/g, '_')
        .slice(0, 120) || 'attachment';
}

export function validateChatAttachment(file, options = {}) {
    if (!file) return { ok: false, message: 'Attachment missing.' };
    const type = file.type || '';
    const allowedPrefixes = options.allowedPrefixes || CHAT_ALLOWED_MIME_PREFIXES;
    const isAllowed = allowedPrefixes.some((prefix) => type.startsWith(prefix));
    if (!isAllowed) {
        return { ok: false, message: 'Only image and video attachments are allowed.' };
    }

    const inferredMaxBytes = type.startsWith('video/') ? CHAT_VIDEO_MAX_BYTES : CHAT_IMAGE_MAX_BYTES;
    const maxBytes = Number.isFinite(options.maxBytes) ? options.maxBytes : inferredMaxBytes;
    if (file.size > maxBytes) {
        return { ok: false, message: `Attachments must be under ${formatUploadFileSize(maxBytes)}.` };
    }

    return { ok: true };
}

export function buildChatMediaPath({ conversationId, messageId, timestamp, filename }) {
    const safeName = sanitizeFileName(filename);
    const stamp = Number.isFinite(timestamp) ? timestamp : Date.now();
    return `chats/${conversationId}/messages/${messageId}/${stamp}_${safeName}`;
}

function formatUploadFileSize(bytes) {
    if (!bytes && bytes !== 0) return '0B';
    const units = ['B', 'KB', 'MB', 'GB'];
    let idx = 0;
    let value = Number(bytes);
    while (value >= 1024 && idx < units.length - 1) {
        value /= 1024;
        idx += 1;
    }
    const rounded = idx === 0 ? Math.round(value) : value.toFixed(1);
    return `${rounded}${units[idx]}`;
}
