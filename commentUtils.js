export function normalizeReplyTarget(targetId) {
    if (typeof targetId !== 'string') return null;
    const trimmed = targetId.trim();
    return trimmed.length ? trimmed : null;
}

export function buildReplyRecord({ text = '', mediaUrl = null, parentCommentId = null, userId = '' }) {
    return {
        text,
        mediaUrl,
        parentCommentId: normalizeReplyTarget(parentCommentId),
        parentId: normalizeReplyTarget(parentCommentId),
        userId,
        likes: 0,
        likedBy: []
    };
}

export function groupCommentsByParent(comments = []) {
    const byParent = {};
    const roots = [];

    comments.forEach(function (c) {
        const parentId = normalizeReplyTarget(c.parentCommentId || c.parentId);
        if (parentId) {
            if (!byParent[parentId]) byParent[parentId] = [];
            byParent[parentId].push(c);
        } else {
            roots.push(c);
        }
    });

    return { roots, byParent };
}
