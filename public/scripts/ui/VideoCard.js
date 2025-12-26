/**
 * UI Component: VideoCard
 * Adds enhanced card layout with duration badge and overflow menu.
 */
export function buildVideoCardElement({
    video,
    author,
    utils,
    onOpen,
    onOpenProfile,
    onOverflow,
    onEdit,
    canEdit = false
} = {}) {
    const {
        formatCompactNumber,
        formatVideoTimestamp,
        resolveVideoThumbnail,
        getVideoViewCount,
        formatVideoDuration,
        applyAvatarToElement,
        ensureVideoStats
    } = utils || {};

    ensureVideoStats?.(video);
    const card = document.createElement('div');
    card.className = 'video-card';
    card.tabIndex = 0;
    card.setAttribute('role', 'button');
    card.setAttribute('aria-label', `Open video ${video.title || video.caption || 'details'}`);

    const thumb = document.createElement('div');
    thumb.className = 'video-thumb';
    thumb.style.backgroundImage = `url('${resolveVideoThumbnail?.(video)}')`;

    const duration = document.createElement('span');
    duration.className = 'video-duration';
    duration.textContent = formatVideoDuration?.(video) || '0:00';
    thumb.appendChild(duration);

    const views = document.createElement('div');
    views.className = 'video-views';
    views.textContent = `${formatCompactNumber?.(getVideoViewCount?.(video))} views`;
    thumb.appendChild(views);

    const meta = document.createElement('div');
    meta.className = 'video-meta';

    const avatar = document.createElement('div');
    avatar.className = 'video-avatar';
    applyAvatarToElement?.(avatar, author, { size: 42 });

    const info = document.createElement('div');
    info.className = 'video-info';

    const title = document.createElement('div');
    title.className = 'video-title';
    title.textContent = video.title || video.caption || 'Untitled video';

    const channel = document.createElement('div');
    channel.className = 'video-channel';
    channel.textContent = author.displayName || author.name || author.username || 'Nexera Creator';

    const stats = document.createElement('div');
    stats.className = 'video-stats';
    stats.textContent = `${formatCompactNumber?.(getVideoViewCount?.(video))} views • ${formatVideoTimestamp?.(video.createdAt)}`;

    const actions = document.createElement('div');
    actions.className = 'video-card-actions';

    if (canEdit) {
        const editBtn = document.createElement('button');
        editBtn.className = 'video-edit';
        editBtn.type = 'button';
        editBtn.setAttribute('aria-label', 'Edit video');
        editBtn.innerHTML = '<i class="ph ph-pencil-simple"></i>';
        editBtn.addEventListener('click', function (event) {
            event.stopPropagation();
            onEdit?.(video, event);
        });
        actions.appendChild(editBtn);
    }

    const overflow = document.createElement('button');
    overflow.className = 'video-overflow';
    overflow.type = 'button';
    overflow.setAttribute('aria-label', 'More options');
    if (video?.id) {
        overflow.setAttribute('data-video-menu', video.id);
    }
    overflow.innerHTML = '<i class="ph ph-dots-three-vertical"></i>';
    overflow.addEventListener('click', function (event) {
        event.stopPropagation();
        onOverflow?.(video, event);
    });
    actions.appendChild(overflow);

    info.appendChild(title);
    info.appendChild(channel);
    info.appendChild(stats);

    meta.appendChild(avatar);
    meta.appendChild(info);
    meta.appendChild(actions);

    card.appendChild(thumb);
    card.appendChild(meta);

    card.addEventListener('click', function () { onOpen?.(video); });
    card.addEventListener('keydown', function (event) {
        if (event.key === 'Enter' || event.key === ' ') {
            event.preventDefault();
            onOpen?.(video);
        }
    });
    thumb.addEventListener('click', function (event) {
        event.stopPropagation();
        onOpen?.(video);
    });

    if (video.ownerId) {
        avatar.addEventListener('click', function (event) {
            event.stopPropagation();
            onOpenProfile?.(video.ownerId, event);
        });
        channel.addEventListener('click', function (event) {
            event.stopPropagation();
            onOpenProfile?.(video.ownerId, event);
        });
    }

    return { card, avatar, channel };
}
