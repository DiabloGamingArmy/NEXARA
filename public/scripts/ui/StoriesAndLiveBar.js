/**
 * UI Component: StoriesAndLiveBar
 * Placeholder stories/live list for the home sidebar until backend data is wired in.
 */
const DEFAULT_STORIES = [
    { id: 'me', label: 'Your Story', isMe: true },
    { id: 'user1', label: 'Guitarboyyee07' },
    { id: 'user2', label: 'MidnightNova245' },
    { id: 'user3', label: 'NeonSkies' }
];

const DEFAULT_LIVE_USERS = [
    { id: 'user4', label: 'SpaceX_Fan' },
    { id: 'user5', label: 'SpeedSouls' },
    { id: 'user6', label: 'NovaPulse' }
];

function getInitials(label = '') {
    const words = label.trim().split(/\s+/).filter(Boolean);
    if (!words.length) return 'N';
    if (words.length === 1) return words[0].slice(0, 2).toUpperCase();
    return (words[0][0] + words[1][0]).toUpperCase();
}

function buildAvatarItem(user, { showLive = false } = {}) {
    const item = document.createElement('div');
    item.className = 'stories-live-item';

    const avatar = document.createElement('div');
    avatar.className = `stories-live-avatar${user.isMe ? ' is-me' : ''}`;
    avatar.textContent = getInitials(user.label || 'User');

    if (user.isMe) {
        const badge = document.createElement('div');
        badge.className = 'stories-live-add-badge';
        badge.innerHTML = '<span aria-hidden="true">+</span>';
        avatar.appendChild(badge);
    }

    if (showLive) {
        const liveDot = document.createElement('div');
        liveDot.className = 'stories-live-dot';
        liveDot.setAttribute('aria-label', 'Live');
        avatar.appendChild(liveDot);
    }

    const label = document.createElement('div');
    label.className = 'stories-live-label';
    label.textContent = user.label || 'User';

    item.appendChild(avatar);
    item.appendChild(label);

    return item;
}

function buildRow(title, items, options = {}) {
    const section = document.createElement('div');
    section.className = 'stories-live-section';

    const heading = document.createElement('h3');
    heading.className = 'stories-live-heading';
    heading.textContent = title;

    const row = document.createElement('div');
    row.className = 'stories-live-row';
    items.forEach(function (item) {
        row.appendChild(buildAvatarItem(item, options));
    });

    section.appendChild(heading);
    section.appendChild(row);

    return section;
}

export function renderStoriesAndLiveBar(container, options = {}) {
    if (!container) return;
    const stories = Array.isArray(options.stories) ? options.stories : DEFAULT_STORIES;
    const liveUsers = Array.isArray(options.liveUsers) ? options.liveUsers : DEFAULT_LIVE_USERS;

    container.innerHTML = '';
    const wrapper = document.createElement('section');
    wrapper.className = 'stories-live-bar';

    wrapper.appendChild(buildRow('Stories', stories, { showLive: false }));
    wrapper.appendChild(buildRow('Live streams', liveUsers, { showLive: true }));

    container.appendChild(wrapper);
}
