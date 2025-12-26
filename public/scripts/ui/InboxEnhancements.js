/**
 * UI Component: InboxEnhancements
 * Adds denser conversation list controls and chat composer actions placeholders.
 */
function ensureButton(label, icon, handler, options = {}) {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = options.className || 'icon-pill';
    btn.innerHTML = icon ? `<i class="${icon}"></i>${label ? ` ${label}` : ''}` : label;
    btn.setAttribute('aria-label', label || options.ariaLabel || 'Action');
    if (options.disabled) btn.disabled = true;
    if (typeof handler === 'function') {
        btn.addEventListener('click', handler);
    }
    return btn;
}

export function enhanceInboxLayout() {
    const header = document.querySelector('.inbox-header');
    const filters = document.querySelector('.inbox-filters');
    const list = document.getElementById('conversation-list');
    if (list) list.classList.add('conversation-list-compact');

    if (header && !header.querySelector('.inbox-header-actions')) {
        const existingButton = header.querySelector('button');
        if (existingButton) existingButton.remove();
        const actions = document.createElement('div');
        actions.className = 'inbox-header-actions';
        actions.appendChild(ensureButton('New message', 'ph ph-plus', function () { window.openNewChatModal?.(); }, { className: 'create-btn-sidebar' }));
        header.appendChild(actions);
    }

    if (filters) {
        filters.innerHTML = '';
        const filterButtons = [
            { key: 'all', label: 'All' },
            { key: 'unread', label: 'Unread' },
            { key: 'pinned', label: 'Pinned' },
            { key: 'archived', label: 'Archived' }
        ];
        filterButtons.forEach(function (filter) {
            const btn = document.createElement('button');
            btn.className = `inbox-filter${filter.key === 'all' ? ' active' : ''}`;
            btn.dataset.filter = filter.key;
            btn.textContent = filter.label;
            btn.onclick = function () { window.setConversationFilter?.(filter.key); };
            filters.appendChild(btn);
        });
    }

    const headerTools = document.querySelector('.message-thread-header-row');
    if (headerTools && !headerTools.querySelector('.message-info-toggle')) {
        const infoBtn = ensureButton('', 'ph ph-info', function () { window.toggleConversationInfoPanel?.(); }, { ariaLabel: 'Toggle info panel', className: 'icon-pill message-info-toggle' });
        headerTools.appendChild(infoBtn);
    }

    const compose = document.querySelector('.message-compose');
    if (compose && !compose.querySelector('.message-compose-actions')) {
        const actions = document.createElement('div');
        actions.className = 'message-compose-actions';
        const existingAttach = compose.querySelector('button.icon-pill');
        if (existingAttach) {
            existingAttach.setAttribute('aria-label', 'Attach file');
            existingAttach.innerHTML = '<i class="ph ph-paperclip"></i>';
            existingAttach.classList.add('message-attach-btn');
        } else {
            actions.appendChild(ensureButton('', 'ph ph-paperclip', function () { document.getElementById('message-media')?.click(); }, { ariaLabel: 'Attach file' }));
        }
        actions.appendChild(ensureButton('', 'ph ph-smiley', function () { window.handleUiStubAction?.('message-emoji'); }, { ariaLabel: 'Emoji picker' }));
        actions.appendChild(ensureButton('', 'ph ph-microphone', function () { window.handleUiStubAction?.('message-audio'); }, { ariaLabel: 'Voice note (coming soon)', disabled: true }));
        compose.insertBefore(actions, compose.firstChild);
    }

    const input = document.getElementById('message-input');
    if (input) input.setAttribute('aria-label', 'Message input');

    const panel = document.getElementById('message-info-panel');
    if (!panel) {
        const container = document.createElement('div');
        container.id = 'message-info-panel';
        container.className = 'message-info-panel';
        container.innerHTML = `
            <div class="message-info-header">Participants</div>
            <div class="message-info-body">
                <div class="empty-state">Participant info will appear here.</div>
                <button class="icon-pill" onclick="window.handleUiStubAction?.('message-panel-refresh')">
                    <i class="ph ph-arrow-clockwise"></i> Refresh
                </button>
            </div>
        `;
        const inboxThread = document.querySelector('.inbox-thread');
        if (inboxThread) inboxThread.appendChild(container);
    }

    const listSearch = document.getElementById('conversation-list-search');
    if (listSearch) listSearch.setAttribute('aria-label', 'Search conversations');
}
