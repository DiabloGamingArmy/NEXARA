/**
 * UI Component: DiscoverHub
 * Adds discover hub widgets and placeholder actions for the Discover page.
 */
function buildWidget(title, items = [], actionLabel = 'Refresh', actionKey = '') {
    const section = document.createElement('section');
    section.className = 'discover-widget';
    section.innerHTML = `
        <div class="discover-widget-header">
            <h3>${title}</h3>
            <button class="icon-pill" aria-label="${actionLabel}" onclick="window.handleUiStubAction?.('${actionKey}')">
                <i class="ph ph-arrow-clockwise"></i> ${actionLabel}
            </button>
        </div>
    `;
    const list = document.createElement('div');
    list.className = 'discover-widget-list';
    items.forEach(function (item) {
        const card = document.createElement('button');
        card.type = 'button';
        card.className = 'discover-widget-chip';
        card.textContent = item;
        card.onclick = function () { window.handleUiStubAction?.(`discover-${title.toLowerCase()}-${item}`); };
        list.appendChild(card);
    });
    section.appendChild(list);
    return section;
}

export function renderDiscoverHub(container) {
    if (!container) return;
    container.innerHTML = '';
    const hub = document.createElement('div');
    hub.className = 'discover-hub';

    const trending = buildWidget('Trending topics', ['#AI', '#Gaming', '#Space', '#Music'], 'Refresh', 'discover-trending-refresh');
    const categories = buildWidget('Browse categories', ['STEM', 'Gaming', 'Music', 'Sports'], 'Browse', 'discover-categories');
    const creators = buildWidget('Creators to follow', ['@nova', '@skyline', '@orbit'], 'Shuffle', 'discover-creators');

    hub.appendChild(trending);
    hub.appendChild(categories);
    hub.appendChild(creators);
    container.appendChild(hub);
}
