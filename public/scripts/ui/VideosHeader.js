/**
 * UI Component: VideosHeader
 * Adds Videos page scaffolding (title, search, sort, filters, and placeholder actions).
 */
import { buildTopBar } from '/scripts/ui/topBar.js';

function buildHeaderAction(label, icon, onClick, options = {}) {
    const btn = document.createElement('button');
    btn.className = options.className || 'icon-pill';
    btn.type = 'button';
    btn.innerHTML = icon ? `<i class="${icon}"></i> ${label}` : label;
    btn.setAttribute('aria-label', label);
    if (options.disabled) {
        btn.disabled = true;
    }
    if (typeof onClick === 'function') {
        btn.addEventListener('click', onClick);
    }
    return btn;
}

export function buildVideosHeader({
    searchValue = '',
    onSearch,
    filter = 'All',
    onFilter,
    sort = 'recent',
    onSort,
    onAction
} = {}) {
    const safeAction = typeof onAction === 'function' ? onAction : function () {};
    const filters = [
        { label: 'All', className: 'discover-pill video-filter-pill', active: filter === 'All', onClick: function () { onFilter?.('All'); } },
        { label: 'Trending', className: 'discover-pill video-filter-pill', active: filter === 'Trending', onClick: function () { onFilter?.('Trending'); } },
        { label: 'Shorts', className: 'discover-pill video-filter-pill', active: filter === 'Shorts', onClick: function () { onFilter?.('Shorts'); } },
        { label: 'Saved', className: 'discover-pill video-filter-pill', active: filter === 'Saved', onClick: function () { onFilter?.('Saved'); } }
    ];

    const dropdowns = [
        {
            id: 'video-sort-select',
            className: 'discover-dropdown',
            forId: 'video-sort-select',
            label: 'Sort:',
            options: [
                { value: 'recent', label: 'Recent' },
                { value: 'popular', label: 'Popular' }
            ],
            selected: sort,
            onChange: onSort
        }
    ];

    const actions = [
        { element: buildHeaderAction('Refresh', 'ph ph-arrow-clockwise', function () { safeAction('videos-refresh'); }) },
        { element: buildHeaderAction('Grid', 'ph ph-grid-four', function () { safeAction('videos-grid'); }) },
        { element: buildHeaderAction('List', 'ph ph-list', function () { safeAction('videos-list'); }) }
    ];

    return buildTopBar({
        title: 'Videos',
        searchPlaceholder: 'Search videos',
        searchValue,
        onSearch,
        filters,
        dropdowns,
        actions
    });
}
