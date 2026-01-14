import { destroyPlayback } from "../media/videoManager.js";

export function createVirtualVideoList({ container, itemHeight = 320, overscan = 3, renderItem, scrollElement = null }) {
    if (!container) throw new Error('Virtual list requires a container');
    const spacer = document.createElement('div');
    const itemsLayer = document.createElement('div');

    container.innerHTML = '';
    container.classList.add('virtual-video-list');
    container.style.position = 'relative';

    spacer.className = 'virtual-video-spacer';
    itemsLayer.className = 'virtual-video-items';

    container.appendChild(spacer);
    container.appendChild(itemsLayer);

    let items = [];
    let columnCount = 1;
    let rowHeight = itemHeight;
    let lastRange = { start: 0, end: -1 };
    let containerOffsetTop = 0;
    const computedStyle = window.getComputedStyle(container);
    const shouldUseContainerScroll = scrollElement
        ? scrollElement !== window
        : ['auto', 'scroll'].includes(computedStyle.overflowY);
    const scrollTarget = scrollElement || (shouldUseContainerScroll ? container : window);

    function cleanupRemoved() {
        const videos = itemsLayer.querySelectorAll('video');
        videos.forEach(function (video) {
            destroyPlayback(video);
        });
    }

    function getGridMetrics() {
        const computed = window.getComputedStyle(container);
        const cols = computed.gridTemplateColumns ? computed.gridTemplateColumns.split(' ').length : 1;
        const gapValue = parseFloat(computed.rowGap || computed.gap || '0');
        columnCount = Math.max(1, cols || 1);
        rowHeight = itemHeight + gapValue;
        itemsLayer.style.gap = computed.gap || computed.rowGap || '0px';
        itemsLayer.style.gridTemplateColumns = computed.gridTemplateColumns || '1fr';
    }

    function renderRange(startIndex, endIndex) {
        if (!renderItem) return;
        if (startIndex === lastRange.start && endIndex === lastRange.end) return;
        cleanupRemoved();
        itemsLayer.innerHTML = '';

        const fragment = document.createDocumentFragment();
        for (let i = startIndex; i <= endIndex; i += 1) {
            const item = items[i];
            if (!item) continue;
            const el = renderItem(item, i);
            if (el) fragment.appendChild(el);
        }
        itemsLayer.appendChild(fragment);
        lastRange = { start: startIndex, end: endIndex };

        const startRow = Math.floor(startIndex / columnCount);
        itemsLayer.style.transform = `translateY(${startRow * rowHeight}px)`;
    }

    function updateLayout() {
        getGridMetrics();
        const totalRows = Math.ceil(items.length / columnCount);
        spacer.style.height = `${totalRows * rowHeight}px`;
        containerOffsetTop = container.getBoundingClientRect().top + window.scrollY;
        handleScroll();
    }

    function handleScroll() {
        const scrollTop = scrollTarget === window
            ? Math.max(0, window.scrollY - containerOffsetTop)
            : scrollTarget.scrollTop;
        const viewportHeight = scrollTarget === window ? window.innerHeight : scrollTarget.clientHeight;
        const startRow = Math.max(0, Math.floor(scrollTop / rowHeight) - overscan);
        const endRow = Math.min(Math.ceil(items.length / columnCount), Math.ceil((scrollTop + viewportHeight) / rowHeight) + overscan);
        const startIndex = startRow * columnCount;
        const endIndex = Math.min(items.length - 1, endRow * columnCount - 1);
        renderRange(startIndex, endIndex);
    }

    function setItems(nextItems = []) {
        items = Array.isArray(nextItems) ? nextItems : [];
        updateLayout();
    }

    function refresh() {
        updateLayout();
    }

    scrollTarget.addEventListener('scroll', handleScroll, { passive: true });
    window.addEventListener('resize', refresh);

    return { setItems, refresh };
}
