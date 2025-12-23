function buildControlsRow({ filters = [], dropdowns = [], actions = [] } = {}) {
    const controlsRow = document.createElement('div');
    controlsRow.className = 'discover-controls topbar-controls';

    const pillRow = document.createElement('div');
    pillRow.className = 'discover-pill-row';
    controlsRow.appendChild(pillRow);

    filters.forEach(function (filter) {
        const btn = document.createElement('button');
        btn.className = filter.className || 'discover-pill';
        if (filter.active) btn.classList.add('active');
        if (filter.dataset) {
            Object.entries(filter.dataset).forEach(function ([key, value]) {
                btn.dataset[key] = value;
            });
        }
        btn.textContent = filter.label || '';
        if (typeof filter.onClick === 'function') {
            btn.addEventListener('click', filter.onClick);
        }
        pillRow.appendChild(btn);
    });

    dropdowns.forEach(function (dropdown) {
        if (typeof dropdown.render === 'function') {
            const customNode = dropdown.render();
            if (customNode) {
                controlsRow.appendChild(customNode);
            }
            return;
        }

        const dropdownWrap = document.createElement('div');
        dropdownWrap.className = dropdown.className || 'discover-dropdown';
        if (dropdown.id) dropdownWrap.id = dropdown.id;
        dropdownWrap.style.display = dropdown.show === false ? 'none' : 'flex';

        const label = document.createElement('label');
        if (dropdown.forId) label.setAttribute('for', dropdown.forId);
        label.textContent = dropdown.label || '';
        dropdownWrap.appendChild(label);

        const select = document.createElement('select');
        select.className = dropdown.selectClass || 'discover-select';
        select.id = dropdown.forId || '';
        (dropdown.options || []).forEach(function (option) {
            const opt = document.createElement('option');
            opt.value = option.value;
            opt.textContent = option.label;
            if (option.value === dropdown.selected) opt.selected = true;
            select.appendChild(opt);
        });
        if (typeof dropdown.onChange === 'function') {
            select.addEventListener('change', dropdown.onChange);
        }
        dropdownWrap.appendChild(select);

        controlsRow.appendChild(dropdownWrap);
    });

    if (actions.length) {
        const actionGroup = document.createElement('div');
        actionGroup.className = 'topbar-actions';
        actions.forEach(function (action) {
            if (action && action.element) {
                actionGroup.appendChild(action.element);
            }
        });
        controlsRow.appendChild(actionGroup);
    }

    return controlsRow;
}

export function buildTopBarControls(config) {
    return buildControlsRow(config);
}

export function buildTopBar({
    title = '',
    searchPlaceholder = '',
    searchValue = '',
    onSearch = null,
    onSearchCommit = null,
    filters = [],
    dropdowns = [],
    actions = []
} = {}) {
    const wrapper = document.createElement('div');
    wrapper.className = 'topbar-shell';

    const heading = document.createElement('div');
    heading.className = 'topbar-heading';

    const titleEl = document.createElement('h2');
    titleEl.className = 'topbar-title';
    titleEl.textContent = title;
    heading.appendChild(titleEl);

    wrapper.appendChild(heading);

    const searchRow = document.createElement('div');
    searchRow.className = 'topbar-search-row';

    const searchInput = document.createElement('input');
    searchInput.type = 'text';
    searchInput.className = 'form-input';
    searchInput.placeholder = searchPlaceholder;
    searchInput.value = searchValue || '';
    searchInput.style.flex = '1';
    if (typeof onSearch === 'function') {
        searchInput.addEventListener('input', onSearch);
    }
    if (typeof onSearchCommit === 'function') {
        searchInput.addEventListener('keydown', function (event) {
            if (event.key === 'Enter') {
                onSearchCommit(event);
            }
        });
        searchInput.addEventListener('blur', onSearchCommit);
    }
    searchRow.appendChild(searchInput);

    wrapper.appendChild(searchRow);

    const controlsRow = buildControlsRow({ filters, dropdowns, actions });
    wrapper.appendChild(controlsRow);

    return wrapper;
}
