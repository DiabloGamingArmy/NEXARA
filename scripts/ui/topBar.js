export function buildTopBar({
    title = '',
    searchPlaceholder = '',
    searchValue = '',
    onSearch = null,
    filters = [],
    dropdowns = [],
    actions = []
} = {}) {
    const wrapper = document.createElement('div');
    wrapper.className = 'topbar-shell';
    wrapper.style.padding = '1rem 1rem 0 1rem';

    const heading = document.createElement('div');
    heading.style.display = 'flex';
    heading.style.alignItems = 'center';
    heading.style.justifyContent = actions.length ? 'space-between' : 'flex-start';
    heading.style.gap = '12px';

    const titleEl = document.createElement('h2');
    titleEl.style.fontWeight = '800';
    titleEl.style.fontSize = '1.5rem';
    titleEl.style.marginBottom = '1rem';
    titleEl.textContent = title;
    heading.appendChild(titleEl);

    wrapper.appendChild(heading);

    const searchRow = document.createElement('div');
    searchRow.className = 'topbar-search-row';
    searchRow.style.display = 'flex';
    searchRow.style.alignItems = 'center';
    searchRow.style.gap = '12px';

    const searchInput = document.createElement('input');
    searchInput.type = 'text';
    searchInput.className = 'form-input';
    searchInput.placeholder = searchPlaceholder;
    searchInput.value = searchValue || '';
    searchInput.style.flex = '1';
    if (typeof onSearch === 'function') {
        searchInput.addEventListener('input', onSearch);
    }
    searchRow.appendChild(searchInput);

    wrapper.appendChild(searchRow);

    const controlsRow = document.createElement('div');
    controlsRow.className = 'discover-controls topbar-controls';

    const pillRow = document.createElement('div');
    pillRow.className = 'discover-pill-row';
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

    controlsRow.appendChild(pillRow);

    dropdowns.forEach(function (dropdown) {
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

    wrapper.appendChild(controlsRow);

    return wrapper;
}
