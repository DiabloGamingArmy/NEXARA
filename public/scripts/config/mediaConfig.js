export const CDN_BASE_URL = (window.NEXERA_CDN_BASE_URL || '').toString().trim();

export function buildCdnUrl(path = '') {
    const base = CDN_BASE_URL.replace(/\/$/, '');
    const cleanPath = (path || '').replace(/^\//, '');
    if (!base || !cleanPath) return '';
    return `${base}/${cleanPath}`;
}
