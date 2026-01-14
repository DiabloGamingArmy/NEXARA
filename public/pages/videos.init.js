window.NEXERA_DEFAULT_ROUTE = window.NEXERA_DEFAULT_ROUTE || "/videos";
window.NEXERA_PAGE_ROUTE = "/videos";
if (window.location.pathname.startsWith('/video/')) {
  const id = window.location.pathname.replace('/video/', '').split('/')[0];
  if (id) window.__NEXERA_BOOT_ROUTE = { viewType: 'video', id };
} else if (window.location.pathname === '/videos') {
  window.__NEXERA_BOOT_ROUTE = { viewType: 'section', view: 'videos' };
}
