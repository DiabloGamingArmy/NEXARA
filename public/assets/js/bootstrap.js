(() => {
  const path = window.location.pathname || '/';
  const search = window.location.search || '';
  const hash = window.location.hash || '';
  const segments = path.split('/').filter(Boolean);
  const params = new URLSearchParams(search);

  window.__NEXERA_INITIAL_URL = { path, search, hash };
  window.__NEXERA_INITIAL_ROUTE = {
    path,
    search,
    hash,
    segments,
    params
  };
  window.__NEXERA_BOOT = { parsedAt: Date.now() };
})();
