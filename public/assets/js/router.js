(() => {
  if (window.NexeraRouter?.initialized) return;

  const SECTION_ROUTES = {
    home: 'feed',
    live: 'live',
    videos: 'videos',
    messages: 'messages',
    discover: 'discover',
    saved: 'saved',
    profile: 'profile',
    staff: 'staff'
  };

  const ENTITY_ROUTES = {
    video: { collection: 'videos', view: 'videos' },
    post: { collection: 'posts', view: 'feed' },
    profile: { collection: 'users', view: 'profile' }
  };

  const state = {
    initialized: true,
    applying: false,
    lastRouteKey: null
  };

  const isDebugEnabled = () => {
    try {
      return window.localStorage?.getItem('NEXERA_DEBUG_ROUTER') === '1';
    } catch {
      return false;
    }
  };

  const debugLog = (...args) => {
    if (isDebugEnabled()) {
      console.log('[NexeraRouter]', ...args);
    }
  };

  function snapshotUrl() {
    return {
      path: window.location.pathname || '/',
      search: window.location.search || '',
      hash: window.location.hash || ''
    };
  }

  function parseRoute(url = snapshotUrl()) {
    const params = new URLSearchParams(url.search || '');
    const segments = (url.path || '/').split('/').filter(Boolean);
    const route = {
      path: url.path || '/',
      search: url.search || '',
      hash: url.hash || '',
      segments,
      params
    };

    if (segments.length === 0) {
      return { type: 'section', view: SECTION_ROUTES.home, route };
    }

    const head = segments[0];
    if (SECTION_ROUTES[head]) {
      return { type: 'section', view: SECTION_ROUTES[head], route };
    }

    if (ENTITY_ROUTES[head]) {
      const id = segments[1] || params.get('id') || params.get('uid') || null;
      const handle = head === 'profile' ? params.get('handle') : null;
      if (id || handle) {
        return {
          type: 'entity',
          entityType: head,
          id: id || null,
          handle: handle || null,
          route
        };
      }
    }

    if (segments.length >= 2 && ENTITY_ROUTES[head]) {
      return { type: 'entity', entityType: head, id: segments[1], route };
    }

    return { type: 'not-found', route };
  }

  async function fetchEntity(entityType, id) {
    const cfg = ENTITY_ROUTES[entityType];
    if (!cfg || !id) return { exists: false, data: null };
    const db = window.Nexera?.db;
    const docFn = window.Nexera?.firestore?.doc;
    const getDocFn = window.Nexera?.firestore?.getDoc;
    if (!db || !docFn || !getDocFn) return { exists: null, data: null };

    const start = performance.now();
    const snap = await getDocFn(docFn(db, cfg.collection, id));
    debugLog('getDoc', entityType, id, `${Math.round(performance.now() - start)}ms`);
    if (!snap.exists()) return { exists: false, data: null };
    return { exists: true, data: { id: snap.id, ...snap.data() } };
  }

  function ensureNotFoundShell() {
    let container = document.getElementById('nexera-not-found');
    if (container) return container;
    container = document.createElement('div');
    container.id = 'nexera-not-found';
    container.innerHTML = `
      <div id="auth-screen" style="display:flex;">
        <img class="brand-logo brand-logo-auth" data-logo-variant="dark" alt="Nexera logo"
          src="https://firebasestorage.googleapis.com/v0/b/spike-streaming-service.firebasestorage.app/o/apps%2Fnexera%2Fassets%2Ficons%2Fwhiteicon.png?alt=media&token=366d09a9-61f6-4096-af08-a01a119c339e">
        <div class="auth-box">
          <h2 class="nexera-gradient-text"
            style="margin-bottom:1rem; font-weight:800; font-size:2rem; letter-spacing: -0.5px;">Signal lost.</h2>
          <p id="not-found-message" style="color:var(--text-muted); margin-bottom: 1.5rem;">
            We couldn't find that page.
          </p>
          <button id="not-found-home" class="create-btn-sidebar"
            style="background: var(--primary); color:black;">Go Home</button>
        </div>
      </div>
    `;
    document.body.appendChild(container);
    const btn = container.querySelector('#not-found-home');
    if (btn) {
      btn.addEventListener('click', () => navigateToPath('/home'));
    }
    fetch('/assets/data/notFoundMessages.json', { cache: 'no-cache' })
      .then((response) => response.json())
      .then((data) => {
        const msg = container.querySelector('#not-found-message');
        if (!msg) return;
        const list = Array.isArray(data) && data.length ? data : null;
        if (!list) return;
        msg.textContent = list[Math.floor(Math.random() * list.length)];
      })
      .catch(() => {});
    return container;
  }

  function showNotFound() {
    const appLayout = document.getElementById('app-layout');
    if (appLayout) appLayout.style.display = 'none';
    ensureNotFoundShell();
  }

  function hideNotFound() {
    const container = document.getElementById('nexera-not-found');
    if (container) container.remove();
  }

  function navigateToPath(path) {
    history.pushState({}, '', path);
    applyCurrentRoute('push');
  }

  async function applyRoute(route) {
    if (!route) return;
    hideNotFound();

    if (route.type === 'not-found') {
      showNotFound();
      return;
    }

    if (route.type === 'section') {
      if (window.Nexera?.navigateTo) {
        window.Nexera.navigateTo({ view: route.view });
      }
      return;
    }

    if (route.type === 'entity') {
      if (route.entityType === 'profile' && route.handle && typeof window.openUserProfileByHandle === 'function') {
        window.openUserProfileByHandle(route.handle);
        return;
      }

      if (window.Nexera?.navigateTo && ENTITY_ROUTES[route.entityType]?.view) {
        window.Nexera.navigateTo({ view: ENTITY_ROUTES[route.entityType].view });
      }

      if (!route.id) {
        showNotFound();
        return;
      }

      const entity = await fetchEntity(route.entityType, route.id);
      if (entity.exists === false) {
        showNotFound();
        return;
      }

      if (window.Nexera?.openEntity) {
        window.Nexera.openEntity(route.entityType, route.id, entity.data || null);
      }
      return;
    }
  }

  async function applyCurrentRoute(source = 'load') {
    if (state.applying) return;
    state.applying = true;

    const start = performance.now();
    const initialUrl = source === 'init' && window.__NEXERA_INITIAL_URL ? window.__NEXERA_INITIAL_URL : null;
    const parsed = parseRoute(initialUrl || undefined);
    debugLog('route parsed', parsed, source);

    if (window.Nexera?.ready) {
      const readyStart = performance.now();
      await window.Nexera.ready;
      debugLog('ready resolved', `${Math.round(performance.now() - readyStart)}ms`);
    }

    await applyRoute(parsed);

    const done = Math.round(performance.now() - start);
    debugLog('route applied', parsed.type, `${done}ms`);

    window.Nexera?.releaseSplash?.();
    state.applying = false;
  }

  function isSameOrigin(href) {
    try {
      return new URL(href, window.location.origin).origin === window.location.origin;
    } catch {
      return false;
    }
  }

  function shouldHandleLink(anchor) {
    if (!anchor || anchor.target || anchor.hasAttribute('download')) return false;
    const href = anchor.getAttribute('href');
    if (!href || href.startsWith('mailto:') || href.startsWith('tel:')) return false;
    if (!isSameOrigin(href)) return false;
    return true;
  }

  function interceptLinkClicks(event) {
    const anchor = event.target.closest('a');
    if (!shouldHandleLink(anchor)) return;
    event.preventDefault();
    navigateToPath(anchor.getAttribute('href'));
  }

  function patchHistory() {
    ['pushState', 'replaceState'].forEach((method) => {
      const original = history[method];
      history[method] = function (...args) {
        const result = original.apply(this, args);
        window.dispatchEvent(new Event('nexera:navigation'));
        return result;
      };
    });
  }

  function init() {
    patchHistory();
    document.addEventListener('click', interceptLinkClicks);
    window.addEventListener('popstate', () => applyCurrentRoute('popstate'));
    window.addEventListener('nexera:navigation', () => applyCurrentRoute('history'));

    applyCurrentRoute('init');
  }

  window.NexeraRouter = {
    initialized: true,
    parseRoute,
    applyCurrentRoute
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init, { once: true });
  } else {
    init();
  }
})();
