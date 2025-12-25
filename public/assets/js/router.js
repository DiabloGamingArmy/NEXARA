(() => {
  if (window.NexeraRouter?.initialized) return;

  const SECTION_ROUTES = {
    home: 'feed',
    live: 'live',
    videos: 'videos',
    messages: 'messages',
    inbox: 'messages',
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
    restoring: false,
    lastRouteKey: null,
    suppressEvents: false
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

  function buildUrlForSection(view) {
    const map = {
      feed: '/',
      live: '/live',
      videos: '/videos',
      messages: '/inbox',
      discover: '/discover',
      saved: '/saved',
      profile: '/profile',
      staff: '/staff'
    };
    if (view === 'profile') {
      const user = typeof window.getCurrentUser === 'function' ? window.getCurrentUser() : null;
      if (user?.uid) {
        return buildUrlForProfile(user.uid);
      }
    }
    return map[view] || null;
  }

  function buildUrlForVideo(videoId) {
    return videoId ? `/videos/${encodeURIComponent(videoId)}` : '/videos';
  }

  function buildUrlForVideoManager() {
    return '/videos/video-manager';
  }

  function buildUrlForCreateVideo() {
    return '/videos/create-video';
  }

  function buildUrlForPost(postId) {
    return postId ? `/post/${encodeURIComponent(postId)}` : '/home';
  }

  function buildUrlForThread(threadId) {
    return threadId ? `/view-thread/${encodeURIComponent(threadId)}` : '/home';
  }

  function buildUrlForProfile(uidOrHandle, params = {}) {
    const search = new URLSearchParams(params);
    const suffix = search.toString();
    if (params.handle && !uidOrHandle) {
      return `/profile${suffix ? `?${suffix}` : ''}`;
    }
    if (uidOrHandle) {
      return `/profile/${encodeURIComponent(uidOrHandle)}${suffix ? `?${suffix}` : ''}`;
    }
    return `/profile${suffix ? `?${suffix}` : ''}`;
  }

  function buildUrlForMessages(conversationId, params = {}) {
    const search = new URLSearchParams(params);
    const suffix = search && typeof search.toString === 'function' ? search.toString() : '';
    if (conversationId) {
      return `/inbox/messages/${encodeURIComponent(conversationId)}${suffix ? `?${suffix}` : ''}`;
    }
    return `/inbox/messages${suffix ? `?${suffix}` : ''}`;
  }

  function updateUrl(path, replace = false) {
    if (state.restoring) return;
    if (!path || window.location.pathname + window.location.search + window.location.hash === path) return;
    if (replace) {
      history.replaceState({}, '', path);
    } else {
      history.pushState({}, '', path);
    }
  }

  function isFeedRoute(url = snapshotUrl()) {
    const parsed = parseRoute(url);
    return parsed.type === 'section' && parsed.view === 'feed';
  }

  function recordFeedScrollState() {
    if (!isFeedRoute()) return;
    const current = history.state || {};
    const scrollState = {
      scrollY: window.scrollY || 0,
      path: window.location.pathname + window.location.search
    };
    replaceStateSilently(scrollState.path + window.location.hash, { ...current, nexeraFeedScroll: scrollState });
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
    if (head === 'videos' && segments[1] === 'video-manager') {
      return {
        type: 'video-manager',
        canonicalPath: '/videos/video-manager',
        route
      };
    }
    if (head === 'videos' && segments[1] === 'create-video') {
      return {
        type: 'video-create',
        canonicalPath: '/videos/create-video',
        route
      };
    }
    if (head === 'videos' && segments[1]) {
      return { type: 'entity', entityType: 'video', id: segments[1], route };
    }
    if (head === 'video' && segments[1] === 'video-manager') {
      return {
        type: 'video-manager',
        canonicalPath: '/videos/video-manager',
        route
      };
    }
    if (head === 'create-video') {
      return {
        type: 'video-create',
        canonicalPath: '/videos/create-video',
        route
      };
    }
    if (head === 'profile' && (segments[1] || params.get('handle'))) {
      return {
        type: 'entity',
        entityType: 'profile',
        id: segments[1] || null,
        handle: params.get('handle'),
        route
      };
    }
    if (SECTION_ROUTES[head]) {
      if (head === 'messages' && segments[1]) {
        return { type: 'messages', conversationId: segments[1], route };
      }
      if (head === 'inbox') {
        const mode = segments[1] || 'messages';
        const allowed = ['messages', 'posts', 'videos', 'livestreams', 'account'];
        if (!allowed.includes(mode)) {
          return { type: 'section', view: SECTION_ROUTES[head], route };
        }
        const conversationId = mode === 'messages' ? (segments[2] || null) : null;
        return { type: 'inbox', mode, conversationId, route };
      }
      return { type: 'section', view: SECTION_ROUTES[head], route };
    }

    if (head === 'view-thread' && segments[1]) {
      return { type: 'thread', threadId: segments[1], route };
    }

    if (head === 'discover' && segments[1] === 'search' && segments[2]) {
      return { type: 'discover-search', query: segments.slice(2).join('/'), route };
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

  async function fetchThread(threadId) {
    if (!threadId) return { exists: false, data: null };
    const db = window.Nexera?.db;
    const docFn = window.Nexera?.firestore?.doc;
    const getDocFn = window.Nexera?.firestore?.getDoc;
    if (!db || !docFn || !getDocFn) return { exists: null, data: null };

    try {
      const start = performance.now();
      const snap = await getDocFn(docFn(db, 'posts', threadId));
      debugLog('getDoc', 'thread', threadId, `${Math.round(performance.now() - start)}ms`);
      if (!snap.exists()) return { exists: false, data: null };
      return { exists: true, data: { id: snap.id, ...snap.data() } };
    } catch (error) {
      debugLog('getDoc failed', 'thread', threadId, error);
      return { exists: null, data: null, error };
    }
  }

  function ensureNotFoundShell() {
    let container = document.getElementById('nexera-not-found');
    if (container) return container;
    container = document.createElement('div');
    container.id = 'nexera-not-found';
    container.style.position = 'fixed';
    container.style.inset = '0';
    container.style.zIndex = '9999';
    container.style.background = 'var(--bg-main, #0b0b0b)';
    container.style.display = 'flex';
    container.style.alignItems = 'center';
    container.style.justifyContent = 'center';
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
      replaceStateSilently('/home');
      if (window.Nexera?.navigateTo) {
        window.Nexera.navigateTo({ view: 'feed' });
      }
      return;
    }

    if (route.type === 'section') {
      if (route.view === 'profile') {
        const currentUser = typeof window.getCurrentUser === 'function' ? window.getCurrentUser() : null;
        if (currentUser?.uid && route.route?.segments?.length === 1) {
          replaceStateSilently(buildUrlForProfile(currentUser.uid));
          if (typeof window.openUserProfile === 'function') {
            window.openUserProfile(currentUser.uid, null, false);
          }
          return;
        }
      }
      if (route.view === 'feed' && (route.route?.path === '/' || route.route?.path === '')) {
        replaceStateSilently('/home');
      }
      if (route.view === 'messages' && route.route?.path?.startsWith('/messages')) {
        replaceStateSilently(buildUrlForMessages());
      }
      if (window.Nexera?.navigateTo) {
        window.Nexera.navigateTo({ view: route.view });
      }
      if (route.view === 'profile' && route.route?.params?.get('tab') && typeof window.setProfileFilter === 'function') {
        const tab = route.route.params.get('tab');
        setTimeout(() => window.setProfileFilter(tabFromParam(tab), 'me'), 0);
      }
      return;
    }

    if (route.type === 'messages') {
      if (window.Nexera?.navigateTo) {
        window.Nexera.navigateTo({ view: 'messages' });
      }
      if (typeof window.setInboxMode === 'function') {
        window.setInboxMode('messages', { skipRouteUpdate: true });
      }
      if (route.conversationId && typeof window.openConversation === 'function') {
        window.openConversation(route.conversationId);
      }
      return;
    }

    if (route.type === 'inbox') {
      if (window.Nexera?.navigateTo) {
        window.Nexera.navigateTo({ view: 'messages' });
      }
      if (typeof window.setInboxMode === 'function') {
        window.setInboxMode(route.mode || 'messages', { skipRouteUpdate: true });
      }
      if (route.conversationId && typeof window.openConversation === 'function') {
        window.openConversation(route.conversationId);
      }
      return;
    }

    if (route.type === 'video-manager') {
      if (route.canonicalPath && route.route?.path !== route.canonicalPath) {
        replaceStateSilently(route.canonicalPath);
      }
      if (window.Nexera?.navigateTo) {
        window.Nexera.navigateTo({ view: 'videos' });
      }
      if (typeof window.openVideoTaskViewer === 'function') {
        window.openVideoTaskViewer();
      }
      return;
    }

    if (route.type === 'discover-search') {
      if (route.query) {
        const next = `/discover?q=${encodeURIComponent(route.query)}`;
        replaceStateSilently(next);
      }
      if (window.Nexera?.navigateTo) {
        window.Nexera.navigateTo({ view: 'discover' });
      }
      return;
    }

    if (route.type === 'video-create') {
      if (route.canonicalPath && route.route?.path !== route.canonicalPath) {
        replaceStateSilently(route.canonicalPath);
      }
      if (window.Nexera?.navigateTo) {
        window.Nexera.navigateTo({ view: 'videos' });
      }
      if (typeof window.openVideoUploadModal === 'function') {
        window.openVideoUploadModal();
      }
      return;
    }

    if (route.type === 'thread') {
      if (!route.threadId) {
        showNotFound();
        return;
      }
      const thread = await fetchThread(route.threadId);
      if (thread.exists === false) {
        showNotFound();
        return;
      }
      if (thread.exists === null) {
        if (typeof window.showThreadLoadError === 'function') {
          window.showThreadLoadError('Unable to load this thread right now.');
        }
        return;
      }
      if (thread.data && typeof window.Nexera?.ensurePostInCache === 'function') {
        window.Nexera.ensurePostInCache(thread.data);
      }
      if (typeof window.openThread === 'function') {
        window.openThread(route.threadId);
      }
      return;
    }

    if (route.type === 'entity') {
      if (route.entityType === 'profile' && route.handle && typeof window.openUserProfileByHandle === 'function') {
        window.openUserProfileByHandle(route.handle);
        if (route.route?.params?.get('tab') && typeof window.setProfileFilter === 'function') {
          const tab = route.route.params.get('tab');
          setTimeout(() => window.setProfileFilter(tabFromParam(tab), route.handle), 0);
        }
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
      if (route.entityType === 'profile' && route.route?.params?.get('tab') && typeof window.setProfileFilter === 'function') {
        const tab = route.route.params.get('tab');
        setTimeout(() => window.setProfileFilter(tabFromParam(tab), route.id), 0);
      }
      return;
    }
  }

  async function applyCurrentRoute(source = 'load') {
    if (state.applying) return;
    state.applying = true;
    state.restoring = true;

    if (window.Nexera?.authReady) {
      await window.Nexera.authReady;
    }

    if (window.location.pathname === '/' || window.location.pathname === '') {
      replaceStateSilently('/home');
    }

    const start = performance.now();
    const initialUrl = source === 'init' && window.__NEXERA_INITIAL_URL ? window.__NEXERA_INITIAL_URL : null;
    const parsed = parseRoute(initialUrl || undefined);
    const routeKey = `${parsed.route?.path || ''}${parsed.route?.search || ''}${parsed.route?.hash || ''}`;
    debugLog('route parsed', parsed, source);
    if (state.lastRouteKey === routeKey && source !== 'popstate') {
      debugLog('route skipped (same)', routeKey);
      state.applying = false;
      state.restoring = false;
      return;
    }
    state.lastRouteKey = routeKey;

    if (window.Nexera?.ready) {
      const readyStart = performance.now();
      await window.Nexera.ready;
      debugLog('ready resolved', `${Math.round(performance.now() - readyStart)}ms`);
    }

    await applyRoute(parsed);

    const done = Math.round(performance.now() - start);
    debugLog('route applied', parsed.type, `${done}ms`);

    if (source === 'popstate' && parsed.type === 'section' && parsed.view === 'feed') {
      const scrollState = history.state?.nexeraFeedScroll;
      if (scrollState?.scrollY !== undefined && typeof window.Nexera?.restoreFeedScroll === 'function') {
        window.Nexera.restoreFeedScroll(scrollState.scrollY);
      }
    }

    window.Nexera?.releaseSplash?.();
    state.applying = false;
    state.restoring = false;
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
        if (isDebugEnabled()) {
          debugLog('history', method, args?.[2] || window.location.pathname + window.location.search);
        }
        if (!state.suppressEvents) {
          window.dispatchEvent(new Event('nexera:navigation'));
        }
        return result;
      };
    });
  }

  function tabFromParam(tab) {
    if (!tab) return 'All Results';
    const normalized = tab.toLowerCase();
    if (normalized === 'videos') return 'Videos';
    if (normalized === 'posts') return 'Posts';
    if (normalized === 'livestreams') return 'Livestreams';
    if (normalized === 'categories') return 'Categories';
    if (normalized === 'users') return 'Users';
    return 'All Results';
  }

  function wrapNavigationFunctions() {
    if (typeof window.navigateTo === 'function' && !window.navigateTo.__nexeraWrapped) {
      const original = window.navigateTo;
      window.navigateTo = function (viewId, pushToStack = true) {
        const result = original.call(this, viewId, pushToStack);
        const path = buildUrlForSection(viewId);
        if (path) updateUrl(path);
        return result;
      };
      window.navigateTo.__nexeraWrapped = true;
    }

    if (typeof window.openVideoDetail === 'function' && !window.openVideoDetail.__nexeraWrapped) {
      const original = window.openVideoDetail;
      window.openVideoDetail = function (videoId) {
        const result = original.call(this, videoId);
        updateUrl(buildUrlForVideo(videoId));
        return result;
      };
      window.openVideoDetail.__nexeraWrapped = true;
    }

    if (typeof window.openVideoTaskViewer === 'function' && !window.openVideoTaskViewer.__nexeraWrapped) {
      const original = window.openVideoTaskViewer;
      window.openVideoTaskViewer = function () {
        const result = original.call(this);
        updateUrl(buildUrlForVideoManager());
        return result;
      };
      window.openVideoTaskViewer.__nexeraWrapped = true;
    }

    if (typeof window.openVideoUploadModal === 'function' && !window.openVideoUploadModal.__nexeraWrapped) {
      const original = window.openVideoUploadModal;
      window.openVideoUploadModal = function () {
        const result = original.call(this);
        updateUrl(buildUrlForCreateVideo());
        return result;
      };
      window.openVideoUploadModal.__nexeraWrapped = true;
    }

    if (typeof window.openThread === 'function' && !window.openThread.__nexeraWrapped) {
      const original = window.openThread;
      window.openThread = function (postId) {
        recordFeedScrollState();
        debugLog('openThread', postId);
        const result = original.call(this, postId);
        updateUrl(buildUrlForThread(postId));
        return result;
      };
      window.openThread.__nexeraWrapped = true;
    }

    if (typeof window.openUserProfile === 'function' && !window.openUserProfile.__nexeraWrapped) {
      const original = window.openUserProfile;
      window.openUserProfile = function (uid, event, pushToStack = true) {
        recordFeedScrollState();
        debugLog('openUserProfile', uid);
        const result = original.call(this, uid, event, pushToStack);
        updateUrl(buildUrlForProfile(uid));
        return result;
      };
      window.openUserProfile.__nexeraWrapped = true;
    }

    if (typeof window.openUserProfileByHandle === 'function' && !window.openUserProfileByHandle.__nexeraWrapped) {
      const original = window.openUserProfileByHandle;
      window.openUserProfileByHandle = function (handle) {
        const result = original.call(this, handle);
        updateUrl(buildUrlForProfile(null, { handle }));
        return result;
      };
      window.openUserProfileByHandle.__nexeraWrapped = true;
    }

    if (typeof window.openMessagesPage === 'function' && !window.openMessagesPage.__nexeraWrapped) {
      const original = window.openMessagesPage;
      window.openMessagesPage = async function () {
        const result = await original.call(this);
        updateUrl(buildUrlForMessages());
        return result;
      };
      window.openMessagesPage.__nexeraWrapped = true;
    }

    if (typeof window.openConversation === 'function' && !window.openConversation.__nexeraWrapped) {
      const original = window.openConversation;
      window.openConversation = function (conversationId) {
        const result = original.call(this, conversationId);
        updateUrl(buildUrlForMessages(conversationId));
        return result;
      };
      window.openConversation.__nexeraWrapped = true;
    }

    if (typeof window.setProfileFilter === 'function' && !window.setProfileFilter.__nexeraWrapped) {
      const original = window.setProfileFilter;
      window.setProfileFilter = function (category, uid) {
        const result = original.call(this, category, uid);
        const tab = (category || '').toLowerCase().replace(/\s+/g, '');
        const tabParam = tab === 'allresults' ? null : tab;
        const params = tabParam ? { tab: tabParam } : {};
        if (uid && uid !== 'me') {
          updateUrl(buildUrlForProfile(uid, params));
        } else {
          updateUrl(buildUrlForProfile(null, params));
        }
        return result;
      };
      window.setProfileFilter.__nexeraWrapped = true;
    }
  }

  function init() {
    patchHistory();
    document.addEventListener('click', interceptLinkClicks);
    window.addEventListener('popstate', () => applyCurrentRoute('popstate'));
    window.addEventListener('nexera:navigation', () => applyCurrentRoute('history'));

    wrapNavigationFunctions();
    applyCurrentRoute('init');
  }

  window.NexeraRouter = {
    initialized: true,
    parseRoute,
    applyCurrentRoute,
    buildUrlForSection,
    buildUrlForVideo,
    buildUrlForVideoManager,
    buildUrlForCreateVideo,
    buildUrlForPost,
    buildUrlForProfile,
    buildUrlForMessages,
    buildUrlForThread,
    replaceStateSilently(path, nextState = {}) {
      replaceStateSilently(path, nextState);
    }
  };

  function replaceStateSilently(path, nextState = {}) {
    if (!path) return;
    state.suppressEvents = true;
    history.replaceState(nextState, '', path);
    state.suppressEvents = false;
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init, { once: true });
  } else {
    init();
  }
})();
