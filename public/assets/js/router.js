import { getApp } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-app.js";
import { getFirestore, doc, getDoc } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";

(() => {
  if (window.NexeraRouter && window.NexeraRouter.initialized) return;

  const routerState = {
    initialized: true,
    currentView: null,
    isReplaying: false,
    pendingDetail: null,
    pendingRestore: null,
    pendingTimer: null,
    pendingObserver: null,
    desiredSection: null,
    sectionRestoreActive: false,
    sectionRestoreTimer: null,
    sectionRestoreObserver: null,
    sectionRestoreStart: null,
    videoExistenceCache: new Map(),
    videoDocCache: new Map(),
    lastPath: null
  };

  const viewToPath = {
    feed: '/home',
    live: '/live',
    videos: '/videos',
    messages: '/messages',
    discover: '/discover',
    saved: '/saved',
    profile: '/profile',
    staff: '/staff'
  };

  const pathToView = {
    home: 'feed',
    live: 'live',
    videos: 'videos',
    messages: 'messages',
    discover: 'discover',
    saved: 'saved',
    profile: 'profile',
    staff: 'staff'
  };

  const pageDefaults = {
    'feed.html': '/home',
    'home.html': '/home',
    'live.html': '/live',
    'videos.html': '/videos',
    'messages.html': '/messages',
    'discover.html': '/discover',
    'saved.html': '/saved',
    'profile.html': '/profile',
    'staff.html': '/staff'
  };

  const buildLink = {
    profile: ({ uid, handle } = {}) => {
      if (uid) return `/profile/${encodeURIComponent(uid)}`;
      if (handle) return `/profile?handle=${encodeURIComponent(handle)}`;
      return '/profile';
    },
    video: (id) => (id ? `/video/${encodeURIComponent(id)}` : '/videos'),
    post: (id) => (id ? `/post/${encodeURIComponent(id)}` : '/home')
  };

  const NOT_FOUND_PATH = '/not-found.html';

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

  function getCurrentUrl() {
    return `${window.location.pathname}${window.location.search}${window.location.hash}`;
  }

  function updateUrl(path, replace = false) {
    if (!path || routerState.isReplaying) return;
    const current = getCurrentUrl();
    if (current === path) return;
    const state = { nexeraRoute: true };
    if (replace) {
      history.replaceState(state, '', path);
    } else {
      history.pushState(state, '', path);
    }
    routerState.lastPath = path;
  }

  function replaceUrl(path) {
    if (!path) return;
    const state = { nexeraRoute: true };
    history.replaceState(state, '', path);
    routerState.lastPath = path;
  }

  function normalizePath(pathname) {
    if (!pathname) return '/';
    if (pathname.length > 1 && pathname.endsWith('/')) return pathname.slice(0, -1);
    return pathname;
  }

  function parseHashRoute() {
    const hash = window.location.hash || '';
    if (hash.startsWith('#/')) return hash.slice(1);
    if (hash.startsWith('#thread-')) return `/post/${hash.replace('#thread-', '')}`;
    return null;
  }

  function parseRoute() {
    const searchParams = new URLSearchParams(window.location.search || '');
    const hashRoute = parseHashRoute();
    const rawPath = hashRoute || window.location.pathname || '/';
    if (window.__NEXERA_BOOT_ROUTE && !hashRoute) {
      const boot = window.__NEXERA_BOOT_ROUTE;
      if (boot.viewType) {
        return { ...boot, canonical: rawPath };
      }
    }
    const normalized = normalizePath(rawPath);
    const parts = normalized.split('/').filter(Boolean);
    const fromHash = Boolean(hashRoute);

    if (normalized === NOT_FOUND_PATH || normalized === '/not-found') {
      return { viewType: 'not-found', canonical: NOT_FOUND_PATH, fromHash };
    }

    if (normalized.startsWith('/pages/')) {
      const pageName = normalized.split('/').pop();
      const defaultRoute = pageDefaults[pageName];
      const fallback = window.NEXERA_PAGE_ROUTE || window.NEXERA_DEFAULT_ROUTE || defaultRoute;
      if (fallback) {
        return { viewType: 'section', view: pathToView[fallback.replace('/', '')] || 'feed', canonical: fallback, fromHash };
      }
    }

    if (normalized === '/' || normalized === '') {
      const fallbackRoute = window.NEXERA_PAGE_ROUTE || window.NEXERA_DEFAULT_ROUTE || '/home';
      return { viewType: 'section', view: pathToView[fallbackRoute.replace('/', '')] || 'feed', canonical: fallbackRoute, fromHash };
    }

    const head = parts[0];
    if (head === 'profile') {
      const uid = parts[1] || searchParams.get('uid');
      const handle = searchParams.get('handle');
      if (uid || handle) {
        const canonical = uid ? `/profile/${encodeURIComponent(uid)}` : `/profile?handle=${encodeURIComponent(handle || '')}`;
        return { viewType: 'profile', id: uid || null, handle: handle || null, canonical, fromHash };
      }
      return { viewType: 'section', view: 'profile', canonical: '/profile', fromHash };
    }
    if (head === 'video') {
      const id = parts[1] || searchParams.get('id');
      if (id) {
        return { viewType: 'video', id, canonical: `/video/${encodeURIComponent(id)}`, fromHash };
      }
      return { viewType: 'section', view: 'videos', canonical: '/videos', fromHash };
    }
    if (head === 'post') {
      const id = parts[1] || searchParams.get('id');
      if (id) {
        return { viewType: 'post', id, canonical: `/post/${encodeURIComponent(id)}`, fromHash };
      }
      return { viewType: 'section', view: 'feed', canonical: '/home', fromHash };
    }

    const view = pathToView[head];
    if (view) {
      return { viewType: 'section', view, canonical: `/${head}`, fromHash };
    }

    return { viewType: 'unknown', path: normalized, fromHash };
  }

  function withReplayGuard(fn) {
    routerState.isReplaying = true;
    try {
      fn();
    } finally {
      routerState.isReplaying = false;
    }
  }

  function getFirestoreInstance() {
    const exposedDb = window.NexeraApp?.db;
    if (exposedDb) return exposedDb;
    try {
      return getFirestore(getApp());
    } catch (error) {
      debugLog('Firestore unavailable for router checks.', error);
      return null;
    }
  }

  async function fetchVideoDoc(videoId) {
    if (!videoId) return null;
    if (routerState.videoDocCache.has(videoId)) {
      return routerState.videoDocCache.get(videoId);
    }
    const db = getFirestoreInstance();
    if (!db) return null;
    try {
      const snap = await getDoc(doc(db, 'videos', videoId));
      const payload = snap.exists() ? { id: snap.id, ...snap.data() } : null;
      routerState.videoDocCache.set(videoId, payload);
      routerState.videoExistenceCache.set(videoId, Boolean(payload));
      return payload;
    } catch (error) {
      debugLog('Video existence check failed.', error);
      return null;
    }
  }

  async function checkVideoExists(videoId) {
    if (routerState.videoExistenceCache.has(videoId)) {
      return routerState.videoExistenceCache.get(videoId);
    }
    const docData = await fetchVideoDoc(videoId);
    if (docData) return true;
    if (routerState.videoExistenceCache.has(videoId)) {
      return routerState.videoExistenceCache.get(videoId);
    }
    return null;
  }

  function isElementVisible(el) {
    if (!el) return false;
    const style = window.getComputedStyle(el);
    if (!style) return false;
    return style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0';
  }

  function isAuthScreenVisible() {
    const auth = document.getElementById('auth-screen');
    return isElementVisible(auth);
  }

  function isLoadingVisible() {
    const loading = document.getElementById('loading-overlay');
    return isElementVisible(loading);
  }

  function isAppReadyForNavigation() {
    return !isLoadingVisible() && !isAuthScreenVisible();
  }

  function goNotFound() {
    if (window.location.pathname === NOT_FOUND_PATH) return;
    debugLog('Routing to not found.');
    window.location.replace(NOT_FOUND_PATH);
  }

  function findElementByOnclickMatch(keyword, id) {
    const candidates = document.querySelectorAll('[onclick]');
    const patterns = [
      `${keyword}('${id}')`,
      `${keyword}(\"${id}\")`,
      `${keyword}(&quot;${id}&quot;)`
    ];
    for (const el of candidates) {
      const onclickValue = el.getAttribute('onclick') || '';
      if (patterns.some((pattern) => onclickValue.includes(pattern))) return el;
    }
    return null;
  }

  function isProfileRendered(route) {
    const container = document.getElementById('view-public-profile');
    if (!container || !isElementVisible(container)) return false;
    const header = container.querySelector('.profile-header');
    if (!header) return false;
    const text = header.textContent || '';
    if (text.includes('Unknown User') || text.includes('@unknown')) return 'not-found';
    if (route.handle) {
      const normalized = route.handle.replace(/^@/, '').toLowerCase();
      return text.toLowerCase().includes(`@${normalized}`);
    }
    return true;
  }

  function isThreadRendered(postId) {
    const title = document.getElementById('thread-view-title');
    if (!title || !isElementVisible(title)) return false;
    return title.dataset.postId === postId;
  }

  function isVideoModalOpen(videoId) {
    const modal = document.getElementById('video-detail-modal');
    if (!modal || !isElementVisible(modal)) return false;
    return modal.dataset.videoId === videoId;
  }

  function clearPendingRestore() {
    routerState.pendingRestore = null;
    if (routerState.pendingTimer) {
      clearInterval(routerState.pendingTimer);
      routerState.pendingTimer = null;
    }
    if (routerState.pendingObserver) {
      routerState.pendingObserver.disconnect();
      routerState.pendingObserver = null;
    }
  }

  function clearSectionRestore() {
    routerState.desiredSection = null;
    routerState.sectionRestoreActive = false;
    routerState.sectionRestoreStart = null;
    if (routerState.sectionRestoreTimer) {
      clearInterval(routerState.sectionRestoreTimer);
      routerState.sectionRestoreTimer = null;
    }
    if (routerState.sectionRestoreObserver) {
      routerState.sectionRestoreObserver.disconnect();
      routerState.sectionRestoreObserver = null;
    }
  }

  function attemptSectionRestore() {
    if (!routerState.sectionRestoreActive || !routerState.desiredSection) return;
    if (routerState.sectionRestoreStart && Date.now() - routerState.sectionRestoreStart > 15000) {
      clearSectionRestore();
      return;
    }
    if (!isAppReadyForNavigation()) return;
    if (routerState.currentView === routerState.desiredSection) {
      clearSectionRestore();
      return;
    }
    if (typeof window.navigateTo === 'function') {
      debugLog('Restoring section route:', routerState.desiredSection);
      window.navigateTo(routerState.desiredSection, false);
    }
  }

  function scheduleSectionRestore(view) {
    // App boot may call navigateTo('feed') after auth; keep retrying until desired section is active.
    clearSectionRestore();
    routerState.desiredSection = view;
    routerState.sectionRestoreActive = true;
    routerState.sectionRestoreStart = Date.now();
    attemptSectionRestore();
    routerState.sectionRestoreObserver = new MutationObserver(() => attemptSectionRestore());
    routerState.sectionRestoreObserver.observe(document.body, { childList: true, subtree: true });
    routerState.sectionRestoreTimer = setInterval(() => attemptSectionRestore(), 500);
  }

  function attemptRestorePending() {
    const pending = routerState.pendingRestore;
    if (!pending) return;

    const { route, startedAt, timeoutMs } = pending;
    if (Date.now() - startedAt > timeoutMs) {
      if (route.viewType === 'video' && pending.exists !== false) {
        if (pending.exists === true && typeof window.toast === 'function') {
          window.toast('Still loading the video. Please try again in a moment.', 'info');
        }
        clearPendingRestore();
        return;
      }
      clearPendingRestore();
      goNotFound();
      return;
    }

    if (route.viewType === 'profile') {
      const rendered = isProfileRendered(route);
      if (rendered === true) {
        clearPendingRestore();
        return;
      }
      if (rendered === 'not-found') {
        clearPendingRestore();
        goNotFound();
        return;
      }
      if (!isAppReadyForNavigation()) return;
      if (!pending.attempted) {
        pending.attempted = true;
        if (route.id && typeof window.openUserProfile === 'function') {
          window.openUserProfile(route.id, null, false);
        } else if (route.handle && typeof window.openUserProfileByHandle === 'function') {
          window.openUserProfileByHandle(route.handle);
        }
      }
      return;
    }

    if (route.viewType === 'video') {
      if (routerState.currentView !== 'videos' && isAppReadyForNavigation()) {
        if (typeof window.navigateTo === 'function') {
          window.navigateTo('videos', false);
        }
      }
      if (isVideoModalOpen(route.id)) {
        clearPendingRestore();
        return;
      }
      if (!pending.directAttempted) {
        pending.directAttempted = true;
        const cachedDoc = routerState.videoDocCache.get(route.id) || null;
        if (cachedDoc && typeof window.NexeraApp?.ensureVideoInCache === 'function') {
          window.NexeraApp.ensureVideoInCache(cachedDoc);
        }
        if (typeof window.openVideoDetail === 'function') {
          window.openVideoDetail(route.id);
        }
      }
      if (pending.exists === false) {
        clearPendingRestore();
        goNotFound();
        return;
      }
      if (pending.exists === true && !pending.notified && Date.now() - startedAt > 2000) {
        if (typeof window.toast === 'function') {
          window.toast('Opening video…', 'info');
        }
        pending.notified = true;
      }
      const videoEl = document.querySelector(`[data-video-open="${route.id}"]`);
      if (videoEl) {
        videoEl.click();
        return;
      }
      if (!pending.attempted && typeof window.openVideoDetail === 'function') {
        pending.attempted = true;
        window.openVideoDetail(route.id);
      }
      return;
    }

    if (route.viewType === 'post') {
      if (isThreadRendered(route.id)) {
        clearPendingRestore();
        return;
      }
      const postEl = findElementByOnclickMatch('openThread', route.id);
      if (postEl) {
        postEl.click();
        return;
      }
      if (!pending.attempted && typeof window.openThread === 'function' && isAppReadyForNavigation()) {
        pending.attempted = true;
        window.openThread(route.id);
      }
    }
  }

  function schedulePendingRestore(route) {
    clearPendingRestore();
    routerState.pendingRestore = {
      route,
      startedAt: Date.now(),
      timeoutMs: 10000,
      attempted: false,
      notified: false,
      exists: null,
      directAttempted: false
    };

    attemptRestorePending();
    routerState.pendingObserver = new MutationObserver(() => attemptRestorePending());
    routerState.pendingObserver.observe(document.body, { childList: true, subtree: true });
    routerState.pendingTimer = setInterval(() => attemptRestorePending(), 500);

    if (route.viewType === 'video') {
      fetchVideoDoc(route.id).then((docData) => {
        if (!routerState.pendingRestore || routerState.pendingRestore.route !== route) return;
        if (docData && typeof window.NexeraApp?.ensureVideoInCache === 'function') {
          window.NexeraApp.ensureVideoInCache(docData);
        }
        const exists = docData ? true : routerState.videoExistenceCache.get(route.id);
        routerState.pendingRestore.exists = exists ?? null;
        if (exists) {
          routerState.pendingRestore.timeoutMs = 25000;
          if (typeof window.openVideoDetail === 'function') {
            window.openVideoDetail(route.id);
          }
        } else if (exists === false) {
          debugLog('Video does not exist, routing to not found.');
          clearPendingRestore();
          goNotFound();
        }
      });
    }
  }

  function applyRoute(route) {
    if (!route) return;

    if (route.viewType === 'not-found') {
      return;
    }

    if (route.viewType === 'unknown') {
      debugLog('Unknown route, redirecting to not found:', route.path);
      goNotFound();
      return;
    }

    if (route.viewType === 'section') {
      clearPendingRestore();
      const view = route.view || 'feed';
      if (routerState.isReplaying) {
        scheduleSectionRestore(view);
      } else if (typeof window.navigateTo === 'function') {
        window.navigateTo(view, false);
      }
      return;
    }

    if (route.viewType === 'profile') {
      schedulePendingRestore(route);
      if (typeof window.navigateTo === 'function') {
        window.navigateTo('feed', false);
      }
      return;
    }

    if (route.viewType === 'post') {
      schedulePendingRestore(route);
      if (typeof window.navigateTo === 'function') {
        window.navigateTo('feed', false);
      }
      return;
    }

    if (route.viewType === 'video') {
      schedulePendingRestore(route);
      if (typeof window.navigateTo === 'function') {
        window.navigateTo('videos', false);
      }
    }
  }

  function mapViewId(viewId) {
    if (!viewId) return null;
    if (viewId === 'home') return 'feed';
    return viewId;
  }

  function extractOnclickId(value, fnName) {
    const regex = new RegExp(`${fnName}\\(\"([^\"]+)\"|\\'([^\\']+)\\')`);
    const match = value.match(regex);
    if (!match) return null;
    return match[2] || match[1] || null;
  }

  function handleClick(event) {
    const navBtn = event.target.closest('.nav-item, .mobile-nav-btn');
    if (navBtn) {
      const rawView = navBtn.dataset.view || navBtn.id?.replace('nav-', '') || null;
      if (rawView) routerState.pendingDetail = null;
      return;
    }

    const videoOpen = event.target.closest('[data-video-open]');
    if (videoOpen) {
      const videoId = videoOpen.getAttribute('data-video-open');
      if (videoId) routerState.pendingDetail = { type: 'video', id: videoId };
      return;
    }

    const clickable = event.target.closest('[onclick]');
    if (!clickable) return;
    const onclickValue = clickable.getAttribute('onclick') || '';
    if (onclickValue.includes('openThread')) {
      const postId = extractOnclickId(onclickValue, 'openThread');
      if (postId) routerState.pendingDetail = { type: 'post', id: postId };
      return;
    }
    if (onclickValue.includes('openUserProfile')) {
      const uid = extractOnclickId(onclickValue, 'openUserProfile');
      if (uid) routerState.pendingDetail = { type: 'profile', id: uid };
      return;
    }
    if (onclickValue.includes('openVideoDetail')) {
      const videoId = extractOnclickId(onclickValue, 'openVideoDetail');
      if (videoId) routerState.pendingDetail = { type: 'video', id: videoId };
    }
  }

  function wrapNavigateTo() {
    if (typeof window.navigateTo !== 'function' || window.navigateTo.__nexeraWrapped) return;
    const original = window.navigateTo;
    window.navigateTo = function (viewId, pushToStack = true) {
      const resolvedView = mapViewId(viewId);
      const nextPush = routerState.isReplaying ? false : pushToStack;
      const result = original.call(this, resolvedView, nextPush);
      routerState.currentView = resolvedView;

      if (!routerState.isReplaying) {
        let path = viewToPath[resolvedView];
        if (resolvedView === 'public-profile' && routerState.pendingDetail?.type === 'profile') {
          path = buildLink.profile({ uid: routerState.pendingDetail.id, handle: routerState.pendingDetail.handle });
        }
        if (resolvedView === 'thread' && routerState.pendingDetail?.type === 'post') {
          path = buildLink.post(routerState.pendingDetail.id);
        }
        if (path) updateUrl(path);
      }

      if (resolvedView === 'public-profile' || resolvedView === 'thread') {
        routerState.pendingDetail = null;
      }

      return result;
    };
    window.navigateTo.__nexeraWrapped = true;
  }

  function wrapOpenUserProfile() {
    if (typeof window.openUserProfile !== 'function' || window.openUserProfile.__nexeraWrapped) return;
    const original = window.openUserProfile;
    window.openUserProfile = function (uid, event, pushToStack = true) {
      if (uid) {
        if (routerState.pendingDetail?.handle) {
          routerState.pendingDetail.id = uid;
        } else {
          routerState.pendingDetail = { type: 'profile', id: uid };
        }
      }
      return original.call(this, uid, event, routerState.isReplaying ? false : pushToStack);
    };
    window.openUserProfile.__nexeraWrapped = true;
  }

  function wrapOpenUserProfileByHandle() {
    if (typeof window.openUserProfileByHandle !== 'function' || window.openUserProfileByHandle.__nexeraWrapped) return;
    const original = window.openUserProfileByHandle;
    window.openUserProfileByHandle = function (handle) {
      if (handle) routerState.pendingDetail = { type: 'profile', handle };
      return original.call(this, handle);
    };
    window.openUserProfileByHandle.__nexeraWrapped = true;
  }

  function wrapOpenThread() {
    if (typeof window.openThread !== 'function' || window.openThread.__nexeraWrapped) return;
    const original = window.openThread;
    window.openThread = function (postId) {
      if (postId) routerState.pendingDetail = { type: 'post', id: postId };
      return original.call(this, postId);
    };
    window.openThread.__nexeraWrapped = true;
  }

  function wrapOpenVideoDetail() {
    if (typeof window.openVideoDetail !== 'function' || window.openVideoDetail.__nexeraWrapped) return;
    const original = window.openVideoDetail;
    window.openVideoDetail = function (videoId) {
      if (videoId) routerState.pendingDetail = { type: 'video', id: videoId };
      const result = original.call(this, videoId);
      if (!routerState.isReplaying && videoId) {
        const modal = document.getElementById('video-detail-modal');
        if (!modal || modal.dataset.videoId === videoId) {
          updateUrl(buildLink.video(videoId));
        } else {
          setTimeout(() => {
            const checkModal = document.getElementById('video-detail-modal');
            if (checkModal && checkModal.dataset.videoId === videoId) {
              updateUrl(buildLink.video(videoId));
            }
          }, 200);
        }
      }
      return result;
    };
    window.openVideoDetail.__nexeraWrapped = true;
  }

  function wrapCloseVideoDetail() {
    if (typeof window.closeVideoDetail !== 'function' || window.closeVideoDetail.__nexeraWrapped) return;
    const original = window.closeVideoDetail;
    window.closeVideoDetail = function () {
      const result = original.call(this);
      if (!routerState.isReplaying) {
        const fallbackView = routerState.currentView || 'videos';
        const path = viewToPath[fallbackView] || '/videos';
        updateUrl(path);
      }
      return result;
    };
    window.closeVideoDetail.__nexeraWrapped = true;
  }

  function wrapOpenMessagesPage() {
    if (typeof window.openMessagesPage !== 'function' || window.openMessagesPage.__nexeraWrapped) return;
    const original = window.openMessagesPage;
    window.openMessagesPage = async function () {
      const result = await original.call(this);
      return result;
    };
    window.openMessagesPage.__nexeraWrapped = true;
  }

  function wrapGoBack() {
    if (typeof window.goBack !== 'function' || window.goBack.__nexeraWrapped) return;
    const original = window.goBack;
    window.goBack = function () {
      const result = original.call(this);
      const fallbackView = routerState.currentView;
      if (!routerState.isReplaying && fallbackView) {
        const path = viewToPath[fallbackView];
        if (path) updateUrl(path);
      }
      return result;
    };
    window.goBack.__nexeraWrapped = true;
  }

  function initRouter() {
    document.addEventListener('click', handleClick, { passive: true });
    wrapNavigateTo();
    wrapOpenUserProfile();
    wrapOpenUserProfileByHandle();
    wrapOpenThread();
    wrapOpenVideoDetail();
    wrapCloseVideoDetail();
    wrapOpenMessagesPage();
    wrapGoBack();

    const route = parseRoute();
    debugLog('Parsed initial route:', route);
    withReplayGuard(() => {
      applyRoute(route);
    });

    if (window.NexeraApp?.onAuthReady) {
      window.NexeraApp.onAuthReady(() => {
        debugLog('Auth ready, reapplying route.');
        withReplayGuard(() => applyRoute(parseRoute()));
      });
    }

    if (route?.canonical && (route.fromHash || route.canonical !== window.location.pathname)) {
      replaceUrl(route.canonical);
    }

    window.addEventListener('popstate', () => {
      withReplayGuard(() => {
        const route = parseRoute();
        applyRoute(route);
      });
    });
  }

  window.NexeraRouter = {
    initialized: true,
    buildLink,
    parseRoute,
    restoreFromUrl: () => withReplayGuard(() => applyRoute(parseRoute())),
    applyRoute: (route) => withReplayGuard(() => applyRoute(route))
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initRouter, { once: true });
  } else {
    initRouter();
  }
})();
