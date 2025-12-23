(() => {
  if (window.NexeraRouter && window.NexeraRouter.initialized) return;

  const routerState = {
    initialized: true,
    currentView: null,
    isReplaying: false,
    pendingDetail: null,
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

  function parseRouteFromPath(pathname, searchParams) {
    const normalized = normalizePath(pathname);
    const parts = normalized.split('/').filter(Boolean);

    if (normalized.startsWith('/pages/')) {
      const pageName = normalized.split('/').pop();
      const defaultRoute = pageDefaults[pageName];
      const fallback = window.NEXERA_PAGE_ROUTE || window.NEXERA_DEFAULT_ROUTE || defaultRoute;
      if (fallback) {
        return { type: 'section', view: pathToView[fallback.replace('/', '')] || 'feed', canonical: fallback };
      }
    }

    if (normalized === '/' || normalized === '') {
      const fallbackRoute = window.NEXERA_PAGE_ROUTE || window.NEXERA_DEFAULT_ROUTE || '/home';
      return { type: 'section', view: pathToView[fallbackRoute.replace('/', '')] || 'feed', canonical: fallbackRoute };
    }

    const head = parts[0];
    if (head === 'profile') {
      const uid = parts[1] || searchParams.get('uid');
      const handle = searchParams.get('handle');
      if (uid || handle) {
        return { type: 'profile', id: uid || null, handle: handle || null };
      }
      return { type: 'section', view: 'profile' };
    }
    if (head === 'video') {
      const id = parts[1] || searchParams.get('id');
      if (id) return { type: 'video', id };
      return { type: 'section', view: 'videos' };
    }
    if (head === 'post') {
      const id = parts[1] || searchParams.get('id');
      if (id) return { type: 'post', id };
      return { type: 'section', view: 'feed' };
    }

    const view = pathToView[head];
    if (view) return { type: 'section', view };

    return { type: 'section', view: 'feed' };
  }

  function parseLocation() {
    const hashRoute = parseHashRoute();
    const searchParams = new URLSearchParams(window.location.search || '');
    if (hashRoute) {
      const parsed = parseRouteFromPath(hashRoute, searchParams);
      parsed.canonical = hashRoute;
      parsed.fromHash = true;
      return parsed;
    }
    return parseRouteFromPath(window.location.pathname, searchParams);
  }

  function withReplayGuard(fn) {
    routerState.isReplaying = true;
    try {
      fn();
    } finally {
      routerState.isReplaying = false;
    }
  }

  function openVideoWithRetry(videoId, attempts = 8) {
    if (!videoId || typeof window.openVideoDetail !== 'function') return;
    const modal = document.getElementById('video-detail-modal');
    const alreadyOpen = modal && modal.dataset.videoId === videoId;
    if (alreadyOpen) return;

    const attemptOpen = () => {
      if (routerState.isReplaying) {
        window.openVideoDetail(videoId);
      } else {
        window.openVideoDetail(videoId);
      }
      const reopened = modal && modal.dataset.videoId === videoId;
      if (!reopened && attempts > 0) {
        setTimeout(() => openVideoWithRetry(videoId, attempts - 1), 450);
      }
    };

    attemptOpen();
  }

  function applyRoute(route) {
    if (!route) return;
    if (route.type === 'section') {
      const view = route.view || 'feed';
      if (typeof window.navigateTo === 'function') {
        window.navigateTo(view, false);
      }
      return;
    }

    if (route.type === 'profile') {
      if (route.id && typeof window.openUserProfile === 'function') {
        window.openUserProfile(route.id, null, false);
        return;
      }
      if (route.handle && typeof window.openUserProfileByHandle === 'function') {
        window.openUserProfileByHandle(route.handle);
        return;
      }
      if (typeof window.navigateTo === 'function') {
        window.navigateTo('profile', false);
      }
      return;
    }

    if (route.type === 'post') {
      if (typeof window.openThread === 'function') {
        window.openThread(route.id);
      }
      return;
    }

    if (route.type === 'video') {
      if (typeof window.navigateTo === 'function') {
        window.navigateTo('videos', false);
      }
      openVideoWithRetry(route.id);
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

    const route = parseLocation();
    withReplayGuard(() => {
      applyRoute(route);
    });

    if (route?.canonical && (route.fromHash || route.canonical !== window.location.pathname)) {
      replaceUrl(route.canonical);
    }

    window.addEventListener('popstate', () => {
      withReplayGuard(() => {
        const route = parseLocation();
        applyRoute(route);
      });
    });
  }

  window.NexeraRouter = {
    initialized: true,
    buildLink,
    parseLocation,
    applyRoute: (route) => withReplayGuard(() => applyRoute(route))
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initRouter, { once: true });
  } else {
    initRouter();
  }
})();
