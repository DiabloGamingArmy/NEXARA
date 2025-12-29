// UI-only: inbox content filters, video modal mounting, and profile cover controls.
import { initializeApp } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-app.js";
import { getAuth, signInWithEmailAndPassword, createUserWithEmailAndPassword, signInAnonymously, onAuthStateChanged, signOut, updateProfile } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-auth.js";
import { initializeFirestore, getFirestore, collection, addDoc, onSnapshot, query, orderBy, serverTimestamp, doc, setDoc, getDoc, updateDoc, deleteDoc, deleteField, arrayUnion, arrayRemove, increment, where, getDocs, collectionGroup, limit, startAt, startAfter, endAt, Timestamp, runTransaction, writeBatch } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";
import { getStorage, ref, uploadBytes, uploadBytesResumable, getDownloadURL, deleteObject } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-storage.js";
import { getFunctions, httpsCallable } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-functions.js";
import { getMessaging, getToken, onMessage, deleteToken as deleteFcmToken } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-messaging.js";
import { normalizeReplyTarget, buildReplyRecord, groupCommentsByParent } from "/scripts/commentUtils.js";
import { buildTopBar, buildTopBarControls } from "/scripts/ui/topBar.js";
import { NexeraGoLiveController } from "/scripts/GoLive.js";
import { createUploadManager } from "/scripts/uploadManager.js";
import { buildMessagesUrl } from "/assets/js/routes.js";
import { buildChatMediaPath, CHAT_ALLOWED_MIME_PREFIXES, CHAT_IMAGE_MAX_BYTES, CHAT_VIDEO_MAX_BYTES, sanitizeFileName, validateChatAttachment } from "/assets/js/upload-utils.js";
import { buildVideosHeader } from "/scripts/ui/VideosHeader.js";
import { buildVideoViewerLayout } from "/scripts/ui/VideoViewerPanel.js";
import { renderDiscoverHub } from "/scripts/ui/DiscoverHub.js";
import { enhanceInboxLayout } from "/scripts/ui/InboxEnhancements.js";
import { buildVideoCardElement } from "/scripts/ui/VideoCard.js";
import { renderStoriesAndLiveBar } from "/scripts/ui/StoriesAndLiveBar.js";

// --- Firebase Configuration --- 
const firebaseConfig = {
    apiKey: "AIzaSyDg9Duz3xicI3pvvOtLCrV1DJRWDI0NtYA",
    authDomain: "spike-streaming-service.firebaseapp.com",
    projectId: "spike-streaming-service",
    storageBucket: "spike-streaming-service.firebasestorage.app",
    messagingSenderId: "592955741032",
    appId: "1:592955741032:web:dbd629cc957b67fc69bcdd",
    measurementId: "G-BF3GFFY3D6"
};

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const isSafari = /^((?!chrome|android).)*safari/i.test(navigator.userAgent);
const db = isSafari
    ? initializeFirestore(app, { experimentalForceLongPolling: true, useFetchStreams: false })
    : getFirestore(app);
const storage = getStorage(app);
const functions = getFunctions(app);
const messaging = getMessaging(app);
const FCM_VAPID_KEY = window.NEXERA_FCM_VAPID_KEY || '';
const LIVEKIT_URL = window.NEXERA_LIVEKIT_URL || '';
const LIVEKIT_ENABLED = !!LIVEKIT_URL && !!window.LivekitClient;
const LivekitClient = window.LivekitClient;
const LivekitRoom = LivekitClient?.Room;
const LivekitRoomEvent = LivekitClient?.RoomEvent;
const createLivekitAudioTrack = LivekitClient?.createLocalAudioTrack;
const createLivekitVideoTrack = LivekitClient?.createLocalVideoTrack;

// --- Global State & Cache ---
let currentUser = null;
let allPosts = [];
let userCache = {};
let userFetchPromises = {};
const USER_CACHE_TTL_MS = 10 * 60 * 1000;
window.myReviewCache = {}; // Global cache for reviews
let currentCategory = 'For You';
const USE_CUSTOM_VIDEO_VIEWER = true;
const USE_INLINE_VIDEO_WATCH = false;
const USE_INLINE_VIDEO_VIEWER = true;
const VIDEO_DEBUG = window.__NEXERA_DEBUG_VIDEO === true || window.__NEXERA_DEBUG_VIDEOS === true;
let currentProfileFilter = 'All Results';
let discoverFilter = 'All Results';
let discoverSearchTerm = '';
let discoverSearchDebounce = null;
let discoverPostsSort = 'recent';

// UI scaffolding stubs (no backend calls yet).
window.handleUiStubAction = function (action) {
    console.log('UI placeholder action:', action);
};
const debugVideo = function (...args) {
    if (VIDEO_DEBUG) {
        console.log('[VideoViewer]', ...args);
    }
};
let discoverCategoriesMode = 'verified_first';
let savedSearchTerm = '';
let savedFilter = 'All Saved';
const SEARCH_QUERY_KEY = 'q';
const SEARCH_DEBOUNCE_MS = 300;
let preserveFeedState = false;
let pendingFeedScrollRestore = null;
let feedScrollRestoreToken = 0;
let forceConversationScroll = false;

const GO_LIVE_MODE_STORAGE_KEY = 'nexera-go-live-mode';
let videoSearchTerm = '';
let videoSearchDebounce = null;
let videoFilter = 'All';
let videoSortMode = 'recent';
let pendingVideoPreviewUrl = null;
let pendingVideoThumbnailBlob = null;

let messagingRegistration = null;
let messagingListenerReady = false;
const PUSH_TOKEN_STORAGE_KEY = 'nexera_push_token';

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () {
        ensurePushSettingsUI();
        initMessagingForegroundListener();
    });
} else {
    ensurePushSettingsUI();
    initMessagingForegroundListener();
}
let pendingVideoThumbnailUrl = null;
let pendingVideoHasCustomThumbnail = false;
let pendingVideoDurationSeconds = null;
let videoTags = [];
let videoMentions = [];
let videoMentionSearchTimer = null;
let videoUploadMode = 'create';
let editingVideoId = null;
let editingVideoData = null;
let videoDestinationLoading = false;
let videoDestinationError = '';
let videoDestinationLoaded = false;
let videoCategories = [];
let videoCategoryIndex = new Map();
let miniPlayerState = null;
let miniPlayerMode = null;
let videoDetailReturnPath = '/videos';
let videoModalResumeTime = null;
let pendingVideoOpenId = null;
let profileReturnContext = null;
let lastVideoTrigger = null;
let videoViewerInlineState = null;
let messageTypingTimer = null;
let conversationListFilter = 'all';
let conversationListSearchTerm = '';
let conversationListVisibleCount = 30;
let inboxMode = 'messages';
let inboxNotificationsUnsubscribe = null;
let inboxNotifications = [];
let contentNotificationsUnsubscribe = null;
let contentNotifications = [];
let contentNotificationsLegacyFetched = false;
let inboxNotificationCounts = { posts: 0, videos: 0, livestreams: 0, account: 0 };
let inboxContentFilters = { posts: true, videos: true, livestreams: true };
let inboxContentPreferred = 'posts';
let inboxModeRestored = false;
const USE_UPLOAD_SESSION = false;
var uploadTasks = window.uploadTasks || (window.uploadTasks = []);
var activeUploadId = window.activeUploadId || null;
let liveSearchTerm = '';
let liveSearchDebounce = null;
let liveSortMode = 'featured';
let liveCategoryFilter = 'All';
let liveTagFilter = '';
let liveTagSearchDebounce = null;
let isInitialLoad = true;
let feedLoading = false;
let feedHydrationPromise = null;
const FEED_BATCH_SIZE = 5;
const FEED_PREFETCH_OFFSET = 2;
const FEED_PREFETCH_ROOT_MARGIN = '0px 0px 400px 0px';
const animatedItemKeys = new Set();
const videoDurationBackfill = new Set();
const feedPagination = {
    lastDoc: null,
    loading: false,
    done: false
};
let composerTags = [];
let composerCreatedTags = new Set();
let composerMentions = [];
let composerPoll = { title: '', options: ['', ''] };
let composerScheduledFor = '';
let composerLocation = '';
let recentLocations = [];
let categoryVisibleCount = 10;
let commentRootDisplayCount = {};
let replyExpansionState = {};
let currentEditPost = null;
let goLiveController = null;
let tagSuggestionPool = [];
let tagSuggestionState = { query: '', loading: false, token: 0 };
let composerNewTagNotice = '';
let videoNewTagNotice = '';
let mentionSearchTimer = null;
let mentionSuggestionState = {
    query: '',
    lastDoc: null,
    hasMore: false,
    loading: false,
    results: [],
    visibleCount: 5
};
const MENTION_SUGGESTION_PAGE_SIZE = 30;
let currentThreadComments = [];
let liveSessionsCache = [];
let homeVideosCache = [];
let homeLiveSessionsCache = [];
let homeMediaPromise = null;
let homeMediaLoading = false;
let profileMediaPrefetching = {};
let uploadManager = null;
let videoTaskViewerBound = false;
let videoManagerMenuState = { videoId: null };

// Optimistic UI Sets
let followedCategories = new Set();
let followedCategoryList = [];
let followedUsers = new Set();
let followedTopicsUnsubscribe = null;
let followingUnsubscribe = null;
const videoEngagementState = {
    liked: new Set(),
    disliked: new Set(),
    saved: new Set()
};
let videoEngagementHydrated = new Set();

// Snapshot cache to diff changes for thread rendering
let postSnapshotCache = {};

// Category state
let categories = [];
let memberships = {};
let selectedCategoryId = null;
let destinationPickerTab = 'community';
let destinationPickerSearch = '';
let destinationPickerOpen = false;
let destinationPickerLoading = true;
let destinationPickerError = '';
let destinationSearchTimeout = null;
let destinationCreateExpanded = false;
let destinationPickerTarget = 'post';
let destinationPickerSelectionId = null;
let videoPostingDestinationId = null;
let videoPostingDestinationName = '';
let categoryUnsubscribe = null;
let membershipUnsubscribe = null;
let authClaims = {};
let composerError = '';
const DEFAULT_DESTINATION_CONFIG = {
    enableOfficialTab: true,
    enableCommunityTab: true,
    enableCreateCommunity: true,
    officialTabLabel: 'Official (Verified)',
    communityTabLabel: 'Community',
    officialSelectable: true
};
let activeDestinationConfig = { ...DEFAULT_DESTINATION_CONFIG };

const REVIEW_CLASSES = ['review-verified', 'review-citation', 'review-misleading'];
const MOBILE_SECTION_LABELS = {
    feed: 'Home',
    live: 'Live',
    videos: 'Videos',
    messages: 'Inbox',
    discover: 'Discover',
    saved: 'Saved',
    profile: 'Profile',
    staff: 'Staff',
    'live-setup': 'Live',
    thread: 'Post'
};

const BRAND_LOGO_VARIANTS = {
    icon: 'https://firebasestorage.googleapis.com/v0/b/spike-streaming-service.firebasestorage.app/o/apps%2Fnexera%2Fassets%2Ficons%2Ficon.png?alt=media&token=3db62710-7412-46bf-a981-ace4715e2bc6',
    dark: 'https://firebasestorage.googleapis.com/v0/b/spike-streaming-service.firebasestorage.app/o/apps%2Fnexera%2Fassets%2Ficons%2Fwhiteicon.png?alt=media&token=366d09a9-61f6-4096-af08-a01a119c339e',
    light: 'https://firebasestorage.googleapis.com/v0/b/spike-streaming-service.firebasestorage.app/o/apps%2Fnexera%2Fassets%2Ficons%2Fblackicon.png?alt=media&token=52db20ec-c992-4487-9f1c-ee497514e26a'
};

const UPLOAD_SESSION_ENDPOINT = 'https://us-central1-spike-streaming-service.cloudfunctions.net/createUploadSession';

function resolveBrandLogoVariant(el) {
    if (!el) return BRAND_LOGO_VARIANTS.icon;
    const variant = el.dataset.logoVariant || 'auto';
    if (variant === 'icon') return BRAND_LOGO_VARIANTS.icon;
    if (variant === 'dark') return BRAND_LOGO_VARIANTS.dark;
    if (variant === 'light') return BRAND_LOGO_VARIANTS.light;
    const background = el.dataset.logoBackground;
    if (background === 'dark') return BRAND_LOGO_VARIANTS.dark;
    if (background === 'light') return BRAND_LOGO_VARIANTS.light;
    const prefersLight = document.body.classList.contains('light-mode');
    return prefersLight ? BRAND_LOGO_VARIANTS.light : BRAND_LOGO_VARIANTS.dark;
}

function refreshBrandLogos() {
    const logoNodes = document.querySelectorAll('[data-logo-variant]');
    logoNodes.forEach(function (node) {
        const resolved = resolveBrandLogoVariant(node);
        if (node.getAttribute('src') !== resolved) {
            node.setAttribute('src', resolved);
        }
    });
}
window.refreshBrandLogos = refreshBrandLogos;

function isUiDebugEnabled() {
    return window.DEBUG_UI === true;
}

function uiDebugLog(...args) {
    if (!isUiDebugEnabled()) return;
    console.debug('[NexeraUI]', ...args);
}

function shouldAnimateItem(key) {
    if (!key) return true;
    if (animatedItemKeys.has(key)) return false;
    animatedItemKeys.add(key);
    return true;
}

function showSplash() {
    const splash = document.getElementById('nexera-splash');
    if (!splash) return;
    startSplashFailsafeTimer();
    splash.style.display = 'flex';
    splash.classList.remove('nexera-splash-hidden');
    splash.style.pointerEvents = 'auto';
    splash.style.visibility = 'visible';
    splash.setAttribute('aria-hidden', 'false');
    logSplashEvent('show');
}

function hideSplash(options = {}) {
    const splash = document.getElementById('nexera-splash');
    const { force = false, reason = 'hide' } = options || {};
    if (!splash) {
        clearSplashFailsafeTimer();
        logSplashEvent('hide-missing', reason);
        return;
    }
    if (!force && window.Nexera?.splashHold) {
        window.Nexera.splashPending = true;
        logSplashEvent('hold-pending', reason);
        return;
    }
    logSplashEvent('hide', reason);
    splash.classList.add('nexera-splash-hidden');
    splash.style.pointerEvents = 'none';
    splash.setAttribute('aria-hidden', 'true');
    uiDebugLog('splash hidden', reason);
    clearSplashFailsafeTimer();
    const TRANSITION_BUFFER = 520;
    setTimeout(function () {
        if (splash.classList.contains('nexera-splash-hidden')) {
            splash.style.visibility = 'hidden';
            splash.style.display = 'none';
        }
    }, TRANSITION_BUFFER);
}

const SPLASH_FAILSAFE_TIMEOUT = 8000;
let splashFailsafeTimer = null;

function getSplashDebugSummary() {
    return {
        path: `${window.location.pathname || '/'}${window.location.search || ''}${window.location.hash || ''}`,
        user: currentUser?.uid || null,
        hold: !!window.Nexera?.splashHold,
        pending: !!window.Nexera?.splashPending
    };
}

function isSplashDebugEnabled() {
    return window.__NEXERA_DEBUG_SPLASH === true;
}

function logSplashEvent(event, detail) {
    if (!isSplashDebugEnabled()) return;
    const summary = getSplashDebugSummary();
    if (detail !== undefined) {
        console.log('[NexeraSplash]', event, detail, summary);
        return;
    }
    console.log('[NexeraSplash]', event, summary);
}

function clearSplashFailsafeTimer() {
    if (!splashFailsafeTimer) return;
    clearTimeout(splashFailsafeTimer);
    splashFailsafeTimer = null;
}

function startSplashFailsafeTimer() {
    if (splashFailsafeTimer) return;
    splashFailsafeTimer = setTimeout(function () {
        const splash = document.getElementById('nexera-splash');
        if (!splash) return;
        const isHidden = splash.classList.contains('nexera-splash-hidden') || splash.style.display === 'none';
        if (!isHidden) {
            console.warn('[NexeraSplash] Failsafe hide triggered', getSplashDebugSummary());
            if (window.Nexera) {
                window.Nexera.splashHold = false;
                window.Nexera.splashPending = false;
            }
            hideSplash({ force: true, reason: 'failsafe-timeout' });
        }
    }, SPLASH_FAILSAFE_TIMEOUT);
    logSplashEvent('failsafe-start', SPLASH_FAILSAFE_TIMEOUT);
}

function getReviewDisplay(reviewValue) {
    if (reviewValue === 'verified') {
        return { label: 'Verified', className: 'review-verified' };
    }
    if (reviewValue === 'citation') {
        return { label: 'Needs Citations', className: 'review-citation' };
    }
    if (reviewValue === 'misleading') {
        return { label: 'Misleading/False', className: 'review-misleading' };
    }
    return { label: 'Audit', className: '' };
}

function getVerificationState(post = {}) {
    const status = (post.reviewStatus || post.trustStatus || '').toLowerCase();
    if (status === 'verified' || post.trustScore > 2) {
        return { label: 'Verified', className: 'verified', bannerText: 'This post is verified by the community.' };
    }
    if (status === 'false' || status === 'misleading' || post.trustScore < -1) {
        return { label: 'False', className: 'false', bannerText: 'This post has been flagged as false by the community.' };
    }
    if (status === 'disputed' || status === 'citation') {
        return { label: 'Disputed', className: 'disputed', bannerText: 'This post is disputed and may require more context.' };
    }
    return null;
}

function getPostOptionsButton(post, context = 'feed', iconSize = '1.1rem') {
    const ownerId = post.userId;
    return `<button class="post-options-btn" onclick="event.stopPropagation(); window.openPostOptions(event, '${post.id}', '${ownerId}', '${context}')" aria-label="Post options"><i class="ph ph-dots-three" style="font-size:${iconSize};"></i></button>`;
}

function renderPostActions(post, {
    isLiked = false,
    isDisliked = false,
    isSaved = false,
    reviewDisplay = null,
    iconSize = '1.1rem',
    discussionLabel = 'Discuss',
    discussionOnclick = null,
    includeReview = true,
    extraClass = '',
    idPrefix = 'post',
    showCounts = true,
    showLabels = true
} = {}) {
    const actions = [];
    const likeCount = post.likes || 0;
    const dislikeCount = post.dislikes || 0;
    const computedReview = reviewDisplay || getReviewDisplay(window.myReviewCache ? window.myReviewCache[post.id] : null);

    const prefix = idPrefix ? `${idPrefix}-` : '';
    const labelFlag = showLabels !== false;
    const countFlag = showCounts !== false;

    function buildActionButton({ id, action, label, iconClass, count = null, onclick, color = 'inherit', extraClasses = '' }) {
        const countMarkup = (!labelFlag && countFlag && typeof count === 'number') ? `<span class="action-count">${count}</span>` : '';
        const labelMarkup = labelFlag ? `<span class="action-label"> ${label}${countFlag && typeof count === 'number' ? ` ${count}` : ''}</span>` : '';
        const aria = countFlag && typeof count === 'number' ? `${label} (${count})` : label;
        return `<button id="${id}" data-post-id="${post.id}" data-action="${action}" data-show-labels="${labelFlag}" data-show-counts="${countFlag}" data-icon-size="${iconSize}" class="action-btn${extraClasses ? ' ' + extraClasses : ''}" aria-label="${aria}" onclick="${onclick}" style="color: ${color}"><i class="${iconClass}" style="font-size:${iconSize};"></i>${labelMarkup}${countMarkup}</button>`;
    }

    actions.push(buildActionButton({
        id: `${prefix}like-btn-${post.id}`,
        action: 'like',
        label: 'Like',
        iconClass: `${isLiked ? 'ph-fill' : 'ph'} ph-thumbs-up`,
        count: likeCount,
        onclick: `window.toggleLike('${post.id}', event)`,
        color: isLiked ? '#00f2ea' : 'inherit'
    }));
    actions.push(buildActionButton({
        id: `${prefix}dislike-btn-${post.id}`,
        action: 'dislike',
        label: 'Dislike',
        iconClass: `${isDisliked ? 'ph-fill' : 'ph'} ph-thumbs-down`,
        count: dislikeCount,
        onclick: `window.toggleDislike('${post.id}', event)`,
        color: isDisliked ? '#ff3d3d' : 'inherit'
    }));
    const discussionTarget = discussionOnclick || `window.openThread('${post.id}')`;
    actions.push(buildActionButton({
        id: `${prefix}discuss-btn-${post.id}`,
        action: 'discuss',
        label: discussionLabel,
        iconClass: 'ph ph-chat-circle',
        count: null,
        onclick: discussionTarget
    }));
    actions.push(buildActionButton({
        id: `${prefix}share-btn-${post.id}`,
        action: 'share',
        label: 'Share',
        iconClass: 'ph ph-paper-plane-tilt',
        count: null,
        onclick: `event.stopPropagation(); window.sharePost('${post.id}', event)`
    }));
    actions.push(buildActionButton({
        id: `${prefix}save-btn-${post.id}`,
        action: 'save',
        label: isSaved ? 'Saved' : 'Save',
        iconClass: `${isSaved ? 'ph-fill' : 'ph'} ph-bookmark-simple`,
        count: null,
        onclick: `window.toggleSave('${post.id}', event)`,
        color: isSaved ? '#00f2ea' : 'inherit'
    }));

    if (includeReview) {
        actions.push(buildActionButton({
            id: `${prefix}review-btn-${post.id}`,
            action: 'review',
            label: computedReview.label,
            iconClass: 'ph ph-scales',
            count: null,
            onclick: `event.stopPropagation(); window.openPeerReview('${post.id}')`,
            extraClasses: `review-action ${computedReview.className}`
        }));
    }

    return `<div class="card-actions${extraClass ? ' ' + extraClass : ''}">${actions.join('')}</div>`;
}

function renderDiscussionActionsMobile(post, {
    isLiked = false,
    isDisliked = false,
    isSaved = false,
    reviewDisplay = null
} = {}) {
    const likeCount = post.likes || 0;
    const dislikeCount = post.dislikes || 0;
    const commentCount = typeof post.comments === 'number' ? post.comments : (Array.isArray(post.comments) ? post.comments.length : (post.commentCount ?? post.commentsCount ?? 0));
    const shareCount = typeof post.shares === 'number' ? post.shares : (post.shareCount ?? 0);
    const saveCount = typeof post.saves === 'number' ? post.saves : (post.saveCount ?? 0);
    const computedReview = reviewDisplay || getReviewDisplay(window.myReviewCache ? window.myReviewCache[post.id] : null);

    function build(idSuffix, label, iconClass, count, onclick, { activeColor = null, extraClasses = '' } = {}) {
        const aria = typeof count === 'number' && count !== null ? `${label} (${count})` : label;
        const colorStyle = activeColor ? ` style="color:${activeColor}"` : '';
        const countMarkup = typeof count === 'number' ? `<span class="action-count">${count}</span>` : '<span class="action-count"></span>';
        return `<button id="thread-${idSuffix}-${post.id}" class="discussion-action-btn${extraClasses ? ' ' + extraClasses : ''}" data-show-labels="true" data-show-counts="true" data-icon-size="1.2rem" onclick="${onclick}" aria-label="${aria}"${colorStyle}><i class="${iconClass}"></i><span class="action-label"> ${label}</span>${countMarkup}</button>`;
    }

    return `<div class="discussion-actions">
        <div class="discussion-action-row">
            ${build('like-btn', 'Like', `${isLiked ? 'ph-fill' : 'ph'} ph-thumbs-up`, likeCount, `window.toggleLike('${post.id}', event)`, { activeColor: isLiked ? '#00f2ea' : null })}
            ${build('dislike-btn', 'Dislike', `${isDisliked ? 'ph-fill' : 'ph'} ph-thumbs-down`, dislikeCount, `window.toggleDislike('${post.id}', event)`, { activeColor: isDisliked ? '#ff3d3d' : null })}
            ${build('share-btn', 'Share', 'ph ph-paper-plane-tilt', shareCount, `event.stopPropagation(); window.sharePost('${post.id}', event)`)}
        </div>
        <div class="discussion-action-row">
            ${build('discuss-btn', 'Comment', 'ph ph-chat-circle', commentCount, `document.getElementById('thread-input').focus()`)}
            ${build('save-btn', isSaved ? 'Saved' : 'Save', `${isSaved ? 'ph-fill' : 'ph'} ph-bookmark-simple`, saveCount, `window.toggleSave('${post.id}', event)`, { activeColor: isSaved ? '#00f2ea' : null })}
            ${build('review-btn', computedReview.label, 'ph ph-scales', null, `event.stopPropagation(); window.openPeerReview('${post.id}')`, { extraClasses: computedReview.className })}
        </div>
    </div>`;
}

function applyReviewButtonState(buttonEl, reviewValue) {
    if (!buttonEl) return;
    const { label, className } = getReviewDisplay(reviewValue);
    const iconSize = buttonEl.dataset.iconSize || '1.1rem';
    buttonEl.classList.remove(...REVIEW_CLASSES);
    if (className) buttonEl.classList.add(className);
    const showLabels = buttonEl.dataset.showLabels !== 'false';
    const showCounts = buttonEl.dataset.showCounts !== 'false';
    const labelMarkup = showLabels ? `<span class="action-label"> ${label}</span>` : '';
    const countMarkup = (!showLabels && showCounts) ? `<span class="action-count"></span>` : '';
    buttonEl.innerHTML = `<i class="ph ph-scales" style="font-size:${iconSize};"></i>${labelMarkup}${countMarkup}`;
    buttonEl.setAttribute('aria-label', label);
}

function applyMyReviewStylesToDOM() {
    const cache = window.myReviewCache || {};
    document.querySelectorAll('.review-action').forEach(function (btn) {
        const pid = btn.dataset.postId;
        applyReviewButtonState(btn, pid ? cache[pid] : null);
    });
}

let userProfile = {
    name: "Nexera User",
    realName: "",
    nickname: "",
    username: "nexera_explorer",
    bio: "Stream, Socialize, and Strive.",
    links: "mysite.com",
    email: "",
    phone: "",
    gender: "Prefer not to say",
    region: "",
    photoURL: "",
    photoPath: "",
    avatarColor: "",
    theme: "system",
    accountRoles: [],
    savedPosts: [],
    savedVideos: [],
    following: [],
    followersCount: 0,
    followedCategories: []
};

const AVATAR_COLORS = ['#9b8cff', '#6dd3ff', '#ffd166', '#ff7b9c', '#a3f7bf', '#ffcf99', '#8dd3c7', '#f8b195'];
const AVATAR_TEXT_COLOR = '#0f172a';
let avatarColorBackfilled = false;

function computeAvatarColor(seed = 'user') {
    let hash = 0;
    for (let i = 0; i < seed.length; i++) {
        hash = (hash << 5) - hash + seed.charCodeAt(i);
        hash |= 0;
    }
    return AVATAR_COLORS[Math.abs(hash) % AVATAR_COLORS.length];
}

function normalizeUsername(value = '') {
    return (value || '').toString().trim().replace(/^@/, '');
}

function resolveDisplayName(userLike = {}) {
    return userLike.displayName || userLike.name || userLike.fullName || userLike.nickname || '';
}

function resolveAvatarInitial(userLike = {}) {
    const source = resolveDisplayName(userLike) || userLike.username || 'U';
    return (source || 'U').trim().charAt(0).toUpperCase() || 'U';
}

function ensureAvatarColor(profile = {}, uid = '') {
    if (profile.avatarColor) return profile.avatarColor;
    const color = computeAvatarColor(uid || profile.username || resolveDisplayName(profile) || 'user');
    profile.avatarColor = color;
    return color;
}

function resolveAvatarData(userLike = {}, uidOverride = '') {
    const uid = uidOverride || userLike.uid || userLike.id || '';
    const avatarColor = ensureAvatarColor(userLike, uid);
    const initial = resolveAvatarInitial(userLike);
    return { photoURL: userLike.photoURL || '', avatarColor, initial };
}

function renderAvatar(userLike = {}, options = {}) {
    const { size = 42, className = '', shape = 'circle' } = options;
    const data = resolveAvatarData(userLike);
    const hasPhoto = !!data.photoURL;
    const background = hasPhoto
        ? `background-image:url('${data.photoURL}'); background-size:cover; background-position:center; color:transparent;`
        : `background:${data.avatarColor}; color:${AVATAR_TEXT_COLOR};`;
    const radius = shape === 'rounded' ? '12px' : '50%';
    return `<div class="user-avatar ${className}" style="width:${size}px; height:${size}px; border-radius:${radius}; ${background}">${hasPhoto ? '' : data.initial}</div>`;
}

function applyAvatarToElement(el, userLike = {}, options = {}) {
    if (!el) return;
    const { size } = options;
    const data = resolveAvatarData(userLike);
    const hasPhoto = !!data.photoURL;
    el.classList.add('user-avatar');
    if (size) {
        el.style.width = `${size}px`;
        el.style.height = `${size}px`;
    }
    el.style.borderRadius = el.classList.contains('profile-pic') ? '50%' : '50%';
    el.style.backgroundImage = hasPhoto ? `url('${data.photoURL}')` : 'none';
    el.style.backgroundSize = hasPhoto ? 'cover' : '';
    el.style.backgroundPosition = hasPhoto ? 'center' : '';
    el.style.backgroundColor = hasPhoto ? '' : data.avatarColor;
    el.style.color = hasPhoto ? 'transparent' : AVATAR_TEXT_COLOR;
    el.textContent = hasPhoto ? '' : data.initial;
}

function getAccountRoleSet(profile = userProfile) {
    const roles = new Set(profile.accountRoles || []);
    // Backward compatibility for legacy `role` strings stored on the profile
    if (profile.role) roles.add(profile.role);
    return roles;
}

function normalizeUserProfileData(data = {}, uid = '') {
    const accountRoles = Array.isArray(data.accountRoles) ? data.accountRoles : (data.role ? [data.role] : []);
    const username = normalizeUsername(data.username || data.handle || data.nickname || (data.email ? data.email.split('@')[0] : ''));
    const name = resolveDisplayName(data) || '';
    const photoURL = data.photoURL || data.avatar || data.profilePhoto || data.profilePic || data.pfp || data.avatarUrl || '';
    const avatarColor = data.avatarColor || computeAvatarColor(username || uid || 'user');
    const locationHistory = Array.isArray(data.locationHistory) ? data.locationHistory : [];
    const profile = {
        ...data,
        uid: data.uid || data.id || uid || '',
        id: data.id || data.uid || uid || '',
        username,
        name: name || '',
        displayName: name || '',
        photoURL,
        avatarColor,
        accountRoles,
        verified: isUserVerified({ ...data, accountRoles }),
        locationHistory,
        photoPath: data.photoPath || '',
        savedPosts: Array.isArray(data.savedPosts) ? data.savedPosts : [],
        savedVideos: Array.isArray(data.savedVideos) ? data.savedVideos : []
    };
    return profile;
}

function storeUserInCache(uid, profile = {}) {
    if (!uid) return null;
    const normalized = normalizeUserProfileData(profile, uid);
    normalized._fetchedAt = Date.now();
    userCache[uid] = normalized;
    return normalized;
}

function isUserCacheStale(entry) {
    if (!entry) return true;
    if (!entry._fetchedAt) return true;
    return Date.now() - entry._fetchedAt > USER_CACHE_TTL_MS;
}

function getCachedUser(uid, { allowStale = true } = {}) {
    const entry = userCache[uid];
    if (!entry) return null;
    if (!allowStale && isUserCacheStale(entry)) return null;
    return entry;
}

function userHasRole(userLike = {}, role = '') {
    const roles = new Set(Array.isArray(userLike.accountRoles) ? userLike.accountRoles : []);
    if (userLike.role) roles.add(userLike.role);
    if (userLike.verified === true) roles.add('verified');
    return roles.has(role);
}

function isUserVerified(userLike = {}) {
    if (typeof userLike === 'boolean') return !!userLike;
    if (!userLike) return false;
    if (userLike.isVerified === true || userLike.verified === true) return true;
    if (typeof userLike.verificationStatus === 'string') {
        const status = userLike.verificationStatus.toLowerCase();
        if (status === 'approved' || status === 'verified') return true;
    }
    return userHasRole(userLike, 'verified');
}

function getVerifiedIconSvg() {
    return '<svg viewBox="0 0 256 256" fill="currentColor" aria-hidden="true"><path d="M232 128a104 104 0 1 1-104-104a104.11 104.11 0 0 1 104 104Zm-117.66 34.34l60-60a8 8 0 0 0-11.32-11.32L112 140.69l-19.31-19.31a8 8 0 0 0-11.32 11.32l24 24a8 8 0 0 0 11.32 0Z"></path></svg>';
}

function renderVerifiedBadge(userLike = {}, extraClass = '') {
    return isUserVerified(userLike) ? `<span class="verified-badge ${extraClass}">${getVerifiedIconSvg()}</span>` : '';
}

function hasGlobalRole(role) {
    return getAccountRoleSet().has(role);
}

function getMembershipRoles(categoryId) {
    const membership = memberships[categoryId] || {};
    const roleList = Array.isArray(membership.roles) ? membership.roles.slice() : [];
    if (membership.role && !roleList.includes(membership.role)) roleList.push(membership.role);
    return new Set(roleList);
}

function hasCommunityRole(categoryId, role) {
    return getMembershipRoles(categoryId).has(role);
}

async function refreshIdToken() {
    const u = auth.currentUser;
    if (!u) return null;
    await u.getIdToken(true);
    const res = await u.getIdTokenResult();
    updateAuthClaims(res?.claims || {});
    return res;
}

async function getClaims() {
    const u = auth.currentUser;
    if (!u) return {};
    const res = await u.getIdTokenResult();
    updateAuthClaims(res?.claims || {});
    return res?.claims || {};
}

function updateAuthClaims(claims = {}) {
    authClaims = claims || {};
}

function hasFounderClaimClient() {
    return authClaims.founder === true;
}

// Thread & View State
let activePostId = null;
let activeReplyId = null;
let threadUnsubscribe = null;
let viewingUserId = null;
let currentReviewId = null;
let conversationsUnsubscribe = null;
let messagesUnsubscribe = null;
let activeConversationId = null;
let conversationsCache = [];
let activeMessageUpload = null;
let conversationMappings = [];
let conversationDetailsCache = {};
let conversationSettingsId = null;
let conversationSettingsSearchResults = [];
let messageThreadCache = {};
let pendingMessageAttachments = [];
let messageUploadState = {
    status: 'idle',
    progress: 0,
    error: null,
    retries: 0,
    conversationId: null,
    messageId: null,
    files: []
};
let conversationDetailsUnsubscribe = null;
let lastDeliveredAtLocal = {};
let lastReadAtLocal = {};
let typingStateByConversation = {};
let activeReplyContext = null;
let editingMessageId = null;
let conversationSearchTerm = '';
let conversationSearchHits = [];
let conversationSearchIndex = 0;
let messageActionsMenuEl = null;
const REACTION_EMOJIS = ['👍', '❤️', '😂', '🎉', '😮', '😢'];
let newChatSelections = [];
let chatSearchResults = [];
let videosFeedLoaded = false;
let videosFeedLoading = false;
let videosCache = [];
let feedScrollObserver = null;
let videosScrollObserver = null;
const VIDEOS_BATCH_SIZE = 10;
const videosPagination = {
    lastDoc: null,
    loading: false,
    done: false
};
let liveSessionsUnsubscribe = null;
let activeLiveSessionId = null;
let activeCallSession = null;
let callDocUnsubscribe = null;
let callUiInitialized = false;
let livekitRoom = null;
let livekitLocalAudioTrack = null;
let livekitLocalVideoTrack = null;

function arrayShallowEqual(a = [], b = []) {
    if (!Array.isArray(a) || !Array.isArray(b)) return false;
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
        if (a[i] !== b[i]) return false;
    }
    return true;
}

const ListenerRegistry = (function () {
    const listeners = new Map();
    const loggedKeys = new Set();
    const authUnsubs = new Map();

    function devLog(message, key) {
        const shouldLog = window?.location?.hostname === 'localhost' || window?.__DEV_LISTENER_DEBUG__;
        if (shouldLog && !loggedKeys.has(`${key}-replace`)) {
            console.debug(message);
            loggedKeys.add(`${key}-replace`);
        }
    }

    return {
        register(key, unsubscribeFn) {
            if (!key || typeof unsubscribeFn !== 'function') return unsubscribeFn;

            if (listeners.has(key)) {
                const existing = listeners.get(key);
                try { existing(); } catch (e) { console.warn('Listener cleanup failed for', key, e); }
                devLog(`ListenerRegistry: replaced existing listener for key "${key}"`, key);
            }

            listeners.set(key, unsubscribeFn);
            authUnsubs.set(key, unsubscribeFn);
            return function deregister() {
                if (!listeners.has(key)) return;
                const current = listeners.get(key);
                listeners.delete(key);
                authUnsubs.delete(key);
                try { current(); } catch (e) { console.warn('Listener cleanup failed for', key, e); }
            };
        },
        unregister(key) {
            if (!listeners.has(key)) return;
            const unsub = listeners.get(key);
            listeners.delete(key);
            authUnsubs.delete(key);
            try { unsub(); } catch (e) { console.warn('Listener cleanup failed for', key, e); }
        },
        clearAll() {
            listeners.forEach(function (unsub, key) {
                try { unsub(); } catch (e) { console.warn('Listener cleanup failed for', key, e); }
            });
            listeners.clear();
            authUnsubs.clear();
        },
        has(key) {
            return listeners.has(key);
        },
        debugPrint() {
            console.log('Active listeners:', Array.from(listeners.keys()));
        }
    };
})();

window.ListenerRegistry = ListenerRegistry;
window.debugActiveListeners = function () { return ListenerRegistry.debugPrint(); };
window.addEventListener('beforeunload', function () { ListenerRegistry.clearAll(); });
window.Nexera = window.Nexera || {};
if (!window.Nexera.ready) {
    let readyResolve;
    window.Nexera.ready = new Promise(function (resolve) { readyResolve = resolve; });
    window.Nexera.__resolveReady = readyResolve;
}
if (!window.Nexera.authReady) {
    let authReadyResolve;
    window.Nexera.authReady = new Promise(function (resolve) { authReadyResolve = resolve; });
    window.Nexera.__resolveAuthReady = authReadyResolve;
    window.Nexera.__authReadyResolved = false;
}
window.Nexera.authResolved = false;
window.Nexera.auth = auth;
window.Nexera.db = db;
window.Nexera.storage = storage;
window.Nexera.firestore = { doc, getDoc };
window.Nexera.onAuthReady = function (callback) {
    return onAuthStateChanged(auth, callback);
};
window.Nexera.ensureVideoInCache = function (video) {
    if (!video || !video.id) return;
    if (videosCache.some(function (entry) { return entry.id === video.id; })) return;
    videosCache = [video].concat(videosCache);
};
window.Nexera.ensurePostInCache = function (post) {
    if (!post || !post.id) return;
    const normalized = normalizePostData(post.id, post);
    const existingIndex = allPosts.findIndex(function (entry) { return entry.id === post.id; });
    if (existingIndex >= 0) {
        allPosts[existingIndex] = { ...allPosts[existingIndex], ...normalized };
    } else {
        allPosts = [normalized].concat(allPosts);
    }
    if (post.userId && !userCache[post.userId]) {
        fetchMissingProfiles([{ userId: post.userId }]);
    }
};
window.Nexera.restoreFeedScroll = function (scrollY) {
    if (typeof scrollY !== 'number') return;
    pendingFeedScrollRestore = scrollY;
    const token = ++feedScrollRestoreToken;
    const started = performance.now();
    const attempt = function () {
        if (token !== feedScrollRestoreToken) return;
        const container = document.getElementById('feed-content');
        const ready = container && (container.children.length > 0 || !feedLoading);
        if (ready || performance.now() - started > 1500) {
            window.scrollTo(0, pendingFeedScrollRestore || 0);
            pendingFeedScrollRestore = null;
            return;
        }
        requestAnimationFrame(attempt);
    };
    requestAnimationFrame(attempt);
};
window.Nexera.navigateTo = function (routeObj) {
    if (!routeObj) return;
    if (routeObj.view) {
        window.navigateTo(routeObj.view, false);
    }
};
window.Nexera.openEntity = function (type, id, payload) {
    if (!type || !id) return;
    if (type === 'video') {
        if (payload) window.Nexera.ensureVideoInCache(payload);
        if (typeof window.openVideoDetail === 'function') window.openVideoDetail(id);
        return;
    }
    if (type === 'profile') {
        if (typeof window.openUserProfile === 'function') window.openUserProfile(id, null, false);
        return;
    }
    if (type === 'post') {
        if (typeof window.openThread === 'function') window.openThread(id);
    }
};
window.Nexera.splashHold = window.Nexera.splashHold !== false;
logSplashEvent('hold-init');
window.Nexera.releaseSplash = function (reason = 'release') {
    window.Nexera.splashHold = false;
    window.Nexera.splashPending = false;
    logSplashEvent('release', reason);
    hideSplash({ force: true, reason });
};
let staffRequestsUnsub = null;
let staffReportsUnsub = null;
let staffLogsUnsub = null;
let staffTrendingSyncBound = false;
let activeOptionsPost = null;
let threadComments = [];
let optimisticThreadComments = [];
let commentFilterMode = 'popularity';
let commentFilterQuery = '';

// --- Navigation Stack ---
let navStack = [];
let currentViewId = 'feed';
const MOBILE_VIEWPORT = typeof window !== 'undefined' && window.matchMedia ? window.matchMedia('(max-width: 820px)') : null;
let lastAuthUid = null;

function isMobileViewport() {
    return !!(MOBILE_VIEWPORT && MOBILE_VIEWPORT.matches);
}

function shouldShowRightSidebar(viewId) {
    return ['feed', 'videos', 'discover', 'saved', 'messages', 'profile', 'live'].includes(viewId);
}

window.shouldShowRightSidebar = shouldShowRightSidebar;

function getViewerUid() {
    return currentUser && currentUser.uid ? currentUser.uid : null;
}

const permissionDeniedLogCache = new Set();
function logPermissionDeniedOnce(scope) {
    if (permissionDeniedLogCache.has(scope)) return;
    permissionDeniedLogCache.add(scope);
    console.warn('Permission denied for', scope);
}

function isPermissionDeniedError(error) {
    return error?.code === 'permission-denied' || error?.code === 'storage/unauthorized';
}

async function guardFirebaseCall(actionKey, actionFn, options = {}) {
    try {
        const data = await actionFn();
        return { ok: true, data, error: null, permissionDenied: false };
    } catch (error) {
        const permissionDenied = isPermissionDeniedError(error);
        if (permissionDenied) {
            logPermissionDeniedOnce(actionKey);
            if (typeof options.onPermissionDenied === 'function') {
                options.onPermissionDenied(error);
            }
        } else {
            console.warn(`${actionKey} failed`, error?.message || error);
        }
        return { ok: false, data: null, error, permissionDenied };
    }
}

const missingProfileLogCache = new Set();
function logMissingProfileOnce(uid) {
    const key = `profiles:missing:${uid}`;
    if (missingProfileLogCache.has(key)) return;
    missingProfileLogCache.add(key);
    console.warn('Profile not found for', uid);
}

const dmMediaUrlCache = new Map();
const dmMediaUrlPromises = new Map();

function resolveDmMediaUrl(rawUrl = '') {
    if (!rawUrl) return { url: null, status: 'empty' };
    if (/^https?:\/\//i.test(rawUrl)) return { url: rawUrl, status: 'ok' };
    if (dmMediaUrlCache.has(rawUrl)) return dmMediaUrlCache.get(rawUrl);
    if (!dmMediaUrlPromises.has(rawUrl)) {
        const promise = guardFirebaseCall('storage:dm_media', function () {
            return getDownloadURL(ref(storage, rawUrl));
        }).then(function (result) {
            if (result.ok) {
                const entry = { url: result.data, status: 'ok' };
                dmMediaUrlCache.set(rawUrl, entry);
                return entry;
            }
            const status = result.permissionDenied ? 'denied' : 'error';
            const entry = { url: null, status };
            dmMediaUrlCache.set(rawUrl, entry);
            return entry;
        }).finally(function () {
            dmMediaUrlPromises.delete(rawUrl);
        });
        dmMediaUrlPromises.set(rawUrl, promise);
    }
    return { url: null, status: 'pending' };
}

function fetchDmMediaUrl(rawUrl = '') {
    if (!rawUrl) return Promise.resolve({ url: null, status: 'empty' });
    if (/^https?:\/\//i.test(rawUrl)) return Promise.resolve({ url: rawUrl, status: 'ok' });
    const cached = dmMediaUrlCache.get(rawUrl);
    if (cached) return Promise.resolve(cached);
    const pending = dmMediaUrlPromises.get(rawUrl);
    return pending || Promise.resolve({ url: null, status: 'pending' });
}

function getDmMediaFallbackText(status) {
    if (status === 'denied') return "You don’t have access to this attachment";
    return 'Attachment unavailable';
}

function buildPublicProfilePayload(uid, profile = {}) {
    return {
        uid,
        displayName: profile.displayName || profile.name || profile.nickname || '',
        username: profile.username || '',
        photoURL: profile.photoURL || '',
        bio: profile.bio || '',
        avatarColor: profile.avatarColor || computeAvatarColor(uid || profile.username || 'user'),
        verified: !!profile.verified || (Array.isArray(profile.accountRoles) && profile.accountRoles.includes('verified')),
        followersCount: profile.followersCount || profile.followerCount || 0,
        followingCount: profile.followingCount || 0,
        updatedAt: serverTimestamp()
    };
}

async function syncPublicProfile(uid, profile = {}, options = {}) {
    if (!uid) return;
    try {
        const viewerUid = getViewerUid();
        // Public profiles are written only for the signed-in user on first login/sign-up
        // or explicit settings saves to avoid permission-denied writes per Firestore rules.
        if (viewerUid && uid !== viewerUid) return { ok: false, permissionDenied: false };
        if (auth.currentUser) {
            await auth.currentUser.getIdToken();
        }
        return await guardFirebaseCall('profiles:write', function () {
            return setDoc(doc(db, 'profiles', uid), buildPublicProfilePayload(uid, profile), { merge: true });
        }, { onPermissionDenied: options.onPermissionDenied });
    } catch (e) {
        console.warn('Public profile sync failed', uid, e?.message || e);
        return { ok: false, permissionDenied: isPermissionDeniedError(e), error: e };
    }
}


const SIDEBAR_COLLAPSED_KEY = 'nexera_sidebar_collapsed';
const FEED_TYPE_STORAGE_KEY = 'nexera_feed_types';
const FEED_TYPE_TOGGLES = [
    { key: 'threads', label: 'Threads' },
    { key: 'videos', label: 'Videos' },
    { key: 'livestreams', label: 'Livestreams' }
];

let sidebarCollapsed = false;

function getFeedTypeToggleSlot() {
    return document.getElementById('feed-type-toggle-bar') || document.querySelector('.feed-type-toggle-bar');
}

function buildFeedTypeToggleButtons(container) {
    if (!container) return;
    container.innerHTML = '';
    FEED_TYPE_TOGGLES.forEach(function (toggle) {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'feed-type-toggle';
        btn.dataset.type = toggle.key;
        btn.innerHTML = `
            <span class="feed-type-toggle-text">${toggle.label}</span>
            <span class="feed-type-toggle-switch" aria-hidden="true"></span>
        `;
        btn.addEventListener('click', function (event) {
            event.preventDefault();
            toggleFeedType(toggle.key);
        });
        container.appendChild(btn);
    });
}

function getDefaultFeedTypes() {
    return { threads: true, videos: true, livestreams: true };
}

function loadFeedTypeState() {
    const defaults = getDefaultFeedTypes();
    let stored = null;
    if (window.localStorage) {
        try {
            stored = JSON.parse(window.localStorage.getItem(FEED_TYPE_STORAGE_KEY) || '');
        } catch (e) {
            stored = null;
        }
    }
    const next = { ...defaults, ...(stored && stored.types ? stored.types : stored) };
    const anySelected = Object.values(next).some(Boolean);
    if (!anySelected) {
        Object.assign(next, defaults);
    }
    window.NexeraFeedState = window.NexeraFeedState || {};
    window.NexeraFeedState.types = next;
    persistFeedTypeState();
}

function persistFeedTypeState() {
    if (!window.localStorage) return;
    const payload = { types: { ...(window.NexeraFeedState?.types || getDefaultFeedTypes()) } };
    window.localStorage.setItem(FEED_TYPE_STORAGE_KEY, JSON.stringify(payload));
}

function getActiveFeedTypes() {
    const types = window.NexeraFeedState?.types || getDefaultFeedTypes();
    return Object.keys(types).filter(function (key) { return !!types[key]; });
}

function applyFeedTypeFilterAndRefresh({ preserveScroll = false } = {}) {
    if (currentViewId !== 'feed') return;
    const scrollY = preserveScroll ? window.scrollY : null;
    uiDebugLog('feed toggles', { active: getActiveFeedTypes() });
    loadHomeMediaData().finally(function () {
        renderFeed();
        if (preserveScroll && typeof scrollY === 'number') {
            if (window.Nexera?.restoreFeedScroll) {
                window.Nexera.restoreFeedScroll(scrollY);
            } else {
                window.scrollTo(0, scrollY);
            }
        }
    });
}

function toggleFeedType(typeKey) {
    if (!typeKey) return;
    window.NexeraFeedState = window.NexeraFeedState || {};
    const types = window.NexeraFeedState.types || getDefaultFeedTypes();
    types[typeKey] = !types[typeKey];
    if (!Object.values(types).some(Boolean)) {
        Object.assign(types, getDefaultFeedTypes());
    }
    window.NexeraFeedState.types = types;
    persistFeedTypeState();
    syncFeedTypeToggleState();
    if (window?.location?.hostname === 'localhost' || window?.__DEV_FEED_TOGGLE_DEBUG__) {
        console.debug('[FeedTypes] Active:', getActiveFeedTypes());
    }
    applyFeedTypeFilterAndRefresh({ preserveScroll: true });
}

function mountFeedTypeToggleBar() {
    const slot = getFeedTypeToggleSlot();
    if (!slot) return;
    const header = document.querySelector('.right-sidebar-header');
    if (header && slot.parentElement !== header) {
        header.appendChild(slot);
    }
    if (!slot.children.length) {
        buildFeedTypeToggleButtons(slot);
    }
    syncFeedTypeToggleState();
}

function syncFeedTypeToggleState() {
    const activeTypes = getActiveFeedTypes();
    document.querySelectorAll('.feed-type-toggle-bar [data-type]').forEach(function (btn) {
        btn.classList.toggle('active', activeTypes.includes(btn.dataset.type));
    });
}

function applyDesktopSidebarState(collapsed, persist = true) {
    sidebarCollapsed = !!collapsed;
    if (!isMobileViewport()) {
        document.body.classList.toggle('sidebar-collapsed', sidebarCollapsed);
    }
    document.querySelectorAll('.sidebar-left').forEach(function (sidebar) {
        sidebar.classList.toggle('collapsed', sidebarCollapsed);
    });
    if (persist && window.localStorage) {
        window.localStorage.setItem(SIDEBAR_COLLAPSED_KEY, sidebarCollapsed ? '1' : '0');
    }
}

function setSidebarOverlayOpen(open) {
    document.body.classList.toggle('sidebar-overlay-open', !!open);
}

function initSidebarState() {
    if (window.localStorage) {
        sidebarCollapsed = window.localStorage.getItem(SIDEBAR_COLLAPSED_KEY) === '1';
    }
    applyDesktopSidebarState(sidebarCollapsed, false);
}

function openSidebar() {
    if (isMobileViewport()) {
        setSidebarOverlayOpen(true);
        return;
    }
    applyDesktopSidebarState(false);
}

function closeSidebar() {
    if (isMobileViewport()) {
        setSidebarOverlayOpen(false);
        return;
    }
    applyDesktopSidebarState(true);
}

function toggleSidebar() {
    if (isMobileViewport()) {
        setSidebarOverlayOpen(!document.body.classList.contains('sidebar-overlay-open'));
        return;
    }
    applyDesktopSidebarState(!sidebarCollapsed);
}

function bindSidebarEvents() {
    document.querySelectorAll('.sidebar-left .nav-item, .sidebar-left .create-btn-sidebar').forEach(function (item) {
        item.addEventListener('click', function () {
            if (isMobileViewport()) {
                setSidebarOverlayOpen(false);
            }
        });
    });

    document.addEventListener('keydown', function (event) {
        if (event.key === 'Escape' && document.body.classList.contains('sidebar-overlay-open')) {
            setSidebarOverlayOpen(false);
        }
    });

    if (MOBILE_VIEWPORT && MOBILE_VIEWPORT.addEventListener) {
        MOBILE_VIEWPORT.addEventListener('change', function () {
            if (isMobileViewport()) {
                document.body.classList.remove('sidebar-collapsed');
            } else {
                setSidebarOverlayOpen(false);
                applyDesktopSidebarState(sidebarCollapsed);
            }
        });
    }
}

window.Nexera.toggleSidebar = toggleSidebar;
window.Nexera.openSidebar = openSidebar;
window.Nexera.closeSidebar = closeSidebar;
window.Nexera.mountFeedTypeToggleBar = mountFeedTypeToggleBar;
// --- Mock Data ---
const MOCK_LIVESTREAMS = [
    { id: 'l1', title: '🔴 Mars Rover Landing Watch Party', viewerCount: '12.5k', author: 'SpaceX_Fan', category: 'STEM', color: '#00f2ea' },
    { id: 'l2', title: '🎮 Elden Ring Speedrun (No Hit)', viewerCount: '45.2k', author: 'SpeedSouls', category: 'Gaming', color: '#7000ff' },
    { id: 'l3', title: '⚽ Premier League Match Reaction', viewerCount: '8.1k', author: 'FootyDaily', category: 'Sports', color: '#ff4d00' },
    { id: 'l4', title: '🎻 Lo-Fi Beats & Coding Session', viewerCount: '2.3k', author: 'ChillHop', category: 'Music', color: '#ff0050' },
    { id: 'l5', title: '🤡 Reacting to Cringe TikToks', viewerCount: '105k', author: 'Roaster', category: 'Brainrot', color: '#00ff41' }
];

const HISTORICAL_EVENTS = {
    "1-1": "1983: The ARPANET officially changes to using the Internet Protocol (IP), creating the Internet.",
    "1-9": "2007: Steve Jobs introduces the original iPhone at Macworld San Francisco.",
    "1-24": "1984: Apple Computer Inc. unveils the Macintosh personal computer.",
    "1-28": "1986: The Space Shuttle Challenger disaster occurs.",
    "2-4": "2004: Facebook (originally TheFacebook) is launched by Mark Zuckerberg.",
    "2-14": "1946: ENIAC, the first general-purpose electronic computer, is unveiled.",
    "2-15": "1564: Galileo Galilei, the father of modern science, is born.",
    "2-28": "1953: Watson and Crick discover the chemical structure of DNA.",
    "3-10": "1876: Alexander Graham Bell makes the first successful telephone call.",
    "3-14": "1879: Albert Einstein is born (Pi Day).",
    "3-18": "1965: Cosmonaut Alexei Leonov performs the first spacewalk.",
    "4-3": "1973: Martin Cooper makes the first handheld mobile phone call.",
    "4-12": "1961: Yuri Gagarin becomes the first human to travel into space.",
    "4-24": "1990: The Hubble Space Telescope is launched aboard Space Shuttle Discovery.",
    "4-30": "1993: CERN releases the World Wide Web source code into the public domain.",
    "5-20": "1927: Charles Lindbergh begins the first solo non-stop transatlantic flight.",
    "5-25": "1977: The first Star Wars movie (Episode IV: A New Hope) is released.",
    "5-29": "1919: A solar eclipse confirms Einstein's theory of general relativity.",
    "6-10": "2003: The Spirit Rover is launched, beginning NASA's Mars Exploration Rover mission.",
    "6-23": "1912: Alan Turing, the father of theoretical computer science, is born.",
    "6-29": "2007: The first iPhone goes on sale.",
    "7-5": "1994: Jeff Bezos founds Amazon in Bellevue, Washington.",
    "7-16": "1969: Apollo 11 launches from Cape Kennedy carrying Neil Armstrong, Buzz Aldrin, and Michael Collins.",
    "7-20": "1969: Neil Armstrong and Buzz Aldrin become the first humans to land on the Moon.",
    "8-6": "1991: Tim Berners-Lee publishes the first-ever website.",
    "8-12": "1981: The IBM Personal Computer is released.",
    "8-24": "2006: Pluto is reclassified as a dwarf planet by the IAU.",
    "9-4": "1998: Google is founded by Larry Page and Sergey Brin.",
    "9-13": "1985: Super Mario Bros. is released in Japan for the NES.",
    "9-28": "1928: Alexander Fleming discovers penicillin.",
    "10-4": "1957: Sputnik 1, the first artificial satellite, is launched by the Soviet Union.",
    "10-5": "2011: Steve Jobs, co-founder of Apple Inc., passes away.",
    "10-29": "1969: The first message is sent over ARPANET, the precursor to the modern Internet.",
    "11-9": "1989: The Berlin Wall falls, marking the end of the Cold War era.",
    "11-20": "1998: The first module of the International Space Station (Zarya) is launched.",
    "11-24": "1859: Charles Darwin publishes 'On the Origin of Species'.",
    "12-9": "1965: 'A Charlie Brown Christmas' premieres on CBS.",
    "12-10": "1815: Ada Lovelace, considered the first computer programmer, is born.",
    "12-17": "1903: The Wright brothers make the first powered flight.",
    "12-25": "2021: The James Webb Space Telescope is launched.",
    "DEFAULT": "Did you know? On this day in history, something amazing likely happened!"
};

const timeCapsuleState = {
    event: null,
    source: 'fallback',
    dateKey: null,
    dateLabel: '',
    loading: false
};

const TRENDING_RANGE_WINDOWS = {
    day: { label: '1d', ms: 24 * 60 * 60 * 1000 },
    week: { label: '7d', ms: 7 * 24 * 60 * 60 * 1000 },
    month: { label: '1mo', ms: 30 * 24 * 60 * 60 * 1000 },
    six_months: { label: '6mo', ms: 182 * 24 * 60 * 60 * 1000 },
    year: { label: '1y', ms: 365 * 24 * 60 * 60 * 1000 },
    five_years: { label: '5y', ms: 5 * 365 * 24 * 60 * 60 * 1000 },
    lifetime: { label: 'All', ms: null }
};

const TRENDING_RANGE_STORAGE_KEY = 'nexera_trending_timeframe';
const TRENDING_DEFAULT_RANGE = 'six_months';
const TRENDING_PAGE_SIZE = 3;
const trendingTopicsState = {
    range: TRENDING_DEFAULT_RANGE,
    loading: false,
    lastLoaded: 0,
    items: [],
    lastDoc: null,
    hasMore: false,
    lastLoadSucceeded: false,
    needsRefresh: false
};

const THEMES = {
    'For You': '#00f2ea', 'Following': '#ffffff', 'STEM': '#00f2ea',
    'History': '#ffd700', 'Coding': '#00ff41', 'Art': '#ff0050',
    'Random': '#bd00ff', 'Brainrot': '#ff00ff', 'Sports': '#ff4500',
    'Gaming': '#7000ff', 'News': '#ff3d3d', 'Music': '#00bfff'
};

const DEFAULT_CATEGORY_RULES = [
    'Stay on-topic; explain context for beginners.',
    'No misinformation; if making factual claims, include a source link when possible.',
    'No low-effort engagement bait (ragebait, spam, “bro just trust me”).',
    'Respectful critique only; attack ideas, not people.',
    'No unsolicited self-promo; keep it relevant and add value.',
    'Mark speculation vs fact clearly.',
    'Keep titles descriptive; avoid clickbait.'
];

const OFFICIAL_CATEGORIES = [
    'STEM Lab',
    'Code & Coffee',
    'History’s Greatest Hits',
    'Mythbusters Academy',
    'Money Moves (Personal Finance)',
    'Language Lounge',
    'Space Desk',
    'Design & Media',
    'Life Skills 101',
    'Health & Fitness (No Bro-Science)'
];

// Shared state + render helpers
window.getCurrentUser = function () { return currentUser; };
window.getUserDoc = async function (uid) { return getDoc(doc(db, 'users', uid)); };
window.requireAuth = function () {
    if (!currentUser) {
        document.getElementById('auth-screen').style.display = 'flex';
        document.getElementById('app-layout').style.display = 'none';
        return false;
    }
    return true;
};
window.refreshIdToken = refreshIdToken;
window.getClaims = getClaims;
window.setView = function (name) { return window.navigateTo(name); };
window.toast = function (msg, type = 'info') {
    console.log(`[${type}]`, msg);
    const overlay = document.createElement('div');
    overlay.textContent = msg;
    overlay.className = 'toast-msg';
    document.body.appendChild(overlay);
    setTimeout(() => overlay.remove(), 2500);
};

const confirmModalState = { onConfirm: null, getData: null, previousFocus: null, keyHandler: null, busy: false };

function closeConfirmModal() {
    const modal = document.getElementById('confirm-modal');
    if (!modal) return;
    modal.style.display = 'none';
    confirmModalState.busy = false;
    const submitBtn = document.getElementById('confirm-submit-btn');
    const label = document.getElementById('confirm-submit-label');
    if (submitBtn) submitBtn.disabled = false;
    if (label) label.textContent = label?.dataset?.defaultText || label?.textContent || 'Confirm';
    if (confirmModalState.keyHandler) document.removeEventListener('keydown', confirmModalState.keyHandler, true);
    if (confirmModalState.previousFocus) confirmModalState.previousFocus.focus();
    confirmModalState.onConfirm = null;
    confirmModalState.getData = null;
    confirmModalState.previousFocus = null;
    confirmModalState.keyHandler = null;
}

window.cancelConfirmModal = closeConfirmModal;

function trapConfirmFocus(modal) {
    const focusable = modal.querySelectorAll('button, [href], input, select, textarea');
    if (!focusable.length) return;
    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    confirmModalState.keyHandler = function (e) {
        if (e.key === 'Escape') { e.preventDefault(); closeConfirmModal(); return; }
        if (e.key !== 'Tab') return;
        if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
        else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
    };
    document.addEventListener('keydown', confirmModalState.keyHandler, true);
    first.focus();
}

async function openConfirmModal(options = {}) {
    const modal = document.getElementById('confirm-modal');
    const body = document.getElementById('confirm-body');
    const helper = document.getElementById('confirm-helper');
    const extra = document.getElementById('confirm-extra');
    const submitBtn = document.getElementById('confirm-submit-btn');
    const submitLabel = document.getElementById('confirm-submit-label');
    const cancelBtn = document.getElementById('confirm-cancel-btn');
    const title = document.getElementById('confirm-title');
    if (!modal || !body || !helper || !extra || !submitBtn || !submitLabel || !cancelBtn) return false;

    modal.style.display = 'flex';
    if (title) title.textContent = options.title || 'Confirm';
    body.textContent = options.message || '';
    helper.textContent = options.helperText || '';
    extra.innerHTML = '';
    submitLabel.dataset.defaultText = options.confirmText || 'Confirm';
    submitLabel.textContent = options.confirmText || 'Confirm';
    const cancelIcon = options.cancelIcon || '<i class="ph ph-x"></i>';
    cancelBtn.innerHTML = `${cancelIcon} ${options.cancelText || 'Cancel'}`;

    confirmModalState.previousFocus = document.activeElement;
    confirmModalState.onConfirm = options.onConfirm || null;
    confirmModalState.getData = typeof options.buildContent === 'function' ? options.buildContent(extra) || function () { return {}; } : function () { return {}; };

    submitBtn.onclick = async function () {
        if (confirmModalState.busy) return;
        confirmModalState.busy = true;
        submitBtn.disabled = true;
        submitLabel.innerHTML = `<span class="button-spinner"></span> ${options.confirmText || 'Confirm'}`;
        try {
            if (confirmModalState.onConfirm) {
                await confirmModalState.onConfirm(confirmModalState.getData());
            }
            closeConfirmModal();
        } catch (e) {
            console.error(e);
            toast(options.errorText || 'Action failed', 'error');
        } finally {
            confirmModalState.busy = false;
            submitBtn.disabled = false;
            submitLabel.textContent = options.confirmText || 'Confirm';
        }
    };

    cancelBtn.onclick = function () { closeConfirmModal(); };
    trapConfirmFocus(modal);
    return true;
}

function slugifyCategory(name) {
    return name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').substring(0, 80);
}

async function ensureOfficialCategories() {
    if (!hasFounderClaimClient()) return;
    const promises = OFFICIAL_CATEGORIES.map(async function (name) {
        const slug = slugifyCategory(name);
        const catRef = doc(db, 'categories', slug);
        const snap = await getDoc(catRef);
        const payload = {
            name,
            slug,
            type: 'official',
            verified: true,
            description: `${name} official category`,
            rules: DEFAULT_CATEGORY_RULES,
            createdBy: null,
            ownerId: null,
            createdAt: snap.exists() ? snap.data().createdAt || serverTimestamp() : serverTimestamp(),
            isPublic: true,
            memberCount: snap.exists() && snap.data().memberCount ? snap.data().memberCount : 0,
            mods: []
        };
        await setDoc(catRef, payload, { merge: true });
    });

    await Promise.all(promises);
}

// --- Initialization & Auth Listener ---
function initApp(onReady) {
    let readyResolver;
    const readyPromise = new Promise(function (resolve) { readyResolver = resolve; });
    const markReady = function () {
        if (markReady.done) return;
        markReady.done = true;
        if (typeof onReady === 'function') onReady();
        if (window.Nexera?.__resolveReady) window.Nexera.__resolveReady();
        if (readyResolver) readyResolver();
        if (window.Nexera?.releaseSplash) {
            window.Nexera.releaseSplash('auth-ready');
        }
    };

    onAuthStateChanged(auth, async function (user) {
        const loadingOverlay = document.getElementById('loading-overlay');
        const authScreen = document.getElementById('auth-screen');
        const appLayout = document.getElementById('app-layout');
        window.Nexera.authResolved = true;
        if (user?.uid !== lastAuthUid) {
            userCache = {};
            userFetchPromises = {};
            lastAuthUid = user?.uid || null;
        }
        if (!window.Nexera.__authReadyResolved && window.Nexera.__resolveAuthReady) {
            window.Nexera.__authReadyResolved = true;
            window.Nexera.__resolveAuthReady();
        }
        uiDebugLog('auth resolved', { signedIn: !!user });

        try {
            if (user) {
                currentUser = user;
                console.log("User logged in:", user.uid);

                try {
                    const claimResult = await getClaims();
                    updateAuthClaims(claimResult);
                    const ensuredSnap = await ensureUserDocument(user);
                    const docSnap = ensuredSnap;

                    // Fetch User Profile
                    if (docSnap.exists()) {
                        userProfile = { ...userProfile, ...normalizeUserProfileData(docSnap.data(), docSnap.id) };
                        storeUserInCache(user.uid, userProfile);

                        // Normalize role storage
                        userProfile.accountRoles = Array.isArray(userProfile.accountRoles) ? userProfile.accountRoles : [];
                        recentLocations = Array.isArray(userProfile.locationHistory) ? userProfile.locationHistory.slice() : [];

                        await backfillAvatarColorIfMissing(user.uid, userProfile);
                        await syncPublicProfile(user.uid, userProfile);

                        // Apply stored theme preference
                        const savedTheme = userProfile.theme || nexeraGetStoredThemePreference() || 'system';
                        userProfile.theme = savedTheme;
                        applyTheme(savedTheme);

                        syncSavedVideosFromProfile(userProfile);
                        await hydrateFollowedCategories(user.uid, userProfile);
                        await hydrateFollowingState(user.uid, userProfile);
                        const staffNav = document.getElementById('nav-staff');
                        if (staffNav) staffNav.style.display = (hasGlobalRole('staff') || hasGlobalRole('admin') || hasFounderClaimClient()) ? 'flex' : 'none';
                    } else {
                        // Create new profile placeholder if it doesn't exist
                        userProfile.email = user.email || "";
                        userProfile.name = user.displayName || "Nexera User";
                        const storedTheme = nexeraGetStoredThemePreference() || userProfile.theme || 'system';
                        userProfile.theme = storedTheme;
                        userProfile.avatarColor = userProfile.avatarColor || computeAvatarColor(user.uid || user.email || 'user');
                        userProfile.locationHistory = [];
                        recentLocations = [];
                        applyTheme(storedTheme);
                        syncSavedVideosFromProfile(userProfile);
                        await syncPublicProfile(user.uid, userProfile);
                        const staffNav = document.getElementById('nav-staff');
                        if (staffNav) staffNav.style.display = 'none';
                    }
                } catch (e) {
                    console.error("Profile Load Error", e);
                }

                // UI Transitions
                if (authScreen) authScreen.style.display = 'none';
                if (appLayout) appLayout.style.display = 'flex';
                if (loadingOverlay) loadingOverlay.style.display = 'none';

                // Start Logic
                await ensureOfficialCategories();
                startCategoryStreams(user.uid);
                await loadFeedData({ showSplashDuringLoad: true });
                startUserReviewListener(user.uid); // PATCH: Listen for USER reviews globally on load
                loadInboxModeFromStorage();
                const storedInboxMode = inboxMode || 'content';
                setInboxMode(storedInboxMode, { skipRouteUpdate: true });
                initContentNotifications(user.uid);
                initConversations(storedInboxMode === 'messages');
                if (LIVEKIT_ENABLED) {
                    initCallUi();
                }
                if ('Notification' in window && Notification.permission === 'granted') {
                    registerMessagingServiceWorker();
                    syncStoredPushToken(user.uid);
                }
                updateTimeCapsule();
                const path = window.location.pathname || '/';
                if (path === '/' || path === '/home') {
                    window.navigateTo('feed', false);
                }
                renderProfile(); // Pre-render profile
                if (!uploadManager) {
                    uploadManager = createUploadManager({ storage, onStateChange: setUploadTasks });
                }
                ensureVideoTaskViewerBindings();
                uploadManager.restorePendingUploads(currentUser.uid);
            } else {
                const previousUserId = currentUser?.uid;
                currentUser = null;
                updateAuthClaims({});
                ListenerRegistry.clearAll(); // Cleanup listeners on logout to prevent permission errors.
                if (previousUserId) removePushTokenForUser(previousUserId);
                if (followedTopicsUnsubscribe) {
                    try { followedTopicsUnsubscribe(); } catch (err) { }
                    followedTopicsUnsubscribe = null;
                }
                if (followingUnsubscribe) {
                    try { followingUnsubscribe(); } catch (err) { }
                    followingUnsubscribe = null;
                }
                if (inboxNotificationsUnsubscribe) {
                    try { inboxNotificationsUnsubscribe(); } catch (err) { }
                    inboxNotificationsUnsubscribe = null;
                }
                if (contentNotificationsUnsubscribe) {
                    try { contentNotificationsUnsubscribe(); } catch (err) { }
                    contentNotificationsUnsubscribe = null;
                }
                inboxNotifications = [];
                contentNotifications = [];
                inboxNotificationCounts = { posts: 0, videos: 0, livestreams: 0, account: 0 };
                updateInboxNavBadge();
                followedCategories = new Set();
                followedCategoryList = [];
                userProfile.followedCategories = [];
                followedUsers = new Set();
                userProfile.following = [];
                videoEngagementState.liked.clear();
                videoEngagementState.disliked.clear();
                videoEngagementState.saved.clear();
                videoEngagementHydrated = new Set();
                if (loadingOverlay) loadingOverlay.style.display = 'none';
                if (appLayout) appLayout.style.display = 'none';
                if (authScreen) authScreen.style.display = 'flex';
                setUploadTasks([]);
                closeVideoTaskViewer();
            }
        } catch (err) {
            console.error('Initialization error', err);
        } finally {
            markReady();
        }
    });

    return readyPromise;
}

async function initializeNexeraApp() {
    showSplash();
    try {
        refreshBrandLogos();
        await initApp(hideSplash);
    } catch (err) {
        console.error('Failed to initialize Nexera', err);
        hideSplash();
    }
}

async function fetchWikipediaEventsForToday() {
    try {
        const now = new Date();
        const month = String(now.getMonth() + 1).padStart(2, '0');
        const day = String(now.getDate()).padStart(2, '0');
        const url = `https://en.wikipedia.org/api/rest_v1/feed/onthisday/events/${month}/${day}`;

        const response = await fetch(url);
        if (!response.ok) throw new Error('Failed to load Wikipedia events');
        const data = await response.json();
        const events = Array.isArray(data.events) ? data.events : [];
        if (!events.length) return null;

        const event = events[Math.floor(Math.random() * events.length)];
        if (!event || !event.text) return null;

        const firstPage = Array.isArray(event.pages) && event.pages.length ? event.pages[0] : null;
        let wikiUrl = firstPage && firstPage.content_urls && firstPage.content_urls.desktop && firstPage.content_urls.desktop.page;
        if (!wikiUrl && firstPage && firstPage.titles && firstPage.titles.normalized) {
            const normalized = encodeURIComponent(firstPage.titles.normalized.replace(/ /g, '_'));
            wikiUrl = `https://en.wikipedia.org/wiki/${normalized}`;
        }

        return {
            year: event.year,
            text: event.text,
            url: wikiUrl || null
        };
    } catch (err) {
        console.warn('Wikipedia events fetch failed', err);
        return null;
    }
}

function getFallbackEventForDate(key) {
    const eventText = HISTORICAL_EVENTS[key] || HISTORICAL_EVENTS["DEFAULT"];
    return eventText ? { year: '', text: eventText, url: null } : null;
}

function renderTimeCapsule() {
    const dateEl = document.getElementById('otd-date-display');
    const eventEl = document.getElementById('otd-event-display');
    const wikiLink = document.getElementById('otd-wiki-link');
    const refreshBtn = document.getElementById('otd-refresh-btn');

    const currentEvent = timeCapsuleState.event;
    const displayText = currentEvent ? `${currentEvent.year ? currentEvent.year + ' – ' : ''}${currentEvent.text}` : HISTORICAL_EVENTS["DEFAULT"];

    if (dateEl) dateEl.textContent = timeCapsuleState.dateLabel || '';
    if (eventEl) eventEl.textContent = displayText;

    if (refreshBtn) {
        refreshBtn.disabled = !!timeCapsuleState.loading;
        refreshBtn.classList.toggle('disabled', refreshBtn.disabled);
    }

    if (wikiLink) {
        if (timeCapsuleState.source === 'wikipedia' && currentEvent && currentEvent.url) {
            wikiLink.style.display = 'inline-flex';
            wikiLink.href = currentEvent.url;
            wikiLink.setAttribute('target', '_blank');
            wikiLink.setAttribute('rel', 'noopener noreferrer');
        } else {
            wikiLink.style.display = 'none';
            wikiLink.removeAttribute('href');
        }
    }
}

async function updateTimeCapsule(forceReload = false) {
    const date = new Date();
    const key = `${date.getMonth() + 1}-${date.getDate()}`;

    if (!forceReload && timeCapsuleState.dateKey === key && timeCapsuleState.event) {
        renderTimeCapsule();
        return;
    }

    timeCapsuleState.dateKey = key;
    timeCapsuleState.dateLabel = date.toLocaleDateString('en-US', { month: 'long', day: 'numeric' });
    timeCapsuleState.loading = true;
    renderTimeCapsule();

    const wikiEvent = await fetchWikipediaEventsForToday();
    if (wikiEvent) {
        timeCapsuleState.event = wikiEvent;
        timeCapsuleState.source = 'wikipedia';
    } else {
        timeCapsuleState.event = getFallbackEventForDate(key);
        timeCapsuleState.source = 'fallback';
    }

    timeCapsuleState.loading = false;
    renderTimeCapsule();
    uiDebugLog('time capsule loaded', { source: timeCapsuleState.source, hasUrl: !!timeCapsuleState.event?.url });
}

function showAnotherTimeCapsuleEvent() {
    updateTimeCapsule(true);
}

window.showAnotherTimeCapsuleEvent = showAnotherTimeCapsuleEvent;

function normalizeTrendingTopic(entry) {
    if (!entry) return null;
    const name = (entry.name || entry.topic || entry.topicName || entry.label || '').toString().trim();
    if (!name) return null;
    if (name.toLowerCase() === 'general') return null;
    const interactionsValue = entry.interactions;
    const interactionsCount = typeof interactionsValue === 'number'
        ? interactionsValue
        : Number(interactionsValue?.total || interactionsValue?.count || 0);
    const count = Number(entry.count || entry.total || entry.posts || entry.value || interactionsCount || 0);
    return { topicId: entry.topicId || entry.id || slugifyCategory(name), name, count };
}

function formatSlugLabel(slug = '') {
    return slug.replace(/[-_]+/g, ' ').replace(/\b\w/g, function (m) { return m.toUpperCase(); });
}

function resolveCategoryNameBySlug(slug) {
    if (!slug) return 'Unknown';
    const match = categories.find(function (cat) { return cat.slug === slug || cat.id === slug; });
    return match?.name || formatSlugLabel(slug);
}

// Trending topics now come from the staff-populated "trendingCategories" collection.
async function fetchTrendingTopicsPage(range, startAfterDoc = null) {
    const items = [];
    let lastDoc = startAfterDoc || null;
    let hasMore = true;
    let iterations = 0;

    while (items.length < TRENDING_PAGE_SIZE && hasMore && iterations < 4) {
        const constraints = [orderBy('popularity', 'desc'), limit(TRENDING_PAGE_SIZE + 1)];
        if (lastDoc) constraints.splice(1, 0, startAfter(lastDoc));
        const snapshot = await getDocs(query(collection(db, 'trendingCategories'), ...constraints));
        const docs = snapshot.docs;
        if (!docs.length) {
            hasMore = false;
            break;
        }
        hasMore = docs.length > TRENDING_PAGE_SIZE;
        const pageDocs = hasMore ? docs.slice(0, TRENDING_PAGE_SIZE) : docs;
        lastDoc = docs[docs.length - 1] || lastDoc;
        iterations += 1;

        for (const docSnap of pageDocs) {
            const data = docSnap.data() || {};
            const count = Number(data.popularity || 0) || 0;
            if (count <= 0) continue;
            items.push({
                topicId: data.slug || docSnap.id,
                categorySlug: data.slug || docSnap.id,
                name: resolveCategoryNameBySlug(data.slug || docSnap.id),
                count
            });
            if (items.length >= TRENDING_PAGE_SIZE) break;
        }
    }

    return {
        items,
        lastDoc,
        hasMore
    };
}

function renderTrendingTopics(topics) {
    const list = document.getElementById('trending-topic-list');
    if (!list) return;
    const box = list.closest('.trend-box');
    let loadMoreWrap = box ? box.querySelector('.trend-load-more-wrapper') : null;
    if (box && !loadMoreWrap) {
        loadMoreWrap = document.createElement('div');
        loadMoreWrap.className = 'trend-load-more-wrapper';
        box.appendChild(loadMoreWrap);
    }
    list.innerHTML = '';
    if (!topics.length) {
        list.innerHTML = `<div class="trend-item"><span>No trending topics yet.</span><span></span></div>`;
        if (loadMoreWrap) loadMoreWrap.innerHTML = '';
        return;
    }
    list.classList.toggle('is-scrollable', topics.length > TRENDING_PAGE_SIZE);
    topics.forEach(function (topic) {
        const item = document.createElement('div');
        item.className = 'trend-item';
        item.innerHTML = `<span>${escapeHtml(topic.name)}</span><span style=\"color:var(--text-muted);\">${formatCompactNumber(topic.count || 0)}</span>`;
        item.addEventListener('click', function () {
            const slug = topic.categorySlug || topic.topicId || '';
            const name = resolveCategoryNameBySlug(slug);
            if (typeof window.setCategory === 'function') window.setCategory(name);
            moveTopicPillAfterAnchors(name);
        });
        list.appendChild(item);
    });

    if (loadMoreWrap) {
        loadMoreWrap.innerHTML = '';
    }

    if (trendingTopicsState.hasMore && loadMoreWrap) {
        const loadMore = document.createElement('button');
        loadMore.type = 'button';
        loadMore.className = 'trend-load-more';
        loadMore.textContent = 'Load More';
        loadMore.addEventListener('click', function () {
            loadMoreTrendingTopics();
        });
        loadMoreWrap.appendChild(loadMore);
    }
}

async function loadTrendingTopics(range = trendingTopicsState.range) {
    const list = document.getElementById('trending-topic-list');
    if (!list) return;
    const box = list.closest('.trend-box');
    const loadMoreWrap = box ? box.querySelector('.trend-load-more-wrapper') : null;
    trendingTopicsState.range = range || trendingTopicsState.range;
    if (trendingTopicsState.loading) {
        trendingTopicsState.needsRefresh = true;
        return;
    }
    trendingTopicsState.loading = true;
    trendingTopicsState.lastLoadSucceeded = false;
    trendingTopicsState.items = [];
    trendingTopicsState.lastDoc = null;
    trendingTopicsState.hasMore = false;
    list.classList.remove('is-scrollable');
    list.innerHTML = `<div class="trend-item"><span>Loading topics...</span><span></span></div>`;
    if (loadMoreWrap) loadMoreWrap.innerHTML = '';
    try {
        const page = await fetchTrendingTopicsPage(trendingTopicsState.range, null);
        trendingTopicsState.items = page.items;
        trendingTopicsState.lastDoc = page.lastDoc;
        trendingTopicsState.hasMore = page.hasMore;
        trendingTopicsState.lastLoadSucceeded = true;
        renderTrendingTopics(trendingTopicsState.items);
        trendingTopicsState.lastLoaded = Date.now();
        uiDebugLog('trending topics loaded', { range: trendingTopicsState.range, count: trendingTopicsState.items.length });
    } catch (error) {
        console.warn('Trending topics load failed', error?.message || error);
        list.innerHTML = `<div class="trend-item"><span>Unable to load topics.</span><span></span></div>`;
    } finally {
        trendingTopicsState.loading = false;
        if (trendingTopicsState.needsRefresh) {
            trendingTopicsState.needsRefresh = false;
            loadTrendingTopics(trendingTopicsState.range);
        }
    }
}

async function loadMoreTrendingTopics() {
    if (trendingTopicsState.loading || !trendingTopicsState.hasMore) return;
    trendingTopicsState.loading = true;
    try {
        const page = await fetchTrendingTopicsPage(trendingTopicsState.range, trendingTopicsState.lastDoc);
        trendingTopicsState.items = trendingTopicsState.items.concat(page.items);
        trendingTopicsState.lastDoc = page.lastDoc;
        trendingTopicsState.hasMore = page.hasMore;
        renderTrendingTopics(trendingTopicsState.items);
    } catch (error) {
        console.warn('Trending topics pagination failed', error?.message || error);
    } finally {
        trendingTopicsState.loading = false;
    }
}

function initTrendingTopicsUI() {
    const select = document.getElementById('trending-range-select');
    if (!select) return;
    let storedRange = null;
    try {
        storedRange = window.localStorage?.getItem(TRENDING_RANGE_STORAGE_KEY);
    } catch (error) {
        storedRange = null;
    }
    const resolvedRange = TRENDING_RANGE_WINDOWS[storedRange] ? storedRange : TRENDING_DEFAULT_RANGE;
    trendingTopicsState.range = resolvedRange;
    trendingTopicsState.items = [];
    trendingTopicsState.lastDoc = null;
    trendingTopicsState.hasMore = false;
    select.value = trendingTopicsState.range;
    if (!select.__nexeraBound) {
        select.addEventListener('change', function () {
            const nextRange = select.value || 'week';
            trendingTopicsState.items = [];
            trendingTopicsState.lastDoc = null;
            trendingTopicsState.hasMore = false;
            try {
                window.localStorage?.setItem(TRENDING_RANGE_STORAGE_KEY, nextRange);
            } catch (error) {
                // ignore storage failures
            }
            loadTrendingTopics(nextRange);
        });
        select.__nexeraBound = true;
    }
    loadTrendingTopics(trendingTopicsState.range);
}

function getSystemTheme() {
    return window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
}

// Unique helper to avoid name collisions elsewhere in the module
function nexeraGetStoredThemePreference() {
    try { return localStorage.getItem('nexera-theme'); }
    catch (e) { return null; }
}

function applyTheme(preference = 'system') {
    const resolved = preference === 'system' ? getSystemTheme() : preference;
    document.body.classList.toggle('light-mode', resolved === 'light');
    document.body.dataset.themePreference = preference;
    refreshBrandLogos();
    try { localStorage.setItem('nexera-theme', preference); }
    catch (e) { console.warn('Theme storage blocked'); }
}

async function persistThemePreference(preference = 'system') {
    userProfile.theme = preference;
    applyTheme(preference);
    if (currentUser) {
        try {
            await setDoc(doc(db, "users", currentUser.uid), { theme: preference }, { merge: true });
        } catch (e) {
            console.warn('Theme save failed', e.message);
        }
    }
}

async function ensureUserDocument(user) {
    const ref = doc(db, "users", user.uid);
    const snap = await getDoc(ref);
    const now = serverTimestamp();

    if (!snap.exists()) {
        const avatarColor = computeAvatarColor(user.uid || user.email || 'user');
        const createResult = await guardFirebaseCall('users:create', function () {
            return setDoc(ref, {
                displayName: user.displayName || "Nexera User",
                username: user.email ? user.email.split('@')[0] : `user_${user.uid.slice(0, 6)}`,
                photoURL: user.photoURL || "",
                photoPath: "",
                avatarColor,
                bio: "",
                website: "",
                region: "",
                email: user.email || "",
                locationHistory: [],
                accountRoles: [],
                tagAffinity: {},
                followedCategories: [],
                interests: [],
                createdAt: now,
                updatedAt: now
            }, { merge: true });
        });
        if (createResult.ok) {
            await syncPublicProfile(user.uid, {
                displayName: user.displayName || "Nexera User",
                username: user.email ? user.email.split('@')[0] : `user_${user.uid.slice(0, 6)}`,
                photoURL: user.photoURL || "",
                avatarColor,
                bio: ""
            });
        }
        return await getDoc(ref);
    }

    const existingData = snap.data() || {};
    if (!existingData.displayName && existingData.name) {
        await setDoc(ref, { displayName: existingData.name }, { merge: true });
    }
    await setDoc(ref, { updatedAt: now }, { merge: true });
    await syncPublicProfile(user.uid, existingData);
    return await getDoc(ref);
}

async function backfillAvatarColorIfMissing(uid, profile = {}) {
    if (!uid || avatarColorBackfilled) return;
    if (!profile.avatarColor) {
        const color = computeAvatarColor(uid || profile.username || profile.name || 'user');
        profile.avatarColor = color;
        try {
            await setDoc(doc(db, 'users', uid), { avatarColor: color }, { merge: true });
            await syncPublicProfile(uid, { ...profile, avatarColor: color });
            avatarColorBackfilled = true;
        } catch (e) {
            console.warn('Unable to backfill avatar color', e);
        }
    } else {
        avatarColorBackfilled = true;
    }
}

function shouldRerenderThread(newData, prevData = {}) {
    const fieldsToWatch = ['title', 'content', 'mediaUrl', 'type', 'category', 'trustScore'];
    return fieldsToWatch.some(function (key) { return newData[key] !== prevData[key]; });
}

function setButtonLoadingState(btn, isLoading, labelText) {
    if (!btn) return;
    if (isLoading) {
        btn.dataset.originalLabel = btn.dataset.originalLabel || btn.textContent;
        btn.innerHTML = '<span class="button-spinner" aria-hidden="true"></span>';
        btn.disabled = true;
    } else {
        const finalLabel = labelText || btn.dataset.originalLabel || btn.textContent || '';
        btn.textContent = finalLabel;
        btn.disabled = false;
    }
}

function buildActionButton(label, icon, onClick, options = {}) {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = options.className || 'icon-pill';
    btn.innerHTML = icon ? `<i class="${icon}"></i> ${label}` : label;
    btn.setAttribute('aria-label', label);
    if (options.disabled) btn.disabled = true;
    if (typeof onClick === 'function') {
        btn.addEventListener('click', onClick);
    }
    return btn;
}

// --- Auth Functions ---
window.handleLogin = async function (e) {
    if (e && typeof e.preventDefault === "function") e.preventDefault();

    const loginBtn = document.getElementById('login-submit');
    try {
        var emailEl = document.getElementById('email');
        var passEl = document.getElementById('password');
        var errEl = document.getElementById('auth-error');
        if (errEl) errEl.textContent = '';

        var email = emailEl ? emailEl.value : '';
        var pass = passEl ? passEl.value : '';

        if (!email || !pass) {
            if (errEl) errEl.textContent = 'Please enter email and password.';
            return;
        }

        setButtonLoadingState(loginBtn, true, 'Log In');
        var cred = await signInWithEmailAndPassword(auth, email, pass);

        if (typeof ensureUserDocument === 'function') {
            await ensureUserDocument(cred.user);
        }
    } catch (err) {
        var errEl2 = document.getElementById('auth-error');
        if (errEl2) errEl2.textContent = err.message;
        console.error(err);
    } finally {
        setButtonLoadingState(loginBtn, false, 'Log In');
    }
};

window.handleSignup = async function (e) {
    e.preventDefault();
    const signupBtn = document.getElementById('signup-submit');
    try {
        setButtonLoadingState(signupBtn, true, 'Sign Up');
        const cred = await createUserWithEmailAndPassword(
            auth,
            document.getElementById('email').value,
            document.getElementById('password').value
        );
        // Create initial user document
        const createResult = await guardFirebaseCall('users:create', function () {
            return setDoc(doc(db, "users", cred.user.uid), {
            displayName: cred.user.displayName || cred.user.email.split('@')[0] || "Nexera User",
            username: cred.user.email.split('@')[0],
            email: cred.user.email,
            createdAt: serverTimestamp(),
            updatedAt: serverTimestamp(),
            savedPosts: [],
            followersCount: 0,
            following: [],
            photoURL: "",
            photoPath: "",
            avatarColor: computeAvatarColor(cred.user.uid || cred.user.email || 'user'),
            bio: "",
            website: "",
            region: "",
            accountRoles: [],
            tagAffinity: {},
            interests: []
            });
        }, {
            onPermissionDenied: function () {
                document.getElementById('auth-error').textContent = 'Profile can’t be updated due to permissions.';
            }
        });
        if (!createResult.ok) {
            if (!createResult.permissionDenied) {
                document.getElementById('auth-error').textContent = 'Sign up failed. Please try again.';
            }
            return;
        }
        await syncPublicProfile(cred.user.uid, {
            displayName: cred.user.displayName || cred.user.email.split('@')[0] || "Nexera User",
            username: cred.user.email.split('@')[0],
            photoURL: "",
            avatarColor: computeAvatarColor(cred.user.uid || cred.user.email || 'user'),
            bio: ""
        }, {
            onPermissionDenied: function () {
                document.getElementById('auth-error').textContent = 'Profile can’t be updated due to permissions.';
            }
        });
    } catch (err) {
        document.getElementById('auth-error').textContent = err.message;
    } finally {
        setButtonLoadingState(signupBtn, false, 'Sign Up');
    }
};

window.handleAnon = async function () {
    try {
        await signInAnonymously(auth);
    } catch (e) {
        console.error(e);
    }
};

function bindAuthFormShortcuts() {
    const emailInput = document.getElementById('email');
    const passwordInput = document.getElementById('password');
    if (emailInput) {
        emailInput.addEventListener('keydown', function (evt) {
            if (evt.key === 'Enter') {
                evt.preventDefault();
                if (passwordInput) passwordInput.focus();
            }
        });
    }
    if (passwordInput) {
        passwordInput.addEventListener('keydown', function (evt) {
            if (evt.key === 'Enter') {
                evt.preventDefault();
                window.handleLogin(evt);
            }
        });
    }
}

window.handleLogout = function () {
    signOut(auth);
    location.reload();
};

// --- Data Fetching & Caching ---
async function fetchMissingProfiles(posts) {
    const missingIds = new Set();
    posts.forEach(function (post) {
        if (post.userId && !userCache[post.userId]) {
            missingIds.add(post.userId);
        }
    });

    if (missingIds.size === 0) return;

    // Fetch up to 10 at a time or simple Promise.all
    if (!getViewerUid()) {
        Array.from(missingIds).forEach(function (uid) { buildUnknownUserProfile(uid); });
        return;
    }

    const fetchPromises = Array.from(missingIds).map(function (uid) { return getDoc(doc(db, "profiles", uid)); });

    try {
        const userDocs = await Promise.all(fetchPromises);
        const missingProfiles = [];
        userDocs.forEach(function (docSnap) {
            if (docSnap.exists()) {
                storeUserInCache(docSnap.id, docSnap.data());
            } else {
                missingProfiles.push(docSnap.id);
            }
        });

        if (missingProfiles.length) {
            try {
                const fallbackDocs = await Promise.all(missingProfiles.map(function (uid) { return getDoc(doc(db, "users", uid)); }));
                fallbackDocs.forEach(function (docSnap) {
                    if (docSnap.exists()) {
                        storeUserInCache(docSnap.id, mapUserDocToProfile(docSnap.data()));
                    } else {
                        logMissingProfileOnce(docSnap.id);
                        storeUserInCache(docSnap.id, { name: "Unknown User", username: "unknown" });
                    }
                });
            } catch (e) {
                if (e?.code === 'permission-denied') {
                    logPermissionDeniedOnce('users:read:fetchMissingProfiles');
                    missingProfiles.forEach(function (uid) { buildUnknownUserProfile(uid); });
                } else {
                    console.warn('Fallback profile read failed', e?.message || e);
                }
            }
        }

        // Re-render dependent views once data arrives
        renderFeed();
        if (activePostId) renderThreadMainPost(activePostId);
    } catch (e) {
        if (e?.code === 'permission-denied') {
            logPermissionDeniedOnce('profiles:read:fetchMissingProfiles');
            try {
                const fallbackDocs = await Promise.all(Array.from(missingIds).map(function (uid) { return getDoc(doc(db, "users", uid)); }));
                fallbackDocs.forEach(function (docSnap) {
                    if (docSnap.exists()) {
                        storeUserInCache(docSnap.id, mapUserDocToProfile(docSnap.data()));
                    } else {
                        buildUnknownUserProfile(docSnap.id);
                    }
                });
            } catch (fallbackError) {
                if (fallbackError?.code === 'permission-denied') {
                    logPermissionDeniedOnce('users:read:fetchMissingProfiles');
                } else {
                    console.warn('Fallback profile read failed', fallbackError?.message || fallbackError);
                }
                Array.from(missingIds).forEach(function (uid) { buildUnknownUserProfile(uid); });
            }
            return;
        }
        console.error("Error fetching profiles:", e);
    }
}

async function waitForFeedMedia(targetId = 'feed-content') {
    const container = document.getElementById(targetId);
    if (!container) return;
    const nodes = Array.from(container.querySelectorAll('img, video'));
    if (nodes.length === 0) return;
    await Promise.all(nodes.map(function (node) {
        if ((node.tagName === 'IMG' && node.complete) || (node.tagName === 'VIDEO' && node.readyState >= 3)) return Promise.resolve();
        return new Promise(function (resolve) {
            node.addEventListener('load', resolve, { once: true });
            node.addEventListener('error', resolve, { once: true });
            if (node.tagName === 'VIDEO') {
                node.addEventListener('loadeddata', resolve, { once: true });
            }
        });
    }));
}

async function fetchFeedBatch({ reset = false } = {}) {
    // Fetch a small batch of posts to keep the initial load fast.
    if (feedPagination.loading || feedPagination.done) return [];
    feedPagination.loading = true;
    try {
        const postsRef = collection(db, 'posts');
        const constraints = [orderBy('timestamp', 'desc'), limit(FEED_BATCH_SIZE)];
        if (!reset && feedPagination.lastDoc) {
            constraints.splice(1, 0, startAfter(feedPagination.lastDoc));
        }
        const snapshot = await getDocs(query(postsRef, ...constraints));
        feedPagination.lastDoc = snapshot.docs[snapshot.docs.length - 1] || feedPagination.lastDoc;
        if (snapshot.docs.length < FEED_BATCH_SIZE) {
            feedPagination.done = true;
        }
        return snapshot.docs.map(function (docSnap) {
            const data = docSnap.data();
            return normalizePostData(docSnap.id, data);
        });
    } finally {
        feedPagination.loading = false;
    }
}

async function loadFeedData({ showSplashDuringLoad = false } = {}) {
    if (feedLoading && feedHydrationPromise) return feedHydrationPromise;

    feedLoading = true;
    feedHydrationPromise = (async function () {
        if (showSplashDuringLoad) showSplash();
        feedPagination.lastDoc = null;
        feedPagination.done = false;
        const batch = await fetchFeedBatch({ reset: true });
        const nextCache = {};
        allPosts = batch.slice();
        batch.forEach(function (post) {
            nextCache[post.id] = post;
        });

        if (currentUser) {
            const ownLocs = allPosts.filter(function (p) { return p.userId === currentUser.uid && p.location; }).map(function (p) { return p.location; });
            const merged = new Set([...(recentLocations || []), ...ownLocs]);
            recentLocations = Array.from(merged).slice(-10);
        }

        fetchMissingProfiles(allPosts);
        await loadHomeMediaData();
        loadTrendingTopics(trendingTopicsState.range);
        feedLoading = false;
        renderFeed();
        if (isInitialLoad) {
            isInitialLoad = false;
            if (window.Nexera?.releaseSplash) {
                window.Nexera.releaseSplash('feed-initial-ready');
            }
        }
        await waitForFeedMedia();
        postSnapshotCache = nextCache;
    })().catch(function (error) {
        console.error('Feed load failed', error);
    }).finally(function () {
        feedLoading = false;
        if (showSplashDuringLoad) hideSplash();
    });

    return feedHydrationPromise;
}

async function loadHomeMediaData() {
    if (homeMediaLoading && homeMediaPromise) return homeMediaPromise;
    homeMediaLoading = true;
    homeMediaPromise = Promise.all([loadHomeVideos(), loadHomeLiveSessions()])
        .catch(function (error) {
            console.warn('Home media load failed', error?.message || error);
        })
        .finally(function () {
            homeMediaLoading = false;
        });
    return homeMediaPromise;
}

async function loadHomeVideos() {
    try {
        const snapshot = await getDocs(query(collection(db, 'videos'), orderBy('createdAt', 'desc'), limit(5)));
        homeVideosCache = snapshot.docs.map(function (docSnap) { return ({ id: docSnap.id, ...docSnap.data() }); });
        homeVideosCache.forEach(ensureVideoStats);
    } catch (error) {
        console.warn('Home videos load failed', error?.message || error);
        homeVideosCache = [];
    }
    return homeVideosCache;
}

async function loadHomeLiveSessions() {
    try {
        const snapshot = await getDocs(query(collection(db, 'liveStreams'), orderBy('createdAt', 'desc'), limit(5)));
        homeLiveSessionsCache = snapshot.docs.map(function (docSnap) { return ({ id: docSnap.id, ...docSnap.data() }); });
    } catch (error) {
        console.warn('Home livestream load failed', error?.message || error);
        homeLiveSessionsCache = [];
    }
    return homeLiveSessionsCache;
}

// Prime Live Directory layout
if (typeof renderLiveDirectoryFromCache === 'function') renderLiveDirectoryFromCache();

function startCategoryStreams(uid) {
    if (categoryUnsubscribe) categoryUnsubscribe();
    if (membershipUnsubscribe) membershipUnsubscribe();

    destinationPickerLoading = true;
    destinationPickerError = '';

    const categoryRef = collection(db, 'categories');
    categoryUnsubscribe = ListenerRegistry.register('categories:all', onSnapshot(categoryRef, function (snapshot) {
        categories = snapshot.docs.map(function (docSnap) {
            return { id: docSnap.id, ...docSnap.data() };
        });
        allPosts = allPosts.map(function (p) { return normalizePostData(p.id, p); });
        destinationPickerLoading = false;
        destinationPickerError = '';
        ensureDefaultDestination();
        renderDestinationField();
        renderDestinationPicker();
        syncPostButtonState();
        renderFeed();
    }, function () {
        destinationPickerLoading = false;
        destinationPickerError = 'Unable to load destinations.';
        renderDestinationField();
        renderDestinationPicker();
    }));

    const membershipRef = collection(db, `users/${uid}/categoryMemberships`);
    membershipUnsubscribe = ListenerRegistry.register(`memberships:${uid}`, onSnapshot(membershipRef, function (snapshot) {
        memberships = {};
        snapshot.forEach(function (docSnap) {
            memberships[docSnap.id] = normalizeMembershipData(docSnap.data());
        });
        allPosts = allPosts.map(function (post) {
            return { ...post, categoryStatus: memberships[post.categoryId]?.status || post.categoryStatus };
        });
        renderDestinationField();
        renderDestinationPicker();
        syncPostButtonState();
        renderFeed();
    }, function () {
        console.warn('Unable to load destination memberships');
    }));
}

function getCategorySnapshot(categoryId) {
    return categories.find(function (c) { return c.id === categoryId; }) || null;
}

async function loadVideoCategories(force = false) {
    if (videoDestinationLoading) return;
    if (videoDestinationLoaded && !force) return;
    videoDestinationLoading = true;
    videoDestinationError = '';
    try {
        const snap = await getDocs(query(collection(db, 'categories'), orderBy('name')));
        videoCategories = snap.docs.map(function (docSnap) {
            const data = docSnap.data() || {};
            return { id: docSnap.id, slug: docSnap.id, ...data };
        }).sort(function (a, b) { return (a.name || a.slug || '').localeCompare(b.name || b.slug || ''); });
        videoCategoryIndex = new Map(videoCategories.map(function (cat) { return [cat.slug || cat.id, cat]; }));
        videoDestinationLoaded = true;
    } catch (error) {
        console.warn('Unable to load video topics', error);
        videoDestinationError = 'Unable to load topics.';
        videoCategories = [];
        videoCategoryIndex = new Map();
    } finally {
        videoDestinationLoading = false;
    }
}

async function getCategoryMetaBySlug(slug) {
    if (!slug) return null;
    const cached = videoCategoryIndex.get(slug);
    if (cached) return cached;
    try {
        const snap = await getDoc(doc(db, 'categories', slug));
        if (!snap.exists()) return null;
        const data = { id: snap.id, slug: snap.id, ...snap.data() };
        videoCategoryIndex.set(slug, data);
        return data;
    } catch (error) {
        console.warn('Unable to load category meta', error);
        return null;
    }
}

function resolveCategoryLabelBySlug(slug) {
    if (!slug || slug === 'no-topic') return 'No topic';
    const entry = videoCategoryIndex.get(slug);
    return entry?.name || entry?.slug || 'No topic';
}

function normalizeMembershipData(raw = {}) {
    const roles = Array.isArray(raw.roles) ? raw.roles : (raw.role ? [raw.role] : []);
    return { ...raw, roles };
}

function normalizeMentionsField(raw = []) {
    if (!Array.isArray(raw)) return [];
    const mentions = raw.map(function (entry) {
        if (typeof entry === 'string') return normalizeMentionEntry(entry);
        return normalizeMentionEntry(entry || {});
    }).filter(function (m) { return !!m.username; });
    const seen = new Set();
    return mentions.filter(function (m) {
        if (seen.has(m.username)) return false;
        seen.add(m.username);
        return true;
    });
}

function normalizePostData(id, data) {
    const visibility = data.visibility || (data.isPrivate ? 'private' : 'public');
    const categoryId = data.categoryId || null;
    const categoryDoc = getCategorySnapshot(categoryId) || {};
    const categoryName = data.categoryName || data.category || categoryDoc.name || 'Uncategorized';
    const categorySlug = data.categorySlug || categoryDoc.slug || slugifyCategory(categoryName);
    const categoryVerified = data.categoryVerified !== undefined ? data.categoryVerified : !!categoryDoc.verified;
    const categoryType = data.categoryType || categoryDoc.type || null;

    const contentType = data.contentType || (data.type === 'video' ? 'video' : data.type === 'image' ? 'image' : 'text');
    const incomingContent = typeof data.content === 'object' && data.content !== null ? data.content : {};
    const content = {
        text: typeof data.content === 'string' ? data.content : incomingContent.text || data.title || '',
        mediaUrl: incomingContent.mediaUrl || data.mediaUrl || null,
        linkUrl: incomingContent.linkUrl || data.linkUrl || null,
        profileUid: incomingContent.profileUid || data.profileUid || null,
        meta: incomingContent.meta || data.meta || {}
    };

    const normalizedTags = Array.isArray(data.tags) ? Array.from(new Set(data.tags.map(normalizeTagValue).filter(Boolean))) : [];
    const normalizedMentions = normalizeMentionsField(data.mentions);
    const normalizedPoll = normalizePollPayload(data.poll);
    const scheduledField = data.scheduledFor;
    const scheduledFor = scheduledField instanceof Timestamp
        ? scheduledField
        : (scheduledField && typeof scheduledField.seconds === 'number' ? new Timestamp(scheduledField.seconds, scheduledField.nanoseconds || 0) : null);
    const location = data.location || '';

    const normalized = {
        id,
        ...data,
        visibility,
        categoryId,
        categoryName,
        categorySlug,
        categoryVerified,
        categoryType,
        category: categoryName,
        contentType,
        tags: normalizedTags,
        mentions: normalizedMentions,
        content,
        categoryStatus: memberships[categoryId]?.status || 'unknown',
        poll: normalizedPoll,
        scheduledFor,
        location
    };

    if (!normalized.title && typeof data.title === 'string') normalized.title = data.title;
    if (!normalized.content && typeof data.content === 'string') normalized.content = data.content;

    return normalized;
}

function getDestinationFromCategory(cat) {
    return {
        type: cat.type === 'official' ? 'official' : 'community',
        id: cat.id,
        name: cat.name || 'Unnamed',
        avatarUrl: cat.avatarUrl || cat.iconUrl || null,
        verified: !!cat.verified,
        meta: { memberCount: cat.memberCount, description: cat.description }
    };
}

function truncateDescription(text = '', wordLimit = 75) {
    const normalized = typeof text === 'string' ? text.trim().replace(/\s+/g, ' ') : '';
    if (!normalized) return '';
    const words = normalized.split(' ');
    if (words.length <= wordLimit) return normalized;
    return `${words.slice(0, wordLimit).join(' ')}...`;
}

function ensureDefaultDestination() {
    if (!selectedCategoryId && categories.length) {
        const fallback = categories.find(function (c) { return c.type === 'community'; })
            || categories.find(function (c) { return c.type === 'official'; })
            || categories[0];
        selectedCategoryId = fallback ? fallback.id : null;
    }
}

function renderDestinationField() {
    const labelEl = document.getElementById('destination-current-label');
    const verifiedEl = document.getElementById('destination-current-verified');
    const spinner = document.getElementById('destination-loading-spinner');
    const currentCategoryDoc = selectedCategoryId ? getCategorySnapshot(selectedCategoryId) : null;

    if (spinner) spinner.style.display = destinationPickerLoading ? 'block' : 'none';
    if (labelEl) labelEl.textContent = currentCategoryDoc ? currentCategoryDoc.name : 'Select...';
    if (verifiedEl) {
        const isVerified = currentCategoryDoc && currentCategoryDoc.verified;
        verifiedEl.style.display = isVerified ? 'inline-flex' : 'none';
        verifiedEl.innerHTML = isVerified ? getVerifiedIconSvg() : '';
    }
}

function renderVideoDestinationField() {
    const labelEl = document.getElementById('video-destination-current-label');
    const verifiedEl = document.getElementById('video-destination-current-verified');
    const currentCategoryDoc = videoPostingDestinationId ? videoCategoryIndex.get(videoPostingDestinationId) : null;
    const label = resolveCategoryLabelBySlug(videoPostingDestinationId) || videoPostingDestinationName || 'No topic';
    if (labelEl) labelEl.textContent = label;
    if (verifiedEl) {
        const isVerified = currentCategoryDoc && currentCategoryDoc.verified;
        verifiedEl.style.display = isVerified ? 'inline-flex' : 'none';
        verifiedEl.innerHTML = isVerified ? getVerifiedIconSvg() : '';
    }
}

function bindVideoDestinationField() {
    const btn = document.getElementById('video-destination-field');
    if (!btn || btn.__nexeraBound) return;
    btn.addEventListener('click', function () {
        window.openVideoDestinationPicker?.();
    });
    btn.__nexeraBound = true;
}

function setVideoPostingDestination(destination) {
    if (!destination) {
        videoPostingDestinationId = 'no-topic';
        videoPostingDestinationName = 'No topic';
    } else {
        videoPostingDestinationId = destination?.id || destination?.slug || 'no-topic';
        videoPostingDestinationName = destination?.name || destination?.label || resolveCategoryLabelBySlug(videoPostingDestinationId) || 'No topic';
    }
    renderVideoDestinationField();
}

function setComposerError(message = '') {
    composerError = message || '';
    syncPostButtonState();
}

function syncPostButtonState() {
    const btn = document.getElementById('publishBtn');
    const helper = document.getElementById('destination-helper');
    const title = document.getElementById('postTitle');
    const content = document.getElementById('postContent');
    const fileInput = document.getElementById('postFile');
    if (!btn) return;
    const hasContent = (title && title.value.trim()) || (content && content.value.trim()) || (fileInput && fileInput.files && fileInput.files[0]);
    btn.disabled = !!composerError || !hasContent;
    if (helper) {
        helper.style.display = composerError ? 'flex' : 'none';
        helper.textContent = composerError || '';
        helper.classList.toggle('error', !!composerError);
    }
}

// --- Destination Picker (DEDUPED / single source of truth) ---
function computeDestinationTabs() {
    if (destinationPickerTarget === 'video') return [];
    const tabs = [];
    if (activeDestinationConfig.enableCommunityTab !== false) {
        tabs.push({ type: 'community', label: activeDestinationConfig.communityTabLabel || 'Community' });
    }
    if (activeDestinationConfig.enableOfficialTab !== false) {
        tabs.push({ type: 'official', label: activeDestinationConfig.officialTabLabel || 'Official (Verified)' });
    }
    return tabs;
}

function setDestinationTab(tab) {
    destinationPickerTab = tab;
    destinationPickerSearch = '';
    destinationCreateExpanded = false;
    renderDestinationPicker();
    setTimeout(function () {
        const input = document.getElementById('destination-search-input');
        if (input) input.focus();
    }, 50);
}

function handleDestinationSelected(destination) {
    destinationPickerSelectionId = destination ? destination.id : null;
    if (destinationPickerTarget === 'video') {
        setVideoPostingDestination(destination);
        renderDestinationPicker();
        closeDestinationPicker();
        return;
    }
    selectedCategoryId = destination ? destination.id : null;
    renderDestinationField();
    renderDestinationPicker();
    syncPostButtonState();
    closeDestinationPicker();
}

function renderDestinationCreateArea() {
    const area = document.getElementById('destination-create-area');
    if (!area) return;

    if (destinationPickerTarget === 'video' || destinationPickerTab !== 'community' || activeDestinationConfig.enableCreateCommunity === false) {
        area.innerHTML = '';
        return;
    }

    area.innerHTML = `
        <div class="destination-create">
            <button class="icon-pill" id="destination-create-toggle"><i class="ph ph-plus"></i> ${destinationCreateExpanded ? 'Hide Create Community' : 'Create Community'}</button>
            ${destinationCreateExpanded ? `
                <div class="destination-create-form">
                    <input type="text" id="new-category-name" class="form-input" placeholder="Community name" aria-label="Community name">
                    <textarea id="new-category-description" class="form-input" placeholder="Description" aria-label="Community description"></textarea>
                    <textarea id="new-category-rules" class="form-input" placeholder="Additional rules (one per line)" aria-label="Community rules"></textarea>
                    <label class="checkbox-row"><input type="checkbox" id="new-category-public" checked> Publicly discoverable</label>
                    <button class="create-btn-sidebar" id="destination-create-submit" style="width:100%;">Create</button>
                </div>
            ` : ''}
        </div>`;

    const toggle = document.getElementById('destination-create-toggle');
    if (toggle) toggle.onclick = function () {
        destinationCreateExpanded = !destinationCreateExpanded;
        renderDestinationPicker();
    };

    const submit = document.getElementById('destination-create-submit');
    if (submit) submit.onclick = function (e) {
        e.preventDefault();
        window.handleCreateCategoryForm();
    };
}

function retryDestinationLoad() {
    if (!currentUser) return;
    destinationPickerError = '';
    destinationPickerLoading = true;
    renderDestinationPicker();
    startCategoryStreams(currentUser.uid);
}

function renderDestinationResults() {
    const resultsEl = document.getElementById('destination-results');
    if (!resultsEl) return;

    const isVideoTarget = destinationPickerTarget === 'video';
    const loading = isVideoTarget ? videoDestinationLoading : destinationPickerLoading;
    const error = isVideoTarget ? videoDestinationError : destinationPickerError;
    if (error) {
        resultsEl.innerHTML = `<div class="destination-error">${error}<div style="margin-top:8px;"><button class="icon-pill" id="destination-retry-btn">Retry</button></div></div>`;
        const retryBtn = document.getElementById('destination-retry-btn');
        if (retryBtn) retryBtn.onclick = function () { isVideoTarget ? loadVideoCategories(true).then(renderDestinationPicker) : retryDestinationLoad(); };
        return;
    }

    if (loading) {
        resultsEl.innerHTML = '<div class="destination-loading"><div class="inline-spinner" style="display:block; margin: 0 auto 8px;"></div>Loading destinations...</div>';
        return;
    }

    const source = isVideoTarget ? videoCategories : categories;
    const filtered = source
        .filter(function (c) {
            if (isVideoTarget) return true;
            return destinationPickerTab === 'official' ? c.type === 'official' : c.type === 'community';
        })
        .filter(function (c) {
            return !destinationPickerSearch || (c.name || '').toLowerCase().includes(destinationPickerSearch.toLowerCase());
        })
        .sort(function (a, b) { return (a.name || '').localeCompare(b.name || ''); });

    if (!filtered.length) {
        const message = isVideoTarget
            ? 'No topics found.'
            : (destinationPickerTab === 'official'
                ? 'No official destinations found.'
                : 'No communities found. Create one?');
        resultsEl.innerHTML = `<div class="destination-empty">${message}</div>`;
        return;
    }

    resultsEl.innerHTML = '';
    filtered.forEach(function (cat) {
        const destination = getDestinationFromCategory(cat);
        const isSelected = destination.id === destinationPickerSelectionId;
        const selectable = destination.type === 'official'
            ? activeDestinationConfig.officialSelectable !== false
            : true;

        const row = document.createElement('div');
        row.className = 'destination-row' + (isSelected ? ' selected' : '');

        const main = document.createElement('div');
        main.className = 'destination-row-main';

        const avatar = document.createElement('div');
        avatar.className = 'destination-avatar';
        avatar.textContent = (destination.name || 'U')[0];
        if (destination.avatarUrl) {
            avatar.style.backgroundImage = `url('${destination.avatarUrl}')`;
            avatar.style.backgroundSize = 'cover';
            avatar.textContent = '';
        }

        const textWrap = document.createElement('div');
        textWrap.className = 'destination-row-text';

        const title = document.createElement('div');
        title.className = 'destination-row-title';
        title.textContent = destination.name || 'Unnamed';
        if (destination.verified) {
            const badge = document.createElement('span');
            badge.className = 'verified-badge';
            badge.innerHTML = getVerifiedIconSvg();
            title.appendChild(badge);
        }

        const desc = document.createElement('div');
        desc.className = 'destination-row-desc';
        const memberCount = destination.meta && destination.meta.memberCount ? `${destination.meta.memberCount} members` : '';
        const description = destination.meta?.description || '';
        desc.textContent = truncateDescription(description, 75) || memberCount || '';

        textWrap.appendChild(title);
        textWrap.appendChild(desc);

        main.appendChild(avatar);
        main.appendChild(textWrap);

        const actions = document.createElement('div');
        actions.className = 'destination-row-actions';

        const selectBtn = document.createElement('button');
        selectBtn.className = 'icon-pill';
        selectBtn.disabled = !selectable;
        selectBtn.innerHTML = isSelected ? '<i class="ph ph-check"></i> Selected' : 'Select';
        selectBtn.onclick = function (e) {
            e.stopPropagation();
            if (selectable) handleDestinationSelected(destination);
        };
        actions.appendChild(selectBtn);

        if (selectable) {
            row.onclick = function () { handleDestinationSelected(destination); };
            main.onclick = function () { handleDestinationSelected(destination); };
        }

        row.appendChild(main);
        row.appendChild(actions);
        resultsEl.appendChild(row);
    });
}

function renderDestinationPicker() {
    const modal = document.getElementById('destination-picker-modal');
    if (!modal) return;
    modal.style.display = destinationPickerOpen ? 'flex' : 'none';

    const tabsContainer = document.getElementById('destination-picker-tabs');
    const availableTabs = computeDestinationTabs();

    if (!availableTabs.some(function (t) { return t.type === destinationPickerTab; }) && availableTabs.length) {
        destinationPickerTab = (availableTabs.find(function (t) { return t.type === 'community'; }) || availableTabs[0]).type;
    }

    if (tabsContainer) {
        tabsContainer.innerHTML = '';
        availableTabs.forEach(function (tab) {
            const btn = document.createElement('button');
            btn.className = 'destination-tab' + (tab.type === destinationPickerTab ? ' active' : '');
            btn.textContent = tab.label;
            btn.onclick = function () { setDestinationTab(tab.type); };
            tabsContainer.appendChild(btn);
        });
    }

    const searchInput = document.getElementById('destination-search-input');
    if (searchInput) {
        if (destinationPickerTarget === 'video') {
            searchInput.placeholder = 'Search topics';
        } else {
            searchInput.placeholder = destinationPickerTab === 'official' ? 'Search official destinations' : 'Search communities';
        }
        searchInput.value = destinationPickerSearch;
        searchInput.oninput = function (e) {
            const value = e.target.value;
            clearTimeout(destinationSearchTimeout);
            destinationSearchTimeout = setTimeout(function () {
                destinationPickerSearch = value.trim();
                renderDestinationPicker();
            }, 250);
        };
    }

    renderDestinationCreateArea();
    renderDestinationResults();
}

function openDestinationPicker(config = {}) {
    activeDestinationConfig = { ...DEFAULT_DESTINATION_CONFIG, ...config };
    destinationPickerTarget = config.target || 'post';
    destinationPickerSelectionId = destinationPickerTarget === 'video'
        ? (videoPostingDestinationId || selectedCategoryId)
        : selectedCategoryId;
    if (destinationPickerTarget === 'video') {
        videoDestinationLoading = true;
        videoDestinationError = '';
        loadVideoCategories().then(function () {
            renderVideoDestinationField();
            renderDestinationPicker();
        });
    }
    destinationPickerOpen = true;

    const currentCategoryDoc = destinationPickerSelectionId ? getCategorySnapshot(destinationPickerSelectionId) : null;
    const tabs = computeDestinationTabs();

    if (currentCategoryDoc && tabs.some(function (t) { return t.type === currentCategoryDoc.type; })) {
        destinationPickerTab = currentCategoryDoc.type;
    } else {
        destinationPickerTab = (tabs.find(function (t) { return t.type === 'community'; }) || tabs[0] || { type: 'community' }).type;
    }

    destinationPickerSearch = '';
    destinationCreateExpanded = false;

    renderDestinationPicker();

    setTimeout(function () {
        const input = document.getElementById('destination-search-input');
        if (input) input.focus();
    }, 50);
}

function closeDestinationPicker() {
    destinationPickerOpen = false;
    destinationPickerTarget = 'post';
    renderDestinationPicker();
}

window.openDestinationPicker = openDestinationPicker;
window.closeDestinationPicker = closeDestinationPicker;
window.openVideoDestinationPicker = function () {
    openDestinationPicker({ target: 'video' });
};


async function createCategory(payload) {
    if (!requireAuth()) return;
    const name = (payload.name || '').trim();
    if (!name) return toast('Name required', 'error');

    let slug = slugifyCategory(name);
    const slugExists = categories.some(function (c) { return c.slug === slug; });
    if (slugExists) slug = `${slug}-${Math.random().toString(36).substring(2, 6)}`;

    const rules = DEFAULT_CATEGORY_RULES.concat((payload.rules || []).filter(function (r) { return r && r.trim(); }));
    const categoryDoc = {
        name,
        slug,
        type: payload.type || 'community',
        verified: payload.type === 'official',
        description: payload.description || '',
        rules,
        createdBy: payload.type === 'official' ? null : currentUser.uid,
        createdAt: serverTimestamp(),
        isPublic: payload.isPublic !== false,
        memberCount: 0,
        mods: [],
        ownerId: payload.type === 'official' ? null : currentUser.uid
    };

    const docRef = doc(db, 'categories', slug);
    await setDoc(docRef, categoryDoc);
    await joinCategory(slug, 'owner');
    selectedCategoryId = slug;
    renderDestinationField();
    renderDestinationPicker();
    syncPostButtonState();
    closeDestinationPicker();
    toast('Topic created', 'info');
    return slug;
}

async function joinCategory(categoryId, role = 'member') {
    if (!requireAuth()) return;
    const catRef = doc(db, 'categories', categoryId);
    const membershipRef = doc(db, `categories/${categoryId}/members/${currentUser.uid}`);
    const userMembershipRef = doc(db, `users/${currentUser.uid}/categoryMemberships/${categoryId}`);
    const catSnap = await getDoc(catRef);
    if (!catSnap.exists()) return toast('Topic missing', 'error');
    const cat = catSnap.data();

    const membershipPayload = {
        uid: currentUser.uid,
        roles: [role],
        status: 'active',
        joinedAt: serverTimestamp(),
        updatedAt: serverTimestamp(),
        updatedBy: currentUser.uid
    };

    await Promise.all([
        setDoc(membershipRef, membershipPayload, { merge: true }),
        setDoc(userMembershipRef, {
            categoryId,
            name: cat.name,
            slug: cat.slug,
            type: cat.type,
            verified: !!cat.verified,
            roles: [role],
            status: 'active',
            joinedAt: serverTimestamp(),
            updatedAt: serverTimestamp()
        }, { merge: true })
    ]);

    memberships[categoryId] = { ...membershipPayload, name: cat.name };
    renderDestinationField();
    renderDestinationPicker();
    syncPostButtonState();
}

async function isMemberOfCategory(categoryId, uid = currentUser?.uid) {
    if (!categoryId || !uid) return false;
    const membership = memberships[categoryId];
    if (membership && membership.status === 'active') return true;

    const membershipRef = doc(db, `categories/${categoryId}/members/${uid}`);
    const snap = await getDoc(membershipRef);
    return snap.exists() && snap.data().status === 'active';
}

async function ensureJoinedCategory(categoryId, uid = currentUser?.uid) {
    if (!categoryId || !uid) throw new Error('Missing categoryId/uid');
    const alreadyMember = await isMemberOfCategory(categoryId, uid);
    if (alreadyMember) return true;

    await joinCategory(categoryId, 'member');
    return isMemberOfCategory(categoryId, uid);
}

async function leaveCategory(categoryId) {
    if (!requireAuth()) return;
    const membershipRef = doc(db, `categories/${categoryId}/members/${currentUser.uid}`);
    const userMembershipRef = doc(db, `users/${currentUser.uid}/categoryMemberships/${categoryId}`);

    await Promise.all([
        setDoc(membershipRef, { status: 'left', updatedAt: serverTimestamp(), updatedBy: currentUser.uid }, { merge: true }),
        setDoc(userMembershipRef, { status: 'left', updatedAt: serverTimestamp() }, { merge: true })
    ]);

    await enforceCategoryPrivacy(categoryId, currentUser.uid);
    memberships[categoryId] = { ...memberships[categoryId], status: 'left' };
    renderDestinationField();
    renderDestinationPicker();
    syncPostButtonState();
}

async function kickMember(categoryId, targetUid, reason = '') {
    if (!requireAuth()) return;
    const membership = memberships[categoryId];
    const membershipRoles = getMembershipRoles(categoryId);
    if (!membership || (!membershipRoles.has('owner') && !membershipRoles.has('mod'))) {
        return toast('Only mods/owners can kick', 'error');
    }

    const membershipRef = doc(db, `categories/${categoryId}/members/${targetUid}`);
    const userMembershipRef = doc(db, `users/${targetUid}/categoryMemberships/${categoryId}`);
    await Promise.all([
        setDoc(membershipRef, { status: 'kicked', updatedAt: serverTimestamp(), updatedBy: currentUser.uid, reason }, { merge: true }),
        setDoc(userMembershipRef, { status: 'kicked', updatedAt: serverTimestamp(), reason }, { merge: true })
    ]);

    await enforceCategoryPrivacy(categoryId, targetUid);
    toast('Member removed', 'info');
}

async function enforceCategoryPrivacy(categoryId, targetUid) {
    const q = query(collection(db, 'posts'), where('userId', '==', targetUid), where('categoryId', '==', categoryId));
    const snaps = await getDocs(q);
    const updates = snaps.docs.map(function (docSnap) {
        return updateDoc(docSnap.ref, { visibility: 'private', categoryAccess: 'removed', categoryAccessAt: serverTimestamp() });
    });
    await Promise.all(updates);
}

window.createCategory = createCategory;
window.joinCategory = joinCategory;
window.leaveCategory = leaveCategory;
window.kickMember = kickMember;
window.handleCreateCategoryForm = async function () {
    const nameInput = document.getElementById('new-category-name');
    const descriptionInput = document.getElementById('new-category-description');
    const rulesInput = document.getElementById('new-category-rules');
    const publicInput = document.getElementById('new-category-public');
    if (!nameInput || !descriptionInput || !rulesInput || !publicInput) return;

    const name = nameInput.value;
    const description = descriptionInput.value;
    const rulesText = rulesInput.value;
    const isPublic = publicInput.checked;
    const rules = rulesText ? rulesText.split('\n').map(function (r) { return r.trim(); }).filter(Boolean) : [];
    await createCategory({ name, description, rules, isPublic, type: 'community' });
    nameInput.value = '';
    descriptionInput.value = '';
    rulesInput.value = '';
    publicInput.checked = true;
    destinationCreateExpanded = false;
    renderDestinationPicker();
};

// PATCH: New listener to fetch user's reviews across all posts
async function startUserReviewListener(uid) {
    const q = query(collectionGroup(db, 'reviews'), where('userId', '==', uid));

    const handleSnapshot = function (snapshot) {
        window.myReviewCache = {};
        snapshot.forEach(function (doc) {
            const parentPostRef = doc.ref.parent.parent;
            if (parentPostRef) {
                window.myReviewCache[parentPostRef.id] = doc.data().rating;
            }
        });

        allPosts.forEach(function (post) { refreshSinglePostUI(post.id); });
        applyMyReviewStylesToDOM();
    };

    try {
        const initial = await getDocs(q);
        handleSnapshot(initial);
    } catch (error) {
        if (error.code !== 'permission-denied') {
            console.log("Initial audit hydration error:", error.message);
        }
        return; // Skip listener when access is not allowed
    }

    ListenerRegistry.register(`reviews:user:${uid}`, onSnapshot(q, handleSnapshot, function (error) {
        if (error.code !== 'permission-denied') {
            console.log("Review listener note:", error.message);
        }
    }));
}

// --- Navigation Logic ---
function syncSearchStateFromUrl(viewId) {
    const params = new URLSearchParams(window.location.search || '');
    const rawValue = (params.get(SEARCH_QUERY_KEY) || '').trim();
    const normalized = rawValue.toLowerCase();
    if (viewId === 'discover') {
        discoverSearchTerm = normalized;
    }
    if (viewId === 'videos') {
        videoSearchTerm = normalized;
    }
    if (viewId === 'live') {
        liveSearchTerm = normalized;
    }
}

window.navigateTo = function (viewId, pushToStack = true) {
    // Cleanup previous listeners if leaving specific views
    if (viewId !== 'thread') {
        if (threadUnsubscribe) threadUnsubscribe();
        if (activePostId) ListenerRegistry.unregister(`comments:${activePostId}`);
        threadUnsubscribe = null;
    }

    if (viewId !== 'messages') {
        ListenerRegistry.unregister('messages:list');
        if (activeConversationId) ListenerRegistry.unregister(`messages:thread:${activeConversationId}`);
        if (activeConversationId) ListenerRegistry.unregister(`conversation:details:${activeConversationId}`);
        if (activeConversationId) setTypingState(activeConversationId, false);
        if (conversationDetailsUnsubscribe) { conversationDetailsUnsubscribe(); conversationDetailsUnsubscribe = null; }
    }

    if (viewId !== 'videos' && currentViewId === 'videos') {
        transitionModalToMiniPlayer();
        pauseAllVideos();
        ListenerRegistry.unregister('videos:feed');
        videosFeedLoaded = false;
    }

    if (viewId !== 'live') {
        ListenerRegistry.unregister('live:sessions');
        if (activeLiveSessionId) {
            ListenerRegistry.unregister(`live:chat:${activeLiveSessionId}`);
            activeLiveSessionId = null;
        }
    }

    if (viewId !== 'live-setup' && currentViewId === 'live-setup') {
        if (window.__goLiveController?.cleanupSessionOnExit) {
            window.__goLiveController.cleanupSessionOnExit('navigate-away').catch((error) =>
                console.error('[GoLive] cleanup on navigate failed', error)
            );
        }
    }

    if (viewId !== 'staff') {
        ListenerRegistry.unregister('staff:verificationRequests');
        ListenerRegistry.unregister('staff:reports');
        ListenerRegistry.unregister('staff:adminLogs');
    }

    // Stack Management
    if (pushToStack && currentViewId !== viewId) {
        navStack.push({
            view: currentViewId,
            category: currentCategory,
            profileFilter: currentProfileFilter,
            viewingUser: viewingUserId,
            activePost: activePostId,
            scrollY: window.scrollY || 0
        });
    }

    // Toggle Views
    document.querySelectorAll('.view-section').forEach(function (el) { el.style.display = 'none'; });
    const targetView = document.getElementById('view-' + viewId);
    if (targetView) targetView.style.display = 'block';

    document.body.classList.toggle('sidebar-home', viewId === 'feed');
    document.body.classList.toggle('sidebar-wide', shouldShowRightSidebar(viewId));
    if (isMobileViewport()) {
        setSidebarOverlayOpen(false);
    }
    syncFeedTypeToggleState();

    // Toggle Navbar Active State
    if (viewId !== 'thread' && viewId !== 'public-profile') {
        document.querySelectorAll('.nav-item').forEach(function (el) { el.classList.remove('active'); });
        const navTarget = viewId === 'live-setup' ? 'live' : viewId;
        const navEl = document.getElementById('nav-' + navTarget);
        if (navEl) navEl.classList.add('active');
    }

    // View Specific Logic
    syncSearchStateFromUrl(viewId);
    if (viewId === 'feed') {
        if (!preserveFeedState) {
            currentCategory = 'For You';
        }
        preserveFeedState = false;
        renderFeed();
        loadFeedData();
    }
    if (viewId === 'saved') { renderSaved(); }
    if (viewId === 'profile') renderProfile();
    if (viewId === 'discover') { renderDiscover(); }
    if (viewId === 'messages') {
        releaseScrollLockIfSafe();
        initConversations();
        syncMobileMessagesShell();
        setInboxMode(inboxMode || 'messages', { skipRouteUpdate: !pushToStack, routeView: viewId });
        refreshInboxLayout();
    } else {
        document.body.classList.remove('mobile-thread-open');
    }
    if (viewId === 'videos') {
        debugVideo('route-enter', { viewId });
        const routedVideoId = getVideoRouteVideoId();
        const requestedVideoId = routedVideoId || pendingVideoOpenId;
        clearVideoDetailState({ updateRoute: !requestedVideoId });
        initVideoFeed({ force: true });
        if (requestedVideoId) {
            pendingVideoOpenId = null;
            requestAnimationFrame(function () {
                window.openVideoDetail(requestedVideoId);
            });
        }
        pendingVideoOpenId = null;
    }
    if (shouldHideMiniPlayer(viewId) && miniPlayerMode !== 'pip') {
        hideMiniPlayer();
    } else if (miniPlayerState) {
        showMiniPlayer();
    }
    if (viewId === 'live') {
        renderLiveSessions();
    }
    if (viewId === 'live-setup') { renderLiveSetup(); }
    if (viewId === 'staff') { renderStaffConsole(); }

    if (viewId !== 'live-setup' && window.location.hash === '#live-setup') {
        history.replaceState(null, '', window.location.pathname + window.location.search);
    }

    currentViewId = viewId;
    updateMobileNavState(viewId);
    const lockScroll = ((viewId === 'messages' && !isMobileViewport()) || viewId === 'conversation-settings');
    const goLiveLock = viewId === 'live-setup';
    document.body.classList.toggle('messages-scroll-lock', lockScroll);
    document.body.classList.toggle('go-live-open', goLiveLock);
    if (!lockScroll && !goLiveLock) {
        document.body.style.overflow = '';
    }
    if (!lockScroll && !goLiveLock) window.scrollTo(0, 0);

    if (pushToStack && currentViewId === viewId && viewId !== 'messages') {
        const path = window.NexeraRouter?.buildUrlForSection?.(viewId) || null;
        if (path && window.location.pathname !== path) {
            history.pushState({}, '', path);
        }
    }
};

function updateMobileNavState(viewId = 'feed') {
    const label = MOBILE_SECTION_LABELS[viewId] || 'Explore';
    const labelEl = document.getElementById('mobile-section-label');
    if (labelEl) labelEl.textContent = label;
    document.querySelectorAll('.mobile-nav-btn').forEach(function (btn) {
        btn.classList.toggle('active', btn.dataset.view === viewId);
    });
}

function releaseScrollLockIfSafe() {
    const modalOpen = document.querySelector('.modal.show, .modal[open], .overlay.active, .go-live-studio.active');
    if (!modalOpen) {
        document.body.style.overflow = '';
        document.documentElement.style.overflow = '';
        document.body.classList.remove('no-scroll', 'modal-open');
        document.documentElement.classList.remove('no-scroll', 'modal-open');
    }
}

function syncMobileMessagesShell() {
    const backBtn = document.getElementById('mobile-thread-back');
    const shouldShowThread = isMobileViewport() && !!activeConversationId;
    document.body.classList.toggle('mobile-thread-open', shouldShowThread);
    if (backBtn) backBtn.style.display = isMobileViewport() ? 'inline-flex' : 'none';
}

function bindMobileMessageGestures() {
    const thread = document.querySelector('.messages-thread');
    if (!thread || thread.dataset.gestureBound) return;
    let startX = 0;
    let startY = 0;
    thread.addEventListener('touchstart', function (e) {
        const touch = e.touches[0];
        startX = touch.clientX;
        startY = touch.clientY;
    });
    thread.addEventListener('touchend', function (e) {
        if (!isMobileViewport()) return;
        const touch = e.changedTouches[0];
        const dx = touch.clientX - startX;
        const dy = Math.abs(touch.clientY - startY);
        if (startX < 40 && dx > 60 && dy < 40) {
            window.mobileMessagesBack();
        }
    });
    thread.dataset.gestureBound = 'true';
}

if (MOBILE_VIEWPORT && MOBILE_VIEWPORT.addEventListener) {
    MOBILE_VIEWPORT.addEventListener('change', syncMobileMessagesShell);
}

function bindMobileNav() {
    document.querySelectorAll('.mobile-nav-btn').forEach(function (btn) {
        btn.addEventListener('click', function () {
            const view = btn.dataset.view;
            if (view) window.navigateTo(view);
        });
    });
    const fab = document.getElementById('mobile-fab');
    if (fab) fab.onclick = function () { window.openMobileComposer(); };
}

function initMiniPlayerDrag() {
    const container = document.getElementById('video-mini-player');
    const handle = container?.querySelector('[data-mini-drag]');
    if (!container || !handle) return;
    let isDragging = false;
    let offsetX = 0;
    let offsetY = 0;

    const onPointerMove = function (event) {
        if (!isDragging) return;
        const x = event.clientX - offsetX;
        const y = event.clientY - offsetY;
        container.style.left = `${Math.max(8, Math.min(window.innerWidth - container.offsetWidth - 8, x))}px`;
        container.style.top = `${Math.max(8, Math.min(window.innerHeight - container.offsetHeight - 8, y))}px`;
        container.style.right = 'auto';
        container.style.bottom = 'auto';
    };

    const stopDrag = function () {
        if (!isDragging) return;
        isDragging = false;
        handle.style.cursor = 'grab';
        window.removeEventListener('pointermove', onPointerMove);
        window.removeEventListener('pointerup', stopDrag);
    };

    handle.addEventListener('pointerdown', function (event) {
        isDragging = true;
        handle.style.cursor = 'grabbing';
        const rect = container.getBoundingClientRect();
        offsetX = event.clientX - rect.left;
        offsetY = event.clientY - rect.top;
        window.addEventListener('pointermove', onPointerMove);
        window.addEventListener('pointerup', stopDrag);
    });
}

window.goBack = function () {
    if (navStack.length === 0) {
        window.navigateTo('feed', false);
        return;
    }

    const prevState = navStack.pop();

    // Context Restoration
    if (prevState.view === 'feed') currentCategory = prevState.category;
    if (prevState.view === 'public-profile') viewingUserId = prevState.viewingUser;
    if (prevState.view === 'thread') activePostId = prevState.activePost;

    if (prevState.view === 'feed') {
        preserveFeedState = true;
        if (typeof prevState.scrollY === 'number' && window.Nexera?.restoreFeedScroll) {
            window.Nexera.restoreFeedScroll(prevState.scrollY);
        }
    }

    window.navigateTo(prevState.view, false);

    // Re-render Views based on restored context
    if (prevState.view === 'public-profile' && viewingUserId) {
        window.openUserProfile(viewingUserId, null, false);
    }
    if (prevState.view === 'videos' && profileReturnContext?.videoId) {
        videoModalResumeTime = profileReturnContext.currentTime || 0;
        const videoId = profileReturnContext.videoId;
        profileReturnContext = null;
        window.openVideoDetail(videoId);
    }
};

// --- Follow Logic (Optimistic UI) ---
function updateFollowButtonsForUser(uid, isFollowing) {
    const btns = document.querySelectorAll(`.js-follow-user-${uid}`);
    btns.forEach(function (btn) {
        if (isFollowing) {
            btn.innerHTML = 'Following';
            btn.classList.add('following');

            btn.style.background = 'transparent';
            btn.style.borderColor = 'var(--border)';
            btn.style.color = 'var(--text-muted)';

            if (btn.classList.contains('create-btn-sidebar')) {
                btn.textContent = "Following";
                btn.style.background = "transparent";
                btn.style.color = "var(--primary)";
                btn.style.borderColor = "var(--primary)";
            }
        } else {
            btn.innerHTML = '<i class="ph-bold ph-plus"></i> User';
            btn.classList.remove('following');

            btn.style.background = 'rgba(255,255,255,0.1)';
            btn.style.borderColor = 'transparent';
            btn.style.color = 'var(--text-main)';

            if (btn.classList.contains('create-btn-sidebar')) {
                btn.textContent = "Follow";
                btn.style.background = "var(--primary)";
                btn.style.color = "black";
                btn.style.borderColor = "var(--primary)";
            }
        }
    });
}

function updateFollowerCountCache(uid, previousState, nextState) {
    if (previousState === nextState) return;
    const delta = nextState ? 1 : -1;
    const countEl = document.getElementById(`profile-follower-count-${uid}`);

    if (userCache[uid]) {
        const currentCount = userCache[uid].followersCount || 0;
        userCache[uid].followersCount = Math.max(0, currentCount + delta);
    }

    if (countEl) {
        const rendered = parseInt(countEl.textContent || '0', 10) || 0;
        const updated = Math.max(0, rendered + delta);
        countEl.textContent = updated;
    }
}

async function persistFollowChange(uid, wasFollowing) {
    const targetUserRef = doc(db, 'users', uid);
    const currentUserRef = doc(db, 'users', currentUser.uid);
    const followerRef = doc(db, 'users', uid, 'followers', currentUser.uid);
    const followingRef = doc(db, 'users', currentUser.uid, 'following', uid);

    let finalState = wasFollowing;

    /* Dev verification checklist (follow systems):
     * 1) Follow a user via "+ User" on a post -> navigate -> reload -> still followed.
     * 2) Unfollow the same user -> reload -> remains unfollowed.
     * 3) Follow a topic via Discover -> top bar updates immediately and survives reload.
     * 4) Unfollow a topic -> top bar removes it and state persists across reloads.
     * 5) Switching For You vs Following retains follow state and Following reflects the followed set.
     * 6) One click = one write (no duplicate toggles/writes).
     * 7) No console errors during follow/unfollow flows.
     */
    await runTransaction(db, async function (tx) {
        const followerDoc = await tx.get(followerRef);
        const currentlyFollowing = followerDoc.exists();
        const followingUpdate = currentlyFollowing ? arrayRemove(uid) : arrayUnion(uid);

        if (currentlyFollowing) {
            tx.delete(followerRef);
            tx.delete(followingRef);
            tx.set(targetUserRef, { followersCount: increment(-1) }, { merge: true });
            tx.set(currentUserRef, { following: followingUpdate, followingUsers: followingUpdate, followingCount: increment(-1) }, { merge: true });
            finalState = false;
        } else {
            tx.set(followerRef, { followedAt: serverTimestamp() }, { merge: true });
            tx.set(followingRef, { followedAt: serverTimestamp() }, { merge: true });
            tx.set(targetUserRef, { followersCount: increment(1) }, { merge: true });
            tx.set(currentUserRef, { following: followingUpdate, followingUsers: followingUpdate, followingCount: increment(1) }, { merge: true });
            finalState = true;
        }
    });

    return finalState;
}

function normalizeFollowedTopicsFromProfile(data = {}) {
    const ordered = Array.isArray(data.followedCategories) ? data.followedCategories : [];
    const fallbacks = [];
    if (Array.isArray(data.followingTopics)) fallbacks.push(...data.followingTopics);
    if (Array.isArray(data.followedTopics)) fallbacks.push(...data.followedTopics);

    const seen = new Set();
    const normalized = [];
    const pushUnique = function (value) {
        const topic = typeof value === 'string' ? value.trim() : '';
        if (topic && !seen.has(topic)) {
            seen.add(topic);
            normalized.push(topic);
        }
    };

    ordered.forEach(pushUnique);
    fallbacks.forEach(pushUnique);
    return normalized;
}

function applyFollowedCategoryList(list = []) {
    const nextList = [];
    const seen = new Set();
    (list || []).forEach(function (name) {
        const normalized = typeof name === 'string' ? name.trim() : '';
        if (normalized && !seen.has(normalized)) {
            seen.add(normalized);
            nextList.push(normalized);
        }
    });

    followedCategoryList = nextList;
    followedCategories = new Set(nextList);
    userProfile.followedCategories = nextList;
    syncTopicFollowButtons();
    renderCategoryPills();
    if (currentCategory === 'Following') renderFeed();
}

function syncSavedVideosFromProfile(profile = userProfile) {
    const list = Array.isArray(profile.savedVideos) ? profile.savedVideos : [];
    videoEngagementState.saved = new Set(list);
}

function getTopicButtons(topic) {
    const matches = [];
    const cleanTopic = topic.replace(/[^a-zA-Z0-9]/g, '');

    document.querySelectorAll('[data-topic]').forEach(function (btn) {
        if (btn.getAttribute('data-topic') === topic) matches.push(btn);
    });

    document.querySelectorAll(`.js-follow-topic-${cleanTopic}`).forEach(function (btn) {
        if (!matches.includes(btn)) matches.push(btn);
    });

    return matches;
}

function updateTopicFollowButtons(topic, isFollowing) {
    const buttons = getTopicButtons(topic);
    buttons.forEach(function (btn) {
        if (isFollowing) {
            btn.innerHTML = 'Following';
            btn.classList.add('following');
        } else {
            btn.innerHTML = '<i class="ph-bold ph-plus"></i> Topic';
            btn.classList.remove('following');
        }
    });
}

function syncTopicFollowButtons() {
    document.querySelectorAll('[data-topic]').forEach(function (btn) {
        const topic = btn.getAttribute('data-topic');
        const isFollowing = followedCategories.has(topic);
        if (isFollowing) {
            btn.innerHTML = 'Following';
            btn.classList.add('following');
        } else {
            btn.innerHTML = '<i class="ph-bold ph-plus"></i> Topic';
            btn.classList.remove('following');
        }
    });
}

async function hydrateFollowedCategories(uid, profileData = {}) {
    if (!uid) return;

    if (followedTopicsUnsubscribe) {
        try { followedTopicsUnsubscribe(); } catch (err) { console.warn('Topic follow unsubscribe failed', err); }
        followedTopicsUnsubscribe = null;
    }

    const seeded = normalizeFollowedTopicsFromProfile(profileData);
    applyFollowedCategoryList(seeded);

    try {
        const userRef = doc(db, 'users', uid);
        followedTopicsUnsubscribe = onSnapshot(userRef, function (snap) {
            if (!snap.exists()) return;
            const next = normalizeFollowedTopicsFromProfile(snap.data());
            applyFollowedCategoryList(next);
        });
    } catch (err) {
        console.error('Unable to subscribe to followed topics', err);
    }
}

window.toggleFollow = async function (c, event) {
    if (event) event.stopPropagation();
    if (!currentUser || !currentUser.uid) return toast('Please sign in to follow topics.', 'info');

    const topic = (c || '').trim();
    if (!topic) return;

    const wasFollowing = followedCategories.has(topic);
    updateTopicFollowButtons(topic, !wasFollowing);

    try {
        const update = wasFollowing ? arrayRemove(topic) : arrayUnion(topic);
        await setDoc(doc(db, 'users', currentUser.uid), { followedCategories: update, followingTopics: update }, { merge: true });

        const updatedList = wasFollowing
            ? followedCategoryList.filter(function (name) { return name !== topic; })
            : [topic, ...followedCategoryList];

        applyFollowedCategoryList(updatedList);
        if (currentCategory === 'Following') renderFeed();
    } catch (e) {
        console.error('Topic follow action failed', e);
        toast('Could not update topic follow. Please try again.', 'error');
        updateTopicFollowButtons(topic, wasFollowing);
    }
};

window.toggleFollowUser = async function (uid, event) {
    if (event) event.stopPropagation();
    if (!currentUser || !currentUser.uid) return toast('Please sign in to follow users.', 'info');

    const previousState = followedUsers.has(uid);

    try {
        const finalState = await persistFollowChange(uid, previousState);
        if (finalState) followedUsers.add(uid); else followedUsers.delete(uid);
        updateFollowButtonsForUser(uid, finalState);
        updateFollowerCountCache(uid, previousState, finalState);
    } catch (e) {
        console.error('Follow action failed', e);
        toast('Could not update follow status. Please try again.', 'error');
        updateFollowButtonsForUser(uid, previousState);
    }
};

function normalizeFollowingArray(data = {}) {
    const merged = [];
    if (Array.isArray(data.following)) merged.push(...data.following);
    if (Array.isArray(data.followingUsers)) merged.push(...data.followingUsers);
    const unique = [];
    const seen = new Set();
    merged.forEach(function (id) {
        const normalized = typeof id === 'string' ? id.trim() : '';
        if (normalized && !seen.has(normalized)) {
            seen.add(normalized);
            unique.push(normalized);
        }
    });
    return unique;
}

function applyFollowingUsersList(list = []) {
    const next = new Set();
    (list || []).forEach(function (id) {
        if (typeof id === 'string' && id.trim()) next.add(id.trim());
    });
    followedUsers = next;
    userProfile.following = Array.from(followedUsers);
    if (currentUser?.uid && userCache[currentUser.uid]) userCache[currentUser.uid].following = userProfile.following;
    syncFollowButtonsForKnownUsers();
}

function syncFollowButtonsForKnownUsers() {
    document.querySelectorAll('[class*="js-follow-user-"]').forEach(function (btn) {
        const match = Array.from(btn.classList).find(function (cls) { return cls.startsWith('js-follow-user-'); });
        if (!match) return;
        const uid = match.replace('js-follow-user-', '');
        updateFollowButtonsForUser(uid, followedUsers.has(uid));
    });
}

async function hydrateFollowingState(uid, profileData = {}) {
    if (followingUnsubscribe) {
        try { followingUnsubscribe(); } catch (err) { }
        followingUnsubscribe = null;
    }

    applyFollowingUsersList(normalizeFollowingArray(profileData));

    try {
        const docSnap = await getDoc(doc(db, 'users', uid));
        if (docSnap.exists()) {
            applyFollowingUsersList(normalizeFollowingArray(docSnap.data()));
        }
    } catch (err) {
        console.warn('Unable to hydrate following array from profile', err);
    }

    try {
        const followingRef = collection(db, 'users', uid, 'following');
        followingUnsubscribe = onSnapshot(followingRef, function (snap) {
            const ids = [];
            snap.forEach(function (docSnap) { ids.push(docSnap.id); });
            const merged = [...normalizeFollowingArray(userProfile), ...ids];
            applyFollowingUsersList(merged);
        });
    } catch (err) {
        console.error('Unable to refresh following list', err);
    }
}

// --- Render Logic (The Core) ---
function getPostHTML(post, options = {}) {
    try {
        const animateIn = options.animate !== false;
        const date = formatDateTime(post.timestamp) || 'Just now';
        const viewerUid = getViewerUid();

        let authorData = userCache[post.userId] || { name: post.author, username: "loading...", photoURL: null };
        if (!authorData.name) authorData.name = "Unknown User";

        const verifiedBadge = renderVerifiedBadge(authorData);

        const avatarHtml = renderAvatar({ ...authorData, uid: post.userId }, { size: 42 });

        const isLiked = viewerUid ? (post.likedBy && post.likedBy.includes(viewerUid)) : false;
        const isDisliked = viewerUid ? (post.dislikedBy && post.dislikedBy.includes(viewerUid)) : false;
        const isSaved = viewerUid ? (userProfile.savedPosts && userProfile.savedPosts.includes(post.id)) : false;
        const isSelfPost = viewerUid ? post.userId === viewerUid : false;
        const isFollowingUser = followedUsers.has(post.userId);
        const isFollowingTopic = followedCategories.has(post.category);
        const topicClass = post.category.replace(/[^a-zA-Z0-9]/g, '');

        const followButtons = isSelfPost ? '' : `
                                <button class="follow-btn js-follow-user-${post.userId} ${isFollowingUser ? 'following' : ''}" onclick="event.stopPropagation(); window.toggleFollowUser('${post.userId}', event)" style="font-size:0.65rem; padding:2px 8px;">${isFollowingUser ? 'Following' : '<i class="ph-bold ph-plus"></i> User'}</button>
                                <button class="follow-btn js-follow-topic-${topicClass} ${isFollowingTopic ? 'following' : ''}" data-topic="${escapeHtml(post.category)}" onclick="event.stopPropagation(); window.toggleFollow('${post.category}', event)" style="font-size:0.65rem; padding:2px 8px;">${isFollowingTopic ? 'Following' : '<i class="ph-bold ph-plus"></i> Topic'}</button>`;

        let trustBadge = "";
        if (post.trustScore > 2) {
            trustBadge = `<div style="font-size:0.75rem; color:#8b949e; display:flex; align-items:center; gap:7px; font-weight:600;"><i class="ph-fill ph-check-circle"></i> Publicly Verified</div>`;
        } else if (post.trustScore < -1) {
            trustBadge = `<div style="font-size:0.75rem; color:#ff3d3d; display:flex; align-items:center; gap:4px; font-weight:600;"><i class="ph-fill ph-warning-circle"></i> Disputed</div>`;
        }

        const postText = typeof post.content === 'object' && post.content !== null ? (post.content.text || '') : (post.content || '');
        const formattedBody = formatContent(postText, post.tags, post.mentions);
        const tagListHtml = renderTagList(post.tags || []);
        const pollBlock = renderPollBlock(post);
        const locationBadge = renderLocationBadge(post.location);
        const scheduledChip = isPostScheduledInFuture(post) && viewerUid && post.userId === viewerUid ? `<div class="scheduled-chip">Scheduled for ${formatTimestampDisplay(post.scheduledFor)}</div>` : '';
        const verification = getVerificationState(post);
        const verificationChip = verification ? `<span class="verification-chip ${verification.className}">${verification.label}</span>` : '';

        let mediaContent = '';
        if (post.mediaUrl) {
            if (post.type === 'video') {
                mediaContent = `<div class="video-container" onclick="window.openFullscreenMedia('${post.mediaUrl}', 'video')"><video src="${post.mediaUrl}" controls class="post-media"></video></div>`;
            } else {
                mediaContent = `<img src="${post.mediaUrl}" class="post-media" alt="Post Content" onclick="window.openFullscreenMedia('${post.mediaUrl}', 'image')">`;
            }
        }

        let commentPreviewHtml = '';
        if (post.previewComment) {
            commentPreviewHtml = `
                <div class="post-comment-preview" style="margin-top:10px; padding:8px; background:rgba(255,255,255,0.05); border-radius:8px; font-size:0.85rem; color:var(--text-muted); display:flex; gap:6px;">
                    <span style="font-weight:bold; color:var(--text-main);">${escapeHtml(post.previewComment.author)}:</span>
                    <span>${escapeHtml(post.previewComment.text)}</span>
                    ${post.previewComment.likes ? `<span style="margin-left:auto; font-size:0.75rem; display:flex; align-items:center; gap:3px;"><i class="ph-fill ph-thumbs-up"></i> ${post.previewComment.likes}</span>` : ''}
                </div>`;
        }

        let savedTagHtml = "";
        if (currentCategory === 'Saved') {
            const tag = (userProfile.savedTags && userProfile.savedTags[post.id]) || "";
            savedTagHtml = `<div style="margin-top:5px;"><button onclick="event.stopPropagation(); window.addTagToSaved('${post.id}')" style="background:var(--bg-hover); border:1px dashed var(--border); font-size:0.7rem; padding:2px 8px; border-radius:4px; color:var(--text-muted); cursor:pointer; display:flex; align-items:center; gap:4px;">${tag ? '<i class="ph-fill ph-tag"></i> ' + escapeHtml(tag) : '<i class="ph ph-plus"></i> Add Tag'}</button></div>`;
        }

        const myReview = window.myReviewCache ? window.myReviewCache[post.id] : null;
        const reviewDisplay = getReviewDisplay(myReview);

        const accentColor = THEMES[post.category] || THEMES[currentCategory] || THEMES['For You'];
        const mobileView = isMobileViewport();

        const animationClass = animateIn ? ' animate-in' : '';
        return `
            <div id="post-card-${post.id}" class="social-card${animationClass}" style="border-left: 2px solid var(--card-accent); --card-accent: ${accentColor};">
                <div class="card-header">
                    <div class="author-wrapper" onclick="window.openUserProfile('${post.userId}', event)">
                        ${avatarHtml}
                        <div class="header-info">
                            <div class="author-line"><span class="author-name">${escapeHtml(authorData.name)}</span>${verifiedBadge}</div>
                            <span class="post-meta">@${escapeHtml(authorData.username)} • ${date}</span>
                        </div>
                    </div>
                    <div style="flex:1; display:flex; flex-direction:column; align-items:flex-end; gap:6px;">
                        <div style="display:flex; gap:8px; align-items:center; justify-content:flex-end; width:100%;">
                            <div style="display:flex; gap:5px; flex-wrap:wrap; justify-content:flex-end;">${followButtons}</div>
                            ${getPostOptionsButton(post, 'feed')}
                        </div>
                        ${trustBadge}
                    </div>
                </div>
                <div class="card-content" onclick="window.openThread('${post.id}')">
                    <div class="category-badge">${post.category}</div>
                    ${verificationChip}
                    <h3 class="post-title">${escapeHtml(cleanText(post.title))}</h3>
                    <p class="post-body-text">${formattedBody}</p>
                    ${tagListHtml}
                    ${locationBadge}
                    ${scheduledChip}
                    ${pollBlock}
                    ${mediaContent}
                    ${commentPreviewHtml}
                    ${savedTagHtml}
                </div>
                ${renderPostActions(post, { isLiked, isDisliked, isSaved, reviewDisplay, showCounts: !mobileView, showLabels: !mobileView })}
            </div>`;
    } catch (e) {
        console.error("Error generating post HTML", e);
        return "";
    }
}

function insertScrollSentinel(container, sentinelId, offsetFromEnd = FEED_PREFETCH_OFFSET, options = {}) {
    if (!container) return null;
    const sentinel = document.createElement('div');
    sentinel.id = sentinelId;
    sentinel.className = 'scroll-sentinel';
    if (options.placeAfter && container.parentNode) {
        container.parentNode.insertBefore(sentinel, container.nextSibling);
        return sentinel;
    }
    const children = Array.from(container.children);
    if (children.length > offsetFromEnd) {
        const insertBeforeNode = children[children.length - offsetFromEnd];
        if (insertBeforeNode) {
            container.insertBefore(sentinel, insertBeforeNode);
            return sentinel;
        }
    }
    container.appendChild(sentinel);
    return sentinel;
}

function renderFeed(targetId = 'feed-content') {
    if (window.localStorage?.getItem('NEXERA_DEBUG_ROUTER') === '1') {
        window.__nexeraFeedRenderCount = (window.__nexeraFeedRenderCount || 0) + 1;
        console.log('[NexeraRouter] renderFeed', window.__nexeraFeedRenderCount);
    }
    const container = document.getElementById(targetId);
    if (!container) return;

    renderCategoryPills();
    container.innerHTML = "";

    if (feedLoading && allPosts.length === 0) {
        container.innerHTML = `<div class="empty-state"><p>Loading Posts...</p></div>`;
        return;
    }
    let displayPosts = allPosts.filter(function (post) {
        if (post.visibility === 'private') return currentUser && post.userId === currentUser.uid;
        return true;
    });

    displayPosts = displayPosts.filter(function (post) {
        if (isPostScheduledInFuture(post) && (!currentUser || post.userId !== currentUser.uid)) return false;
        return true;
    });

    if (currentCategory === 'Following') {
        displayPosts = allPosts.filter(function (post) { return followedCategories.has(post.category); });
    } else if (currentCategory === 'Saved') {
        displayPosts = allPosts.filter(function (post) { return userProfile.savedPosts && userProfile.savedPosts.includes(post.id); });
        if (savedSearchTerm) displayPosts = displayPosts.filter(function (post) { return post.title.toLowerCase().includes(savedSearchTerm); });
        if (savedFilter === 'Recent') displayPosts.sort(function (a, b) { return userProfile.savedPosts.indexOf(b.id) - userProfile.savedPosts.indexOf(a.id); });
        else if (savedFilter === 'Oldest') displayPosts.sort(function (a, b) { return userProfile.savedPosts.indexOf(a.id) - userProfile.savedPosts.indexOf(b.id); });
        else if (savedFilter === 'Videos') displayPosts = displayPosts.filter(function (p) { return p.type === 'video'; });
        else if (savedFilter === 'Images') displayPosts = displayPosts.filter(function (p) { return p.type === 'image'; });
    } else if (currentCategory !== 'For You') {
        displayPosts = allPosts.filter(function (post) { return post.category === currentCategory; });
    }

    const activeTypes = getActiveFeedTypes();
    const items = [];

    if (activeTypes.includes('threads')) {
        const threadItems = displayPosts
            .filter(function (post) { return matchesCategoryFilter(post.category); })
            .map(function (post) { return ({ type: 'threads', id: post.id, createdAt: getFeedItemTimestamp(post), data: post }); });
        items.push(...threadItems);
    }

    if (activeTypes.includes('videos') && currentCategory !== 'Saved') {
        const videoSource = (homeVideosCache && homeVideosCache.length) ? homeVideosCache : (videosCache || []);
        const videos = videoSource.filter(function (video) {
            return matchesCategoryFilter(video.category || video.genre || video.categoryLabel);
        }).map(function (video) {
            return ({ type: 'videos', id: video.id, createdAt: getFeedItemTimestamp(video), data: video });
        });
        items.push(...videos);
    }

    if (activeTypes.includes('livestreams') && currentCategory !== 'Saved') {
        const sessionSource = (homeLiveSessionsCache && homeLiveSessionsCache.length) ? homeLiveSessionsCache : (liveSessionsCache || []);
        const sessions = sessionSource.filter(function (session) {
            const status = (session.status || session.state || '').toString().toLowerCase();
            const isLive = session.isLive === true || status === 'live' || status === 'streaming' || session.endedAt == null;
            if (!isLive) return false;
            return matchesCategoryFilter(session.category || session.categoryLabel);
        }).map(function (session) {
            return ({ type: 'livestreams', id: session.id, createdAt: getFeedItemTimestamp(session), data: session });
        });
        items.push(...sessions);
    }

    if (currentCategory === 'For You') {
        items.sort(function (a, b) { return b.createdAt - a.createdAt; });
    } else {
        items.sort(function (a, b) { return b.createdAt - a.createdAt; });
    }

    if (items.length === 0) {
        const emptyLabel = feedLoading ? 'Loading Posts...' : 'No posts found.';
        container.innerHTML = `<div class="empty-state"><i class="ph ph-magnifying-glass" style="font-size:3rem; margin-bottom:1rem;"></i><p>${emptyLabel}</p></div>`;
        uiDebugLog('feed render empty', { activeTypes, count: 0 });
        return;
    }

    if (isUiDebugEnabled()) {
        const counts = items.reduce(function (acc, item) {
            acc[item.type] = (acc[item.type] || 0) + 1;
            return acc;
        }, {});
        uiDebugLog('feed render', { activeTypes, counts, total: items.length });
    }

    items.forEach(function (item) {
        let itemKey = `${item.type}:${item.id}`;
        if (item.type === 'videos') {
            itemKey = `video:${item.id}`;
        } else if (item.type === 'livestreams') {
            itemKey = `livestream:${item.id}`;
        }
        const animateIn = shouldAnimateItem(itemKey);
        if (item.type === 'threads') {
            container.insertAdjacentHTML('beforeend', getPostHTML(item.data, { animate: animateIn }));
            return;
        }
        if (item.type === 'videos') {
            const card = buildVideoCard(item.data);
            if (animateIn) card.classList.add('animate-in');
            container.appendChild(card);
            return;
        }
        if (item.type === 'livestreams') {
            const card = buildHomeLiveCard(item.data);
            if (animateIn) card.classList.add('animate-in');
            container.appendChild(card);
        }
    });

    displayPosts.forEach(function (post) {
        const reviewBtn = document.querySelector(`#post-card-${post.id} .review-action`);
        const reviewValue = window.myReviewCache ? window.myReviewCache[post.id] : null;
        applyReviewButtonState(reviewBtn, reviewValue);
    });

    applyMyReviewStylesToDOM();

    insertScrollSentinel(container, 'feed-scroll-sentinel');
    ensureFeedScrollObserver();
}

function ensureFeedScrollObserver() {
    const sentinel = document.getElementById('feed-scroll-sentinel');
    if (!sentinel || feedPagination.done) return;
    if (feedScrollObserver) {
        feedScrollObserver.disconnect();
    }
    feedScrollObserver = new IntersectionObserver(function (entries) {
        entries.forEach(function (entry) {
            if (entry.isIntersecting) {
                loadMoreFeedPosts();
            }
        });
    }, { rootMargin: FEED_PREFETCH_ROOT_MARGIN });
    feedScrollObserver.observe(sentinel);
}

async function loadMoreFeedPosts() {
    if (feedPagination.loading || feedPagination.done) return;
    try {
        const batch = await fetchFeedBatch();
        if (!batch.length) return;
        const existing = new Set(allPosts.map(function (post) { return post.id; }));
        batch.forEach(function (post) {
            if (!existing.has(post.id)) {
                allPosts.push(post);
                existing.add(post.id);
            }
        });
        allPosts.sort(function (a, b) { return (b.timestamp?.seconds || 0) - (a.timestamp?.seconds || 0); });
        fetchMissingProfiles(batch);
        renderFeed();
    } catch (error) {
        console.warn('Feed pagination failed', error);
    }
}


function refreshSinglePostUI(postId) {
    const post = allPosts.find(function (p) { return p.id === postId; });
    if (!post) return;
    const viewerUid = getViewerUid();

    const likeBtn = document.getElementById(`post-like-btn-${postId}`);
    const dislikeBtn = document.getElementById(`post-dislike-btn-${postId}`);
    const saveBtn = document.getElementById(`post-save-btn-${postId}`);
    const reviewBtn = document.querySelector(`#post-card-${postId} .review-action`);

    const isLiked = viewerUid ? (post.likedBy && post.likedBy.includes(viewerUid)) : false;
    const isDisliked = viewerUid ? (post.dislikedBy && post.dislikedBy.includes(viewerUid)) : false;
    const isSaved = viewerUid ? (userProfile.savedPosts && userProfile.savedPosts.includes(postId)) : false;
    const myReview = window.myReviewCache ? window.myReviewCache[postId] : null;

    function renderActionButton(btn, { iconClass, label, count = null, activeColor = 'inherit' }) {
        if (!btn) return;
        const showLabels = btn.dataset.showLabels !== 'false';
        const showCounts = btn.dataset.showCounts !== 'false';
        const icon = `<i class="${iconClass}" style="font-size:${btn.dataset.iconSize || '1.1rem'};"></i>`;
        const countMarkup = (!showLabels && showCounts && typeof count === 'number') ? `<span class="action-count">${count}</span>` : '';
        const labelMarkup = showLabels ? `<span class="action-label"> ${label}${showCounts && typeof count === 'number' ? ` ${count}` : ''}</span>` : '';
        const aria = showCounts && typeof count === 'number' ? `${label} (${count})` : label;
        btn.innerHTML = `${icon}${labelMarkup}${countMarkup}`;
        btn.style.color = activeColor;
        btn.setAttribute('aria-label', aria);
    }

    renderActionButton(likeBtn, { iconClass: `${isLiked ? 'ph-fill' : 'ph'} ph-thumbs-up`, label: 'Like', count: post.likes || 0, activeColor: isLiked ? '#00f2ea' : 'inherit' });
    renderActionButton(dislikeBtn, { iconClass: `${isDisliked ? 'ph-fill' : 'ph'} ph-thumbs-down`, label: 'Dislike', count: post.dislikes || 0, activeColor: isDisliked ? '#ff3d3d' : 'inherit' });
    renderActionButton(saveBtn, { iconClass: `${isSaved ? 'ph-fill' : 'ph'} ph-bookmark-simple`, label: isSaved ? 'Saved' : 'Save', activeColor: isSaved ? '#00f2ea' : 'inherit' });
    if (reviewBtn) {
        applyReviewButtonState(reviewBtn, myReview);
    }

    document.querySelectorAll(`[data-post-id="${postId}"]`).forEach(function (btn) {
        const action = btn.dataset.action;
        if (action === 'like') {
            renderActionButton(btn, { iconClass: `${isLiked ? 'ph-fill' : 'ph'} ph-thumbs-up`, label: 'Like', count: post.likes || 0, activeColor: isLiked ? '#00f2ea' : 'inherit' });
        } else if (action === 'dislike') {
            renderActionButton(btn, { iconClass: `${isDisliked ? 'ph-fill' : 'ph'} ph-thumbs-down`, label: 'Dislike', count: post.dislikes || 0, activeColor: isDisliked ? '#ff3d3d' : 'inherit' });
        } else if (action === 'save') {
            renderActionButton(btn, { iconClass: `${isSaved ? 'ph-fill' : 'ph'} ph-bookmark-simple`, label: isSaved ? 'Saved' : 'Save', activeColor: isSaved ? '#00f2ea' : 'inherit' });
        } else if (action === 'review') {
            applyReviewButtonState(btn, myReview);
        }
    });

    // Update Thread View if active
    const threadLikeBtn = document.getElementById(`thread-like-btn-${postId}`);
    const threadDislikeBtn = document.getElementById(`thread-dislike-btn-${postId}`);
    const threadSaveBtn = document.getElementById(`thread-save-btn-${postId}`);
    const threadTitle = document.getElementById('thread-view-title');
    const threadReviewBtn = document.getElementById(`thread-review-btn-${postId}`);
    const threadSaveCount = post.saveCount ?? post.saves ?? null;

    function updateThreadAction(btn, { iconClass, label, count = null, color = 'inherit' }) {
        if (!btn) return;
        const showLabels = btn.dataset.showLabels !== 'false';
        const showCounts = btn.dataset.showCounts !== 'false';
        const icon = `<i class="${iconClass}" style="font-size:${btn.dataset.iconSize || '1rem'};"></i>`;
        const countMarkup = (!showLabels && showCounts && typeof count === 'number') ? `<span class="action-count">${count}</span>` : '';
        const labelMarkup = showLabels ? `<span class="action-label"> ${label}${showCounts && typeof count === 'number' ? ` ${count}` : ''}</span>` : '';
        const aria = showCounts && typeof count === 'number' ? `${label} (${count})` : label;
        btn.innerHTML = `${icon}${labelMarkup}${countMarkup}`;
        btn.style.color = color;
        btn.setAttribute('aria-label', aria);
    }

    if (threadTitle && threadTitle.dataset.postId === postId) {
        updateThreadAction(threadLikeBtn, { iconClass: `${isLiked ? 'ph-fill' : 'ph'} ph-thumbs-up`, label: 'Like', count: post.likes || 0, color: isLiked ? '#00f2ea' : 'inherit' });
        updateThreadAction(threadDislikeBtn, { iconClass: `${isDisliked ? 'ph-fill' : 'ph'} ph-thumbs-down`, label: 'Dislike', count: post.dislikes || 0, color: isDisliked ? '#ff3d3d' : 'inherit' });
        updateThreadAction(threadSaveBtn, { iconClass: `${isSaved ? 'ph-fill' : 'ph'} ph-bookmark-simple`, label: isSaved ? 'Saved' : 'Save', count: threadSaveCount, color: isSaved ? '#00f2ea' : 'inherit' });
        if (threadReviewBtn) {
            applyReviewButtonState(threadReviewBtn, myReview);
        }
    }
}

// --- Interaction Functions ---
window.toggleLike = async function (postId, event) {
    if (event) event.stopPropagation();
    if (!currentUser) return alert("Please log in to like posts.");

    const post = allPosts.find(function (p) { return p.id === postId; });
    if (!post) return;

    const wasLiked = post.likedBy && post.likedBy.includes(currentUser.uid);
    const hadDisliked = post.dislikedBy && post.dislikedBy.includes(currentUser.uid);

    // Optimistic Update
    if (wasLiked) {
        post.likes = Math.max(0, (post.likes || 0) - 1); // Prevent negative likes
        post.likedBy = post.likedBy.filter(function (uid) { return uid !== currentUser.uid; });
    } else {
        post.likes = (post.likes || 0) + 1;
        if (!post.likedBy) post.likedBy = [];
        post.likedBy.push(currentUser.uid);
        if (hadDisliked) {
            post.dislikes = Math.max(0, (post.dislikes || 0) - 1);
            post.dislikedBy = (post.dislikedBy || []).filter(function (uid) { return uid !== currentUser.uid; });
        }
    }

    recordTagAffinity(post.tags, wasLiked ? -1 : 1);

    refreshSinglePostUI(postId);
    const postRef = doc(db, 'posts', postId);

    try {
        if (wasLiked) {
            await updateDoc(postRef, { likes: increment(-1), likedBy: arrayRemove(currentUser.uid) });
        } else {
            const updatePayload = { likes: increment(1), likedBy: arrayUnion(currentUser.uid) };
            if (hadDisliked) {
                updatePayload.dislikes = increment(-1);
                updatePayload.dislikedBy = arrayRemove(currentUser.uid);
            }
            await updateDoc(postRef, updatePayload);
        }
    } catch (e) {
        console.error("Like error:", e);
        // Revert on error would go here, or just reload on demand
        loadFeedData();
    }
}

window.toggleSave = async function (postId, event) {
    if (event) event.stopPropagation();
    if (!currentUser) return alert("Please log in to save posts.");

    const isSaved = userProfile.savedPosts && userProfile.savedPosts.includes(postId);

    // Optimistic Update
    if (isSaved) {
        userProfile.savedPosts = userProfile.savedPosts.filter(function (id) { return id !== postId; });
    } else {
        userProfile.savedPosts.push(postId);
    }
    storeUserInCache(currentUser.uid, userProfile);

    refreshSinglePostUI(postId);

    const userRef = doc(db, 'users', currentUser.uid);
    try {
        if (isSaved) await updateDoc(userRef, { savedPosts: arrayRemove(postId) });
        else await updateDoc(userRef, { savedPosts: arrayUnion(postId) });
    } catch (e) { console.error("Save error:", e); }
}

// --- Creation & Upload ---
async function uploadFileToStorage(file, path) {
    if (!file) return null;
    const storageRef = ref(storage, path);
    await uploadBytes(storageRef, file);
    return await getDownloadURL(storageRef);
}

function normalizeTagValue(tag = '') {
    return tag.trim().replace(/^#/, '').toLowerCase().replace(/[^\w]/g, '');
}

function buildTagDisplayName(normalized = '') {
    return normalized ? `#${normalized}` : '';
}

function getHashtagQuery(inputValue = '') {
    const trimmed = (inputValue || '').trim();
    if (!trimmed.startsWith('#')) return '';
    if (trimmed.length <= 1) return '';
    return normalizeTagValue(trimmed);
}

function rankTagSuggestions(knownTags = [], query = '', selectedTags = []) {
    const seen = new Set(selectedTags);
    const startsWith = [];
    const contains = [];
    knownTags.forEach(function (tag) {
        if (!query || seen.has(tag)) return;
        if (tag.startsWith(query)) {
            startsWith.push(tag);
        } else if (tag.includes(query)) {
            contains.push(tag);
        }
    });
    return startsWith.concat(contains).slice(0, 5);
}

function ensurePollOptionSlots() {
    if (!Array.isArray(composerPoll.options)) composerPoll.options = ['', ''];
    while (composerPoll.options.length < 2) composerPoll.options.push('');
    if (composerPoll.options.length > 5) composerPoll.options = composerPoll.options.slice(0, 5);
}

function renderPollOptions() {
    ensurePollOptionSlots();
    const container = document.getElementById('poll-options-container');
    if (!container) return;
    container.innerHTML = '';
    composerPoll.options.forEach(function (opt, idx) {
        const row = document.createElement('div');
        row.className = 'poll-option-row';
        row.innerHTML = `
            <input type="text" class="form-input" value="${escapeHtml(opt)}" placeholder="Option ${idx + 1}" oninput="window.handlePollOptionInput(${idx}, this.value)">
            <button type="button" class="icon-pill" ${composerPoll.options.length <= 2 ? 'disabled' : ''} onclick="window.removePollOption(${idx})"><i class="ph ph-x"></i></button>
        `;
        container.appendChild(row);
    });
}

function handlePollOptionInput(index, value) {
    ensurePollOptionSlots();
    if (index < 0 || index >= composerPoll.options.length) return;
    composerPoll.options[index] = value;
}

function handlePollTitleChange(value) {
    composerPoll.title = value;
}

function addPollOption() {
    ensurePollOptionSlots();
    if (composerPoll.options.length >= 5) return toast('Polls can have up to 5 options.', 'info');
    composerPoll.options.push('');
    renderPollOptions();
}

function removePollOption(index) {
    ensurePollOptionSlots();
    if (composerPoll.options.length <= 2) return;
    composerPoll.options.splice(index, 1);
    renderPollOptions();
}

function handleScheduleChange(value = '') {
    composerScheduledFor = value;
}

function setComposerLocation(value = '') {
    composerLocation = value;
    const input = document.getElementById('location-input');
    if (input) input.value = value;
    renderLocationSuggestions(value);
}

function handleLocationInput(value = '') {
    setComposerLocation(value);
}

function renderLocationSuggestions(queryText = '') {
    const listEl = document.getElementById('location-suggestions');
    if (!listEl) return;
    const cleaned = (queryText || '').toLowerCase();
    const source = Array.isArray(recentLocations) ? recentLocations : [];
    const matches = source.filter(function (loc) { return !cleaned || loc.toLowerCase().includes(cleaned); }).slice(0, 5);
    if (!matches.length) {
        listEl.style.display = 'none';
        listEl.innerHTML = '';
        return;
    }
    listEl.style.display = 'block';
    listEl.innerHTML = matches.map(function (loc) {
        return `<button type="button" class="suggestion-chip" onclick="window.setComposerLocation('${escapeHtml(loc)}')"><i class=\"ph ph-map-pin\"></i> ${escapeHtml(loc)}</button>`;
    }).join('');
}

function resetComposerState() {
    composerTags = [];
    composerCreatedTags = new Set();
    composerMentions = [];
    composerPoll = { title: '', options: ['', ''] };
    composerScheduledFor = '';
    composerLocation = '';
    currentEditPost = null;
    renderComposerTags();
    renderComposerMentions();
    renderPollOptions();
    updateComposerTagLimit(false);
    composerNewTagNotice = '';
    updateComposerTagHelper('');
    const scheduleInput = document.getElementById('schedule-input');
    if (scheduleInput) scheduleInput.value = '';
    setComposerLocation('');
    const title = document.getElementById('postTitle');
    const content = document.getElementById('postContent');
    if (title) title.value = '';
    if (content) content.value = '';
    const fileInput = document.getElementById('postFile');
    if (fileInput) fileInput.value = '';
    clearPostImage();
    syncComposerMode();
}

function syncComposerMode() {
    const btn = document.getElementById('publishBtn');
    if (btn) btn.textContent = currentEditPost ? 'Save Changes' : 'Post';
}

function getKnownTags() {
    const collected = new Set(tagSuggestionPool);
    allPosts.forEach(function (post) {
        (post.tags || []).forEach(function (tag) { collected.add(normalizeTagValue(tag)); });
    });
    return Array.from(collected).filter(Boolean);
}

async function fetchTagSuggestions(prefix = '') {
    const cleaned = normalizeTagValue(prefix);
    if (!cleaned) return [];
    const prefixLower = buildTagDisplayName(cleaned).toLowerCase();
    const tagQuery = query(
        collection(db, 'tags'),
        orderBy('nameLower'),
        startAt(prefixLower),
        endAt(prefixLower + '\uf8ff'),
        limit(5)
    );
    const snap = await getDocs(tagQuery);
    return snap.docs.map(function (docSnap) {
        const data = docSnap.data() || {};
        const rawName = data.name || docSnap.id || '';
        return normalizeTagValue(rawName);
    }).filter(Boolean);
}

async function ensureTagDocument(normalizedTag = '') {
    const tagId = normalizeTagValue(normalizedTag);
    if (!tagId) return { created: false };
    const tagRef = doc(db, 'tags', tagId);
    const snap = await getDoc(tagRef);
    if (snap.exists()) {
        return { created: false };
    }
    const name = buildTagDisplayName(tagId);
    await setDoc(tagRef, {
        name,
        nameLower: name.toLowerCase(),
        uses: 1,
        createdAt: serverTimestamp()
    }, { merge: false });
    composerCreatedTags.add(tagId);
    return { created: true };
}

async function incrementTagUses(tags = []) {
    const updates = (tags || [])
        .map(function (tag) { return normalizeTagValue(tag); })
        .filter(Boolean)
        .filter(function (tag) { return !composerCreatedTags.has(tag); })
        .map(function (tag) {
            const name = buildTagDisplayName(tag);
            return setDoc(doc(db, 'tags', tag), {
                name,
                nameLower: name.toLowerCase(),
                uses: increment(1)
            }, { merge: true });
        });
    if (!updates.length) return;
    try {
        await Promise.all(updates);
    } catch (err) {
        console.warn('Tag use increment failed', err);
    }
}

function addComposerTag(tag = '') {
    const normalized = normalizeTagValue(tag);
    if (!normalized) return;
    if (composerTags.length >= 10) {
        updateComposerTagLimit(true);
        return;
    }
    if (composerTags.includes(normalized)) return;
    updateComposerTagLimit(false);
    composerTags.push(normalized);
    renderComposerTags();
    composerNewTagNotice = '';
    const input = document.getElementById('tag-input');
    if (input) {
        input.focus();
        input.setSelectionRange(input.value.length, input.value.length);
    }
}

function removeComposerTag(tag = '') {
    composerTags = composerTags.filter(function (t) { return t !== tag; });
    renderComposerTags();
    updateComposerTagLimit(false);
}

function renderComposerTags() {
    const container = document.getElementById('composer-tags-list');
    if (!container) return;
    if (!composerTags.length) {
        container.innerHTML = '<div class="empty-chip">No tags added</div>';
        return;
    }
    container.innerHTML = composerTags.map(function (tag) {
        return `<span class="tag-chip filled">#${escapeHtml(tag)} <button type="button" class="chip-remove" onclick="window.removeComposerTag('${tag}')">&times;</button></span>`;
    }).join('');
}

async function filterTagSuggestions(queryText = '') {
    const listEl = document.getElementById('tag-suggestions');
    if (!listEl) return;
    // Only show suggestions after a valid hashtag prefix (# + 1 char).
    const cleaned = getHashtagQuery(queryText);
    if (!cleaned) {
        listEl.innerHTML = '';
        listEl.style.display = 'none';
        updateComposerTagHelper(queryText, cleaned);
        return;
    }
    const token = ++tagSuggestionState.token;
    tagSuggestionState.query = cleaned;
    tagSuggestionState.loading = true;
    try {
        const matches = await fetchTagSuggestions(cleaned);
        if (token !== tagSuggestionState.token) return;
        if (!matches.length) {
            listEl.innerHTML = '';
            listEl.style.display = 'none';
        } else {
            listEl.style.display = 'block';
            listEl.innerHTML = matches.map(function (tag) {
                return `<button type="button" class="suggestion-chip" onmousedown="event.preventDefault()" onclick="window.selectTagSuggestion('${tag}')">#${escapeHtml(tag)}</button>`;
            }).join('');
        }
    } catch (err) {
        console.warn('Tag suggestion lookup failed', err);
        listEl.innerHTML = '';
        listEl.style.display = 'none';
    } finally {
        if (token === tagSuggestionState.token) {
            tagSuggestionState.loading = false;
        }
    }
    updateComposerTagHelper(queryText, cleaned);
}

function updateComposerTagHelper(rawValue = '', cleaned = '') {
    const helper = document.getElementById('tag-helper-text');
    if (!helper) return;
    if (composerNewTagNotice) {
        helper.textContent = composerNewTagNotice;
        helper.style.display = 'block';
        return;
    }
    helper.textContent = '';
    helper.style.display = 'none';
}

function updateComposerTagLimit(show) {
    const note = document.getElementById('tag-limit-note');
    if (!note) return;
    note.style.display = show ? 'block' : 'none';
}

function toggleTagInput(show) {
    const row = document.getElementById('tag-input-row');
    if (!row) return;
    const nextState = show !== undefined ? show : row.style.display !== 'flex';
    row.style.display = nextState ? 'flex' : 'none';
    if (nextState) {
        const input = document.getElementById('tag-input');
        if (input) {
            input.focus();
            filterTagSuggestions(input.value);
        }
        updateComposerTagLimit(false);
        composerNewTagNotice = '';
    }
}

window.selectTagSuggestion = function (tag = '') {
    const input = document.getElementById('tag-input');
    if (!input) return;
    const normalized = normalizeTagValue(tag);
    if (!normalized) return;
    input.value = buildTagDisplayName(normalized);
    input.focus();
    filterTagSuggestions(input.value);
};

async function handleTagInputKey(event) {
    if (event.key === 'Enter') {
        event.preventDefault();
        const input = event.target;
        const raw = input.value || '';
        const query = getHashtagQuery(raw);
        if (!query) {
            filterTagSuggestions(raw);
            return;
        }
        if (composerTags.length >= 10) {
            updateComposerTagLimit(true);
            return;
        }
        if (composerTags.includes(query)) {
            input.value = '';
            updateComposerTagLimit(false);
            filterTagSuggestions('');
            return;
        }
        composerNewTagNotice = '';
        try {
            const tagRef = doc(db, 'tags', query);
            const snap = await getDoc(tagRef);
            if (!snap.exists()) {
                composerNewTagNotice = 'Creating new tag...';
                updateComposerTagHelper(raw, query);
                await ensureTagDocument(query);
            }
        } catch (err) {
            console.warn('Tag creation failed', err);
        }
        addComposerTag(query);
        composerNewTagNotice = '';
        input.value = '';
        updateComposerTagHelper(raw, query);
        filterTagSuggestions('');
    } else {
        composerNewTagNotice = '';
        filterTagSuggestions(event.target.value || '');
    }
}

function normalizeMentionEntry(raw = {}) {
    if (typeof raw === 'string') {
        const username = raw.replace(/^@/, '').toLowerCase();
        return {
            username,
            uid: raw.uid || null,
            photoURL: raw.photoURL || '',
            avatarColor: raw.avatarColor || raw.profileColor || raw.color || computeAvatarColor(raw.uid || username || 'user')
        };
    }
    const username = (raw.username || raw.handle || '').replace(/^@/, '').toLowerCase();
    const displayName = raw.displayName || raw.nickname || raw.name || '';
    const uid = raw.uid || raw.userId || null;
    const accountRoles = Array.isArray(raw.accountRoles) ? raw.accountRoles : (raw.role ? [raw.role] : []);
    const verified = accountRoles.includes('verified');
    const avatarColor = raw.avatarColor || raw.profileColor || raw.color || computeAvatarColor(uid || username || 'user');
    return { username, uid, displayName, photoURL: raw.photoURL || '', avatarColor, accountRoles, verified };
}

function addComposerMention(rawUser) {
    const normalized = normalizeMentionEntry(rawUser);
    if (!normalized.username) return;
    if (composerMentions.some(function (m) { return m.username === normalized.username; })) return;
    composerMentions.push(normalized);
    renderComposerMentions();
    const input = document.getElementById('mention-input');
    if (input) {
        input.focus();
        input.setSelectionRange(input.value.length, input.value.length);
    }
}

function removeComposerMention(username) {
    composerMentions = composerMentions.filter(function (m) { return m.username !== username; });
    renderComposerMentions();
}

function renderComposerMentions() {
    const container = document.getElementById('composer-mentions-list');
    if (!container) return;
    if (!composerMentions.length) {
        container.innerHTML = '<div class="empty-chip">No mentions added</div>';
        return;
    }
    container.innerHTML = composerMentions.map(function (mention) {
        const avatar = renderAvatar({ ...mention, name: mention.displayName || mention.username }, { size: 36, className: 'mention-avatar' });
        const badge = renderVerifiedBadge(mention, 'with-gap');
        return `<div class="mention-card">${avatar}<div class="mention-meta"><div class="mention-name">${escapeHtml(mention.displayName || mention.username)}${badge}</div><div class="mention-handle">@${escapeHtml(mention.username)}</div></div><button type="button" class="chip-remove" onclick="window.removeComposerMention('${mention.username}')">&times;</button></div>`;
    }).join('');
}

function renderMentionSuggestionsList(listEl) {
    if (!listEl) return;
    if (!mentionSuggestionState.results.length) {
        listEl.innerHTML = '';
        listEl.style.display = 'none';
        return;
    }
    listEl.style.display = 'flex';
    const visible = mentionSuggestionState.results.slice(0, mentionSuggestionState.visibleCount);
    listEl.innerHTML = visible.map(function (profile) {
        const avatar = renderAvatar({ ...profile, uid: profile.id || profile.uid }, { size: 28 });
        return `<button type="button" class="mention-suggestion" onmousedown="event.preventDefault()" onclick='window.addComposerMention(${JSON.stringify({
            uid: profile.id || profile.uid,
            username: profile.username,
            displayName: profile.name || profile.nickname || profile.displayName || '',
            accountRoles: profile.accountRoles || [],
            photoURL: profile.photoURL || '',
            avatarColor: profile.avatarColor || '',
            followersCount: profile.followersCount || profile.followerCount || 0
        }).replace(/'/g, "&apos;")})'>
            ${avatar}
            <div class="mention-suggestion-meta">
                <div class="mention-name">${escapeHtml(profile.name || profile.nickname || profile.displayName || profile.username)}</div>
                <div class="mention-handle">@${escapeHtml(profile.username || '')}</div>
                <div class="mention-handle">${formatCompactNumber(profile.followersCount || profile.followerCount || 0)} followers</div>
            </div>
        </button>`;
    }).join('');
}

async function searchMentionSuggestions(term = '') {
    const listEl = document.getElementById('mention-suggestions');
    if (!listEl) return;
    listEl.classList.add('mention-suggestions-list');
    listEl.classList.remove('suggestion-row');
    const raw = (term || '').trim();
    if (!raw.startsWith('@') || raw.length <= 1) {
        mentionSuggestionState = { query: '', lastDoc: null, hasMore: false, loading: false, results: [], visibleCount: 5 };
        listEl.innerHTML = '';
        listEl.style.display = 'none';
        return;
    }
    const cleaned = raw.replace(/^@/, '').toLowerCase();
    const isNewQuery = cleaned !== mentionSuggestionState.query;
    if (isNewQuery) {
        mentionSuggestionState = { query: cleaned, lastDoc: null, hasMore: true, loading: false, results: [], visibleCount: 5 };
    }
    if (mentionSuggestionState.loading) return;
    if (!mentionSuggestionState.hasMore) return;
    mentionSuggestionState.loading = true;
    try {
        const buildQuery = function (field) {
            return query(
                collection(db, 'users'),
                orderBy(field),
                startAt(cleaned),
                endAt(cleaned + '\uf8ff'),
                ...(mentionSuggestionState.lastDoc ? [startAfter(mentionSuggestionState.lastDoc)] : []),
                limit(50)
            );
        };
        let snap = null;
        try {
            snap = await getDocs(buildQuery('usernameLower'));
        } catch (err) {
            snap = await getDocs(buildQuery('username'));
        }
        mentionSuggestionState.lastDoc = snap.docs[snap.docs.length - 1] || mentionSuggestionState.lastDoc;
        mentionSuggestionState.hasMore = snap.docs.length === 50;
        const batch = snap.docs.map(function (d) {
            const data = normalizeUserProfileData(d.data(), d.id);
            const followersCount = data.followersCount || data.followerCount || 0;
            return { id: d.id, uid: d.id, followersCount, ...data };
        });
        const merged = mentionSuggestionState.results.concat(batch).reduce(function (acc, item) {
            if (!acc.some(function (existing) { return existing.uid === item.uid; })) acc.push(item);
            return acc;
        }, []);
        mentionSuggestionState.results = merged.sort(function (a, b) {
            return (b.followersCount || 0) - (a.followersCount || 0);
        });
        renderMentionSuggestionsList(listEl);
        if (!listEl.dataset.scrollBound) {
            listEl.addEventListener('scroll', function () {
                const nearBottom = listEl.scrollTop + listEl.clientHeight >= listEl.scrollHeight - 12;
                if (nearBottom) {
                    if (mentionSuggestionState.visibleCount < mentionSuggestionState.results.length) {
                        mentionSuggestionState.visibleCount = Math.min(mentionSuggestionState.visibleCount + 5, mentionSuggestionState.results.length);
                        renderMentionSuggestionsList(listEl);
                    } else {
                        searchMentionSuggestions(`@${mentionSuggestionState.query}`);
                    }
                }
            });
            listEl.dataset.scrollBound = 'true';
        }
        const input = document.getElementById('mention-input');
        if (input) input.focus({ preventScroll: true });
    } catch (err) {
        console.warn('Mention search failed', err);
        listEl.innerHTML = '';
        listEl.style.display = 'none';
    } finally {
        mentionSuggestionState.loading = false;
    }
}

function handleMentionInput(event) {
    const value = event.target.value;
    if (mentionSearchTimer) clearTimeout(mentionSearchTimer);
    mentionSearchTimer = setTimeout(function () { searchMentionSuggestions(value); }, 200);
}

window.toggleTagInput = toggleTagInput;
window.addComposerTag = addComposerTag;
window.removeComposerTag = removeComposerTag;
window.handleTagInputKey = handleTagInputKey;
window.filterTagSuggestions = filterTagSuggestions;
window.toggleMentionInput = function (show) {
    const row = document.getElementById('mention-input-row');
    if (!row) return;
    const nextState = show !== undefined ? show : row.style.display !== 'flex';
    row.style.display = nextState ? 'flex' : 'none';
    if (nextState) {
        const input = document.getElementById('mention-input');
        if (input) input.focus();
    }
};
window.addComposerMention = addComposerMention;
window.removeComposerMention = removeComposerMention;
window.handleMentionInput = handleMentionInput;
window.handlePollOptionInput = handlePollOptionInput;
window.addPollOption = addPollOption;
window.removePollOption = removePollOption;
window.handlePollTitleChange = handlePollTitleChange;
window.handleScheduleChange = handleScheduleChange;
window.handleLocationInput = handleLocationInput;
window.setComposerLocation = setComposerLocation;
window.toggleVideoTagInput = toggleVideoTagInput;
window.addVideoTag = addVideoTag;
window.removeVideoTag = removeVideoTag;
window.handleVideoTagInputKey = handleVideoTagInputKey;
window.filterVideoTagSuggestions = filterVideoTagSuggestions;
window.toggleVideoMentionInput = function (show) {
    const row = document.getElementById('video-mention-input-row');
    if (!row) return;
    const nextState = show !== undefined ? show : row.style.display !== 'flex';
    row.style.display = nextState ? 'flex' : 'none';
    if (nextState) {
        const input = document.getElementById('video-mention-input');
        if (input) input.focus();
    }
};
window.addVideoMention = addVideoMention;
window.removeVideoMention = removeVideoMention;
window.handleVideoMentionInput = handleVideoMentionInput;

function escapeRegex(str = '') {
    return (str || '').replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&');
}

function renderTagList(tags = []) {
    if (!tags.length) return '';
    return `<div style="margin-top:8px;">${tags.map(function (tag) {
        const normalized = normalizeTagValue(tag);
        return normalized ? `<span class="tag-chip">#${escapeHtml(normalized)}</span>` : '';
    }).filter(Boolean).join('')}</div>`;
}

function renderPollBlock(post) {
    const poll = normalizePollPayload(post?.poll);
    if (!poll) return '';
    const votes = poll.votes || {};
    const counts = poll.options.map(function (_, idx) { return Object.values(votes).filter(function (v) { return Number(v) === idx; }).length; });
    const total = counts.reduce(function (sum, c) { return sum + c; }, 0) || 1;
    const userChoice = currentUser ? votes[currentUser.uid] : null;
    const optionsHtml = poll.options.map(function (opt, idx) {
        const pct = Math.round((counts[idx] / total) * 100);
        const selected = Number(userChoice) === idx;
        return `<button type="button" class="poll-option ${selected ? 'selected' : ''}" onclick="window.voteInPoll('${post.id}', ${idx}, event)"><span>${escapeHtml(opt)}</span><span class="poll-count">${counts[idx]} (${pct}%)</span></button>`;
    }).join('');
    const pollTitle = poll.title ? escapeHtml(poll.title) : 'Poll';
    return `<div class="poll-block"><div class="poll-title">${pollTitle}</div>${optionsHtml}</div>`;
}

function getMentionHandles(mentions = []) {
    if (!Array.isArray(mentions)) return [];
    return mentions.map(function (m) { return typeof m === 'string' ? m : (m.username || m.handle || ''); })
        .map(function (h) { return (h || '').replace(/^@/, '').toLowerCase(); })
        .filter(Boolean);
}

function formatContent(text = '', tags = [], mentions = []) {
    let safe = escapeHtml(cleanText(text));
    const mentionSet = new Set(getMentionHandles(mentions));
    mentionSet.forEach(function (handle) {
        const regex = new RegExp('@' + escapeRegex(handle), 'gi');
        safe = safe.replace(regex, `<a class="mention-link" onclick=\"window.openUserProfileByHandle('${handle}')\">@${escapeHtml(handle)}</a>`);
    });
    (tags || []).forEach(function (tag) {
        const normalized = normalizeTagValue(tag);
        if (!normalized) return;
        const regex = new RegExp('#' + escapeRegex(normalized), 'gi');
        safe = safe.replace(regex, `<span class="tag-chip">#${escapeHtml(normalized)}</span>`);
    });
    return safe;
}

async function resolveMentionProfiles(mentions = []) {
    const handles = getMentionHandles(mentions);
    const cleaned = Array.from(new Set(handles));
    const results = [];
    for (const handle of cleaned) {
        const cached = Object.entries(userCache).find(function ([_, data]) { return (data.username || '').toLowerCase() === handle; });
        if (cached) { results.push({ uid: cached[0], handle }); continue; }
        const qSnap = await getDocs(query(collection(db, 'users'), where('username', '==', handle)));
        if (!qSnap.empty) {
            const docSnap = qSnap.docs[0];
            storeUserInCache(docSnap.id, docSnap.data());
            results.push({ uid: docSnap.id, handle });
        }
    }
    return results;
}

async function notifyMentionedUsers(resolved = [], postId, meta = {}) {
    if (!resolved.length || !postId) return;
    const fn = httpsCallable(functions, 'notifyMention');
    const tasks = resolved.map(function (entry) {
        return fn({
            targetUserId: entry.uid,
            postId,
            handle: entry.handle || '',
            postTitle: meta?.title || '',
            thumbnailUrl: meta?.thumbnailUrl || ''
        });
    });
    await Promise.all(tasks);
}

function recordTagAffinity(tags = [], delta = 0) {
    if (!currentUser || !delta || !Array.isArray(tags) || tags.length === 0) return;
    const affinity = { ...(userProfile.tagAffinity || {}) };
    tags.forEach(function (tag) {
        affinity[tag] = (affinity[tag] || 0) + delta;
    });
    userProfile.tagAffinity = affinity;
    storeUserInCache(currentUser.uid, userProfile);
    setDoc(doc(db, 'users', currentUser.uid), { tagAffinity: affinity }, { merge: true });
}

function getPostAffinityScore(post) {
    const affinity = userProfile.tagAffinity || {};
    const tags = Array.isArray(post.tags) ? post.tags : [];
    return tags.reduce(function (total, tag) { return total + (affinity[tag] || 0); }, 0);
}

window.voteInPoll = async function (postId, optionIndex, event) {
    if (event) event.stopPropagation();
    if (!requireAuth()) return;
    const post = allPosts.find(function (p) { return p.id === postId; });
    if (!post) return;
    const poll = normalizePollPayload(post.poll || {});
    if (!poll || optionIndex < 0 || optionIndex >= poll.options.length) return;
    poll.votes = poll.votes || {};
    poll.votes[currentUser.uid] = optionIndex;
    post.poll = poll;
    refreshSinglePostUI(postId);
    try {
        await updateDoc(doc(db, 'posts', postId), { poll: { ...poll, votes: poll.votes } });
    } catch (e) {
        console.error('Poll vote failed', e);
    }
};

function normalizePollPayload(raw = null) {
    if (!raw || typeof raw !== 'object') return null;
    const title = (raw.title || '').toString();
    const options = Array.isArray(raw.options) ? raw.options.map(function (o) { return (o || '').toString().trim(); }).filter(Boolean) : [];
    const votes = raw.votes && typeof raw.votes === 'object' ? raw.votes : {};
    if (options.length < 2) return null;
    return { title, options: options.slice(0, 5), votes };
}

function buildPollPayloadFromComposer() {
    ensurePollOptionSlots();
    const options = (composerPoll.options || []).map(function (o) { return (o || '').trim(); }).filter(Boolean);
    if (options.length < 2) return null;
    return { title: (composerPoll.title || '').trim(), options: options.slice(0, 5), votes: {} };
}

function parseScheduleValue(value = '') {
    if (!value) return null;
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return null;
    return Timestamp.fromDate(parsed);
}

function formatTimestampForInput(ts) {
    if (!ts) return '';
    const date = ts instanceof Timestamp ? ts.toDate() : (typeof ts.seconds === 'number' ? new Date(ts.seconds * 1000) : new Date(ts));
    if (Number.isNaN(date.getTime())) return '';
    const local = new Date(date.getTime() - date.getTimezoneOffset() * 60000);
    return local.toISOString().slice(0, 16);
}

function isPostScheduledInFuture(post) {
    const ts = post?.scheduledFor;
    if (!ts) return false;
    const date = ts instanceof Timestamp ? ts.toDate() : (typeof ts.seconds === 'number' ? new Date(ts.seconds * 1000) : new Date(ts));
    if (Number.isNaN(date.getTime())) return false;
    return date.getTime() > Date.now();
}

function formatTimestampDisplay(ts) {
    if (!ts) return '';
    const date = ts instanceof Timestamp ? ts.toDate() : (typeof ts.seconds === 'number' ? new Date(ts.seconds * 1000) : new Date(ts));
    if (Number.isNaN(date.getTime())) return '';
    return date.toLocaleString([], { year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
}

function renderLocationBadge(location = '') {
    if (!location) return '';
    return `<div class="location-chip"><i class="ph ph-map-pin"></i> ${escapeHtml(location)}</div>`;
}

window.createPost = async function () {
    if (!requireAuth()) return;
    const title = document.getElementById('postTitle').value;
    const content = document.getElementById('postContent').value;
    const tagInput = document.getElementById('tag-input');
    const mentionInput = document.getElementById('mention-input');
    const normalizedTags = Array.from(new Set((composerTags || []).map(normalizeTagValue).filter(Boolean)));
    const tags = normalizedTags.map(function (tag) { return buildTagDisplayName(tag); });
    const mentions = normalizeMentionsField(composerMentions || []);
    const fileInput = document.getElementById('postFile');
    const btn = document.getElementById('publishBtn');
    setComposerError('');

    const pollPayload = buildPollPayloadFromComposer();
    const scheduledFor = parseScheduleValue(composerScheduledFor);
    const locationValue = (composerLocation || '').trim();

    let contentType = currentEditPost?.contentType || 'text';
    if (fileInput.files[0]) {
        const mime = fileInput.files[0].type;
        if (mime.startsWith('video')) contentType = 'video';
        else if (mime.startsWith('image')) contentType = 'image';
    }

    if (!title.trim() && !content.trim() && !fileInput.files[0]) {
        return alert("Please add a title, content, or media.");
    }

    btn.disabled = true;
    btn.textContent = "Uploading...";

    try {
        const targetCategoryId = currentEditPost ? currentEditPost.categoryId : selectedCategoryId;
        if (targetCategoryId && currentUser?.uid && !currentEditPost) {
            try {
                const joined = await ensureJoinedCategory(targetCategoryId, currentUser.uid);
                if (!joined) {
                    toast("You don’t have permission to post in this destination.", 'error');
                    return;
                }
            } catch (error) {
                if (error?.code === 'permission-denied') {
                    toast("You don’t have permission to post in this destination.", 'error');
                    return;
                }
                throw error;
            }
        }

        const mentionProfiles = await resolveMentionProfiles(mentions);
        const notificationTargets = [];
        const seenNotify = new Set();
        mentionProfiles.forEach(function (m) {
            if (m.uid && !seenNotify.has(m.uid)) { seenNotify.add(m.uid); notificationTargets.push(m); }
        });
        mentions.forEach(function (m) {
            if (m.uid && !seenNotify.has(m.uid)) { seenNotify.add(m.uid); notificationTargets.push({ uid: m.uid, handle: m.username }); }
        });
        const mentionUserIds = Array.from(seenNotify);

        let mediaUrl = currentEditPost?.mediaUrl || null;
        if (fileInput.files[0]) {
            const path = `posts/${currentUser.uid}/${Date.now()}_${fileInput.files[0].name}`;
            mediaUrl = await uploadFileToStorage(fileInput.files[0], path);
        }

        const categoryDoc = (currentEditPost ? getCategorySnapshot(currentEditPost.categoryId) : getCategorySnapshot(selectedCategoryId)) || null;
        const resolvedCategoryId = currentEditPost ? currentEditPost.categoryId : (selectedCategoryId || null);
        const visibility = 'public';
        const postPayload = {
            title,
            content,
            categoryId: resolvedCategoryId,
            categoryName: categoryDoc ? categoryDoc.name : null,
            categorySlug: categoryDoc ? categoryDoc.slug : null,
            categoryVerified: categoryDoc ? !!categoryDoc.verified : false,
            categoryType: categoryDoc ? categoryDoc.type : null,
            visibility,
            contentType,
            content: { text: content, mediaUrl, linkUrl: null, profileUid: null, meta: { tags, mentions } },
            mediaUrl,
            author: userProfile.name,
            userId: currentUser.uid,
            tags,
            mentions,
            mentionUserIds,
            poll: pollPayload,
            scheduledFor,
            location: locationValue,
            likes: currentEditPost ? currentEditPost.likes || 0 : 0,
            likedBy: currentEditPost ? currentEditPost.likedBy || [] : [],
            dislikes: currentEditPost ? currentEditPost.dislikes || 0 : 0,
            dislikedBy: currentEditPost ? currentEditPost.dislikedBy || [] : [],
            trustScore: currentEditPost ? currentEditPost.trustScore || 0 : 0,
            timestamp: currentEditPost ? currentEditPost.timestamp || serverTimestamp() : serverTimestamp()
        };

        if (currentEditPost) {
            await updateDoc(doc(db, 'posts', currentEditPost.id), postPayload);
            currentEditPost = null;
        } else {
            const postRef = await addDoc(collection(db, 'posts'), postPayload);
            if (notificationTargets.length) {
                await notifyMentionedUsers(notificationTargets, postRef.id, {
                    title,
                    thumbnailUrl: mediaUrl || ''
                });
            }
            await incrementTagUses(normalizedTags);
        }

        if (locationValue) {
            const existing = new Set(recentLocations || []);
            existing.add(locationValue);
            recentLocations = Array.from(existing).slice(-10);
            try { await setDoc(doc(db, 'users', currentUser.uid), { locationHistory: recentLocations }, { merge: true }); } catch (e) { console.warn('Location history save failed', e); }
        }

        // Reset Form
        resetComposerState();
        if (tagInput) tagInput.value = "";
        if (mentionInput) mentionInput.value = "";
        window.toggleCreateModal(false);
        window.navigateTo('feed');

    } catch (e) {
        console.error('Post/auto-join failed:', e);
        setComposerError('Could not post right now. Please try again.');
    } finally {
        btn.disabled = false;
        btn.textContent = "Post";
    }
}

// --- Settings & Modals ---
function updateRemovePhotoButtonState() {
    const btn = document.getElementById('remove-photo-btn');
    if (!btn) return;
    const hasPhoto = !!(userProfile.photoURL || (auth.currentUser && auth.currentUser.photoURL));
    btn.disabled = !hasPhoto;
    btn.classList.toggle('disabled', !hasPhoto);
}

async function tryDeleteProfilePhotoFromStorage(photoURL = '', photoPath = '') {
    if (!photoURL && !photoPath) return { deleted: false, reason: 'no-photo' };
    try {
        if (photoPath) {
            await deleteObject(ref(storage, photoPath));
            return { deleted: true };
        }
        const bucketHint = storage?.app?.options?.storageBucket || '';
        if (photoURL && bucketHint && photoURL.includes(bucketHint)) {
            await deleteObject(ref(storage, photoURL));
            return { deleted: true };
        }
    } catch (err) {
        console.warn('Storage delete failed', err);
        return { deleted: false, error: err };
    }
    return { deleted: false };
}

function updateSettingsAvatarPreview(src) {
    const preview = document.getElementById('settings-avatar-preview');
    if (!preview) return;

    const tempUser = {
        ...userProfile,
        photoURL: src || '',
        avatarColor: userProfile.avatarColor || computeAvatarColor(currentUser?.uid || 'user')
    };

    applyAvatarToElement(preview, tempUser, { size: 72 });
    updateRemovePhotoButtonState();
}

function syncThemeRadios(themeValue) {
    const selected = document.querySelector(`input[name="theme-choice"][value="${themeValue}"]`);
    if (selected) selected.checked = true;
}

window.toggleCreateModal = function (show) {
    document.getElementById('create-modal').style.display = show ? 'flex' : 'none';
    if (show && currentUser) {
        const avatarEl = document.getElementById('modal-user-avatar');
        applyAvatarToElement(avatarEl, userProfile, { size: 42 });
        setComposerError('');
        renderDestinationField();
        renderComposerTags();
        renderComposerMentions();
        renderPollOptions();
        const scheduleInput = document.getElementById('schedule-input');
        if (scheduleInput) scheduleInput.value = composerScheduledFor || '';
        setComposerLocation(composerLocation || '');
        syncComposerMode();
        syncPostButtonState();
    } else if (!show) {
        closeDestinationPicker();
        currentEditPost = null;
        syncComposerMode();
    }
}

window.toggleSettingsModal = function (show) {
    document.getElementById('settings-modal').style.display = show ? 'flex' : 'none';
    if (show) {
        document.getElementById('set-name').value = userProfile.name || "";
        document.getElementById('set-real-name').value = userProfile.realName || "";
        document.getElementById('set-username').value = userProfile.username || "";
        document.getElementById('set-bio').value = userProfile.bio || "";
        document.getElementById('set-website').value = userProfile.links || "";
        document.getElementById('set-phone').value = userProfile.phone || "";
        const genderInput = document.getElementById('set-gender');
        if (genderInput) genderInput.value = userProfile.gender || "Prefer not to say";
        document.getElementById('set-email').value = userProfile.email || "";
        document.getElementById('set-nickname').value = userProfile.nickname || "";
        document.getElementById('set-region').value = userProfile.region || "";
        const photoUrlInput = document.getElementById('set-photo-url');
        if (photoUrlInput) {
            photoUrlInput.value = userProfile.photoURL || "";
            photoUrlInput.oninput = function (e) { return updateSettingsAvatarPreview(e.target.value); };
        }
        syncThemeRadios(userProfile.theme || 'system');
        updateSettingsAvatarPreview(userProfile.photoURL);
        updateRemovePhotoButtonState();
        updatePushSettingsUI();

        const uploadInput = document.getElementById('set-pic-file');
        const cameraInput = document.getElementById('set-pic-camera');
        if (uploadInput) uploadInput.onchange = function (e) { return handleSettingsFileChange(e.target); };
        if (cameraInput) cameraInput.onchange = function (e) { return handleSettingsFileChange(e.target); };

        document.querySelectorAll('input[name="theme-choice"]').forEach(function (r) {
            r.onchange = function (e) { return persistThemePreference(e.target.value); };
        });
    }
}

window.saveSettings = async function () {
    const name = document.getElementById('set-name').value;
    const realName = document.getElementById('set-real-name').value;
    const nickname = document.getElementById('set-nickname').value;
    const username = document.getElementById('set-username').value.trim();
    const bio = document.getElementById('set-bio').value;
    const links = document.getElementById('set-website').value;
    const phone = document.getElementById('set-phone').value;
    const gender = document.getElementById('set-gender').value;
    const email = document.getElementById('set-email').value;
    const region = document.getElementById('set-region').value;
    const photoUrlInput = document.getElementById('set-photo-url');
    const manualPhoto = photoUrlInput ? photoUrlInput.value.trim() : '';
    const themeChoice = document.querySelector('input[name="theme-choice"]:checked');
    const theme = themeChoice ? themeChoice.value : (userProfile.theme || 'system');
    const fileInput = document.getElementById('set-pic-file');
    const cameraInput = document.getElementById('set-pic-camera');

    if (!username) {
        return alert("Username is required.");
    }
    if (username && !/^[A-Za-z0-9._-]{3,20}$/.test(username)) {
        return alert("Username must be 3-20 characters with letters, numbers, dots, underscores, or hyphens.");
    }

    let photoURL = userProfile.photoURL;
    let photoPath = userProfile.photoPath || '';
    const newPhoto = (fileInput && fileInput.files[0]) || (cameraInput && cameraInput.files[0]);
    if (newPhoto) {
        const path = `users/${currentUser.uid}/pfp_${Date.now()}`;
        photoURL = await uploadFileToStorage(newPhoto, path);
        photoPath = path;
    } else if (manualPhoto) {
        photoURL = manualPhoto;
        photoPath = '';
    }

    const updates = { displayName: name, name, realName, nickname, username, bio, links, phone, gender, email, region, theme, photoURL, photoPath };
    userProfile = { ...userProfile, ...updates };
    storeUserInCache(currentUser.uid, userProfile);

    try {
        const updateResult = await guardFirebaseCall('users:update', function () {
            return setDoc(doc(db, "users", currentUser.uid), updates, { merge: true });
        }, {
            onPermissionDenied: function () {
                toast('Profile can’t be updated due to permissions', 'error');
            }
        });
        if (!updateResult.ok) return;
        await syncPublicProfile(currentUser.uid, userProfile, {
            onPermissionDenied: function () {
                toast('Profile can’t be updated due to permissions', 'error');
            }
        });
        if (name) await updateProfile(auth.currentUser, { displayName: name, photoURL: photoURL });
    } catch (e) {
        console.error("Save failed", e);
    }

    await persistThemePreference(theme);
    renderProfile();
    renderFeed();
    window.toggleSettingsModal(false);
}

function handleSettingsFileChange(inputEl) {
    if (!inputEl || !inputEl.files || !inputEl.files[0]) return;
    const reader = new FileReader();
    reader.onload = function (e) { return updateSettingsAvatarPreview(e.target.result); };
    reader.readAsDataURL(inputEl.files[0]);
    updateRemovePhotoButtonState();
}

window.removeProfilePhoto = async function () {
    if (!currentUser || !auth.currentUser) return toast('You need to be signed in.', 'error');
    const ok = confirm('Remove your profile photo?');
    if (!ok) return;

    const existingPhotoURL = userProfile.photoURL || auth.currentUser.photoURL || '';
    const existingPhotoPath = userProfile.photoPath || '';
    let deleteResult = { deleted: false };

    try {
        deleteResult = await tryDeleteProfilePhotoFromStorage(existingPhotoURL, existingPhotoPath);
    } catch (err) {
        console.warn('Deletion attempt error', err);
    }

    try {
        await updateProfile(auth.currentUser, { photoURL: '' });
    } catch (err) {
        console.warn('Auth photo reset failed', err);
    }

    try {
        const updateResult = await guardFirebaseCall('users:update:photo', function () {
            return setDoc(doc(db, 'users', currentUser.uid), { photoURL: '', photoPath: '' }, { merge: true });
        }, {
            onPermissionDenied: function () {
                toast('Profile can’t be updated due to permissions', 'error');
            }
        });
        if (!updateResult.ok) return;
        await syncPublicProfile(currentUser.uid, userProfile, {
            onPermissionDenied: function () {
                toast('Profile can’t be updated due to permissions', 'error');
            }
        });
    } catch (err) {
        console.error('Failed to clear Firestore photo data', err);
        toast('Could not update profile photo references', 'error');
        return;
    }

    userProfile.photoURL = '';
    userProfile.photoPath = '';
    if (userCache[currentUser.uid]) {
        userCache[currentUser.uid].photoURL = '';
        userCache[currentUser.uid].photoPath = '';
    }

    updateSettingsAvatarPreview('');
    renderProfile();
    renderFeed();
    if (activePostId) renderThreadMainPost(activePostId);

    updateRemovePhotoButtonState();

    if (!existingPhotoURL && !existingPhotoPath) {
        toast('No profile photo to remove', 'info');
        return;
    }

    if (deleteResult.deleted) {
        toast('Profile photo removed', 'info');
    } else {
        toast('Could not delete photo from storage, but removed references', 'error');
    }
}

// --- Peer Review System ---
window.openPeerReview = function (postId) {
    activePostId = postId;
    document.getElementById('review-modal').style.display = 'flex';
    document.getElementById('review-stats-text').textContent = "Loading data...";

    const reviewsRef = collection(db, 'posts', postId, 'reviews');
    const q = query(reviewsRef);

    ListenerRegistry.register(`reviews:post:${postId}`, onSnapshot(q, function (snapshot) {
        const container = document.getElementById('review-list');
        container.innerHTML = "";

        let scores = { verified: 0, citation: 0, misleading: 0, total: 0 };
        let userHasReview = false;
        let myRatingData = null;

        snapshot.forEach(function (doc) {
            const data = doc.data();
            if (data.userId === currentUser.uid) {
                userHasReview = true;
                window.currentReviewId = doc.id;
                myRatingData = data;

                // Cache my review to update the feed button color
                window.myReviewCache[activePostId] = data.rating;
                refreshSinglePostUI(activePostId);
                applyMyReviewStylesToDOM();
            }
            const rAuthor = userCache[data.userId] || { name: "Reviewer" };
            scores.total++;
            if (data.rating === 'verified') scores.verified++;
            if (data.rating === 'citation') scores.citation++;
            if (data.rating === 'misleading') scores.misleading++;

            let badge = data.rating === 'verified'
                ? '<i class="ph-fill ph-check-circle" style="color:#00ff00;"></i> Verified'
                : (data.rating === 'citation'
                    ? '<i class="ph-fill ph-warning-circle" style="color:#ffaa00;"></i> Citation Needed'
                    : '<i class="ph-fill ph-x-circle" style="color:#ff3d3d;"></i> Misleading');

            container.innerHTML += `
                <div style="background:var(--bg-hover); padding:10px; border-radius:8px; margin-bottom:10px;">
                    <div style="font-size:0.8rem; display:flex; justify-content:space-between; margin-bottom:5px;">
                        <strong>${escapeHtml(rAuthor.name)}</strong>
                        <span style="font-weight:bold; display:flex; align-items:center; gap:4px;">${badge}</span>
                    </div>
                    <div style="font-size:0.9rem;">${escapeHtml(data.note)}</div>
                </div>`;
        });

        // User Status UI
        if (userHasReview) {
            document.getElementById('review-submit-section').style.display = 'none';
            document.getElementById('review-remove-section').style.display = 'block';
            let myBadge = "";
            let myNote = "";
            if (myRatingData) {
                myBadge = myRatingData.rating === 'verified'
                    ? '✅ Verified Accurate'
                    : (myRatingData.rating === 'citation' ? '⚠️ Needs Citations' : '🚫 Misleading / False');
                myNote = myRatingData.note || "(No explanation provided)";
            }
            document.getElementById('review-user-status').innerHTML = `
                <div style="margin-bottom:8px; font-size:1rem;">You rated this: ${myBadge}</div>
                <div style="text-align:left; background:var(--bg-hover); padding:10px; border-radius:8px; font-size:0.9rem; color:var(--text-muted); border:1px solid var(--border);">
                    <strong style="color:var(--text-main); display:block; margin-bottom:4px;">Your Explanation:</strong>"${escapeHtml(myNote)}"
                </div>`;
        } else {
            document.getElementById('review-submit-section').style.display = 'block';
            document.getElementById('review-remove-section').style.display = 'none';
        }

        // Stats Bar
        if (scores.total > 0) {
            const vP = (scores.verified / scores.total) * 100;
            const cP = (scores.citation / scores.total) * 100;
            const mP = (scores.misleading / scores.total) * 100;
            document.getElementById('review-bar').innerHTML = `
                <div style="width:${vP}%; background:#00ff00; height:8px;"></div>
                <div style="width:${cP}%; background:#ffaa00; height:8px;"></div>
                <div style="width:${mP}%; background:#ff0000; height:8px;"></div>`;
            document.getElementById('review-stats-text').innerHTML = `${scores.total} Reviews`;
        } else {
            document.getElementById('review-bar').innerHTML = `<div style="width:100%; background:#333; height:8px;"></div>`;
            document.getElementById('review-stats-text').textContent = "No peer reviews yet.";
        }
    }));
}

// FIX: Review Submission Logic Stabilized
window.submitReview = async function () {
    if (!activePostId) return alert("Error: No active post selected.");
    const ratingEl = document.getElementById('review-rating');
    const noteEl = document.getElementById('review-note');

    if (!ratingEl || !noteEl) return;

    const rating = ratingEl.value;
    const note = noteEl.value;

    if (!note.trim()) return alert("Please add a note explaining your review.");

    try {
        // 1. Update Cache
        if (!window.myReviewCache) window.myReviewCache = {};
        window.myReviewCache[activePostId] = rating;

        // 2. Update UI Immediately
        refreshSinglePostUI(activePostId);
        applyMyReviewStylesToDOM();
        noteEl.value = "";

        // 3. Close Modal
        window.closeReview();

        // 4. Backend Update
        await addDoc(collection(db, 'posts', activePostId, 'reviews'), {
            userId: currentUser.uid,
            rating,
            note,
            timestamp: serverTimestamp()
        });

        const postRef = doc(db, 'posts', activePostId);
        const scoreChange = (rating === 'verified') ? 1 : -1;
        await updateDoc(postRef, { trustScore: increment(scoreChange) });

    } catch (e) {
        console.error("Review failed", e);
        alert("Failed to submit review. Please try again.");
    }
}

// PATCH: Clear cache and reset UI color on remove
window.removeReview = async function () {
    if (!window.currentReviewId || !activePostId) return;

    // Clear from cache immediately
    if (window.myReviewCache) delete window.myReviewCache[activePostId];

    // Reset UI color
    refreshSinglePostUI(activePostId);
    applyMyReviewStylesToDOM();
    window.closeReview(); // Close modal

    try {
        await deleteDoc(doc(db, 'posts', activePostId, 'reviews', window.currentReviewId));
    } catch (e) {
        console.error(e);
    }
}

// --- Thread & Comments ---
window.showThreadLoadError = function (message = 'Unable to load this thread right now.') {
    activePostId = null;
    window.navigateTo('thread', false);
    const main = document.getElementById('thread-main-post');
    const stream = document.getElementById('thread-stream');
    if (main) {
        main.innerHTML = `<div class="empty-state"><p>${escapeHtml(message)}</p></div>`;
    }
    if (stream) {
        stream.innerHTML = '';
    }
};

window.openThread = async function (postId) {
    activePostId = postId;
    activeReplyId = null;
    commentRootDisplayCount[postId] = 20;
    replyExpansionState = {};
    commentFilterMode = 'popularity';
    commentFilterQuery = '';
    const filterSelect = document.getElementById('comment-filter-mode');
    const filterInput = document.getElementById('comment-filter-input');
    if (filterSelect) filterSelect.value = 'popularity';
    if (filterInput) { filterInput.value = ''; filterInput.style.display = 'none'; }
    window.resetInputBox();
    window.navigateTo('thread');

    if (!allPosts.find(function (p) { return p.id === postId; })) {
        try {
            const snap = await getDoc(doc(db, 'posts', postId));
            if (!snap.exists()) {
                window.showThreadLoadError('Thread not found.');
                return;
            }
            window.Nexera?.ensurePostInCache?.({ id: snap.id, ...snap.data() });
        } catch (error) {
            console.error('Thread load failed', error);
            window.showThreadLoadError('Unable to load this thread right now.');
            return;
        }
    }

    renderThreadMainPost(postId);
    attachThreadComments(postId);
}

function attachThreadComments(postId) {
    const container = document.getElementById('thread-stream');
    if (!container) return;

    const commentsRef = collection(db, 'posts', postId, 'comments');
    const q = query(commentsRef, orderBy('timestamp', 'asc'));

    if (threadUnsubscribe) threadUnsubscribe();

    threadUnsubscribe = ListenerRegistry.register(`comments:${postId}`, onSnapshot(q, function (snapshot) {
        const comments = snapshot.docs.map(function (d) { return ({ id: d.id, ...d.data() }); });
        const missingCommentUsers = comments.filter(function (c) { return !userCache[c.userId]; }).map(function (c) { return ({ userId: c.userId }); });
        if (missingCommentUsers.length > 0) fetchMissingProfiles(missingCommentUsers);
        threadComments = comments;
        pruneOptimisticMatches(comments);
        renderThreadComments();
    }, function (error) {
        console.error('Comments load error', error);
        container.innerHTML = `<div class="empty-state"><p>Unable to load comments right now.</p></div>`;
    }));
}

function mergeOptimisticComments(base = []) {
    const seen = new Set();
    const merged = [];
    optimisticThreadComments.forEach(function (c) { merged.push(c); seen.add(c.id); });
    base.forEach(function (c) { if (!seen.has(c.id)) merged.push(c); });
    return merged;
}

function pruneOptimisticMatches(serverComments = []) {
    if (!optimisticThreadComments.length) return;
    const serverKeys = new Set(serverComments.map(function (c) { return `${c.userId || ''}::${(c.text || '').trim()}`; }));
    optimisticThreadComments = optimisticThreadComments.filter(function (opt) {
        const key = `${opt.userId || ''}::${(opt.text || '').trim()}`;
        return !serverKeys.has(key);
    });
}

function getCommentSortComparator() {
    if (commentFilterMode === 'popularity') {
        return function (a, b) {
            const likeDiff = (b.likes || 0) - (a.likes || 0);
            if (likeDiff !== 0) return likeDiff;
            return (b.timestamp?.seconds || 0) - (a.timestamp?.seconds || 0);
        };
    }
    if (commentFilterMode === 'datetime') {
        return function (a, b) { return (b.timestamp?.seconds || 0) - (a.timestamp?.seconds || 0); };
    }
    return function (a, b) { return (b.timestamp?.seconds || 0) - (a.timestamp?.seconds || 0); };
}

function filterAndSortComments(list = []) {
    const queryVal = (commentFilterQuery || '').trim().toLowerCase();
    let filtered = list.slice();

    if (commentFilterMode === 'user' && queryVal) {
        filtered = filtered.filter(function (c) {
            const author = userCache[c.userId] || {};
            const name = (author.name || '').toLowerCase();
            const username = (author.username || '').toLowerCase();
            return name.includes(queryVal) || username.includes(queryVal);
        });
    } else if (commentFilterMode === 'content' && queryVal) {
        filtered = filtered.filter(function (c) { return (c.text || '').toLowerCase().includes(queryVal); });
    }

    const sorter = getCommentSortComparator();
    return filtered.slice().sort(sorter);
}

const renderCommentHtml = function (c, isReply) {
    const cAuthor = userCache[c.userId] || { name: "User", photoURL: null };
    const verifiedBadge = renderVerifiedBadge(cAuthor, 'with-gap');

    const isLiked = Array.isArray(c.likedBy) && c.likedBy.includes(currentUser?.uid);
    const isDisliked = Array.isArray(c.dislikedBy) && c.dislikedBy.includes(currentUser?.uid);
    const isOwner = currentUser?.uid && c.userId === currentUser.uid;

    const avatarHtml = renderAvatar({ ...cAuthor, uid: c.userId }, { size: 36 });
    const username = cAuthor.username ? `@${escapeHtml(cAuthor.username)}` : '';
    const timestampText = formatDateTime(c.timestamp) || 'Now';

    const parentCommentId = c.parentCommentId || c.parentId;

    const mediaHtml = c.mediaUrl
        ? `<div onclick="window.openFullscreenMedia('${c.mediaUrl}', 'image')">
         <img src="${c.mediaUrl}" style="max-width:200px; border-radius:8px; margin-top:5px; cursor:pointer;">
       </div>`
        : "";

    const menuButton = `
        <button class="comment-menu-btn" onclick="window.openCommentMenu('${c.id}', ${isOwner}, event)" aria-label="Comment options">
            <i class="ph ph-dots-three-vertical"></i>
        </button>`;

    return `
    <div id="comment-${c.id}" class="comment-item ${isReply ? 'reply-item' : ''}" data-parent="${parentCommentId || ''}">
      <div class="comment-thread-line"></div>
      <div class="comment-card">
        <div class="comment-header">
          <button class="comment-avatar-btn" onclick="window.openUserProfile('${c.userId}', event)">
            ${avatarHtml}
          </button>
          <div class="comment-header-meta">
            <div class="comment-author-row">
              <span class="author-name">${escapeHtml(cAuthor.name || 'User')}</span>${verifiedBadge}
              <span class="comment-username">${username}</span>
            </div>
            <div class="comment-timestamp">${timestampText}</div>
          </div>
          ${menuButton}
        </div>

        <div class="comment-body-text">
          ${escapeHtml(c.text || '')}
        </div>

        ${mediaHtml}

        <div class="comment-actions">
          <button onclick="window.moveInputToComment('${c.id}', '${escapeHtml(cAuthor.name || 'User')}')" class="comment-action-btn">
            <i class="ph ph-arrow-bend-up-left"></i> Reply
          </button>

          <button
            data-role="comment-like"
            data-comment-id="${c.id}"
            data-liked="${isLiked ? 'true' : 'false'}"
            data-disliked="${isDisliked ? 'true' : 'false'}"
            onclick="window.toggleCommentLike('${c.id}', event)"
            class="comment-action-btn ${isLiked ? 'active' : ''}">
            <i class="${isLiked ? 'ph-fill' : 'ph'} ph-thumbs-up"></i>
            <span class="comment-like-count" id="comment-like-count-${c.id}">${c.likes || 0}</span>
          </button>

          <button
            data-role="comment-dislike"
            data-comment-id="${c.id}"
            data-liked="${isLiked ? 'true' : 'false'}"
            data-disliked="${isDisliked ? 'true' : 'false'}"
            onclick="window.toggleCommentDislike('${c.id}', event)"
            class="comment-action-btn ${isDisliked ? 'active dislike' : ''}">
            <i class="${isDisliked ? 'ph-fill' : 'ph'} ph-thumbs-down"></i>
            <span class="comment-dislike-count" id="comment-dislike-count-${c.id}">${c.dislikes || 0}</span>
          </button>
        </div>

        <div id="reply-slot-${c.id}" class="replies-container"></div>
      </div>
    </div>`;
};

function renderThreadComments(comments = mergeOptimisticComments(threadComments)) {
    const container = document.getElementById('thread-stream');
    if (!container) return;

    const filtered = filterAndSortComments(comments);
    currentThreadComments = filtered;
    const grouping = groupCommentsByParent(filtered);
    const sorter = getCommentSortComparator();
    const roots = grouping.roots.slice().sort(sorter);
    const byParent = grouping.byParent;

    const postId = activePostId;
    if (!commentRootDisplayCount[postId]) commentRootDisplayCount[postId] = 20;
    const rootLimit = commentRootDisplayCount[postId];
    const visibleRoots = roots.slice(0, rootLimit);

    container.innerHTML = '';
    visibleRoots.forEach(function (c) { container.innerHTML += renderCommentHtml(c, false); });

    const ensureReplyState = function (parentId) {
        if (!replyExpansionState[parentId]) {
            replyExpansionState[parentId] = { open: false, loaded: 0 };
        }
        return replyExpansionState[parentId];
    };

    const renderReplies = function (parentId) {
        const replies = (byParent[parentId] || []).slice().sort(sorter);
        const slot = document.getElementById(`reply-slot-${parentId}`);
        if (!slot) return;
        const state = ensureReplyState(parentId);
        slot.innerHTML = '';

        if (!state.open && replies.length) {
            const viewBtn = document.createElement('button');
            viewBtn.className = 'see-more-replies';
            viewBtn.textContent = `View replies (${replies.length})`;
            viewBtn.onclick = function () {
                state.open = true;
                state.loaded = Math.min(4, replies.length);
                renderThreadComments(currentThreadComments);
            };
            slot.appendChild(viewBtn);
            return;
        }

        const limit = Math.min(state.loaded || 4, replies.length);
        state.loaded = limit;
        const replyStack = document.createElement('div');
        replyStack.className = 'reply-stack';
        replies.slice(0, limit).forEach(function (reply) {
            replyStack.insertAdjacentHTML('beforeend', renderCommentHtml(reply, true));
            renderReplies(reply.id);
        });
        slot.appendChild(replyStack);

        const controls = document.createElement('div');
        controls.className = 'reply-controls';
        if (replies.length > limit) {
            const moreBtn = document.createElement('button');
            moreBtn.className = 'see-more-replies';
            moreBtn.textContent = 'View more replies';
            moreBtn.onclick = function () {
                state.loaded = Math.min(replies.length, state.loaded + 4);
                renderThreadComments(currentThreadComments);
            };
            controls.appendChild(moreBtn);
        }
        if (replies.length && state.open) {
            const hideBtn = document.createElement('button');
            hideBtn.className = 'see-more-replies subtle';
            hideBtn.textContent = 'Hide replies';
            hideBtn.onclick = function () {
                state.open = false;
                slot.innerHTML = '';
                renderThreadComments(currentThreadComments);
            };
            controls.appendChild(hideBtn);
        }
        if (controls.childElementCount) slot.appendChild(controls);
    };

    visibleRoots.forEach(function (c) { renderReplies(c.id); });

    if (roots.length > rootLimit) {
        const moreBtn = document.createElement('button');
        moreBtn.className = 'load-more-comments';
        moreBtn.textContent = 'Load More Comments';
        moreBtn.onclick = function () { commentRootDisplayCount[postId] = rootLimit + 20; renderThreadComments(currentThreadComments); };
        container.appendChild(moreBtn);
    }

    const inputArea = document.getElementById('thread-input-area');
    const defaultSlot = document.getElementById('thread-input-default-slot');
    if (inputArea && !inputArea.parentElement && defaultSlot) {
        defaultSlot.appendChild(inputArea);
    }

    if (typeof activeReplyId !== 'undefined' && activeReplyId) {
        const slot = document.getElementById(`reply-slot-${activeReplyId}`);
        if (slot && inputArea && !slot.contains(inputArea)) {
            slot.appendChild(inputArea);
            const input = document.getElementById('thread-input');
            if (input) input.focus();
        }
    }
}

window.updateCommentFilterMode = function (mode = 'popularity') {
    commentFilterMode = mode || 'popularity';
    commentFilterQuery = '';
    const input = document.getElementById('comment-filter-input');
    if (input) {
        const needsInput = mode === 'user' || mode === 'content';
        input.style.display = needsInput ? 'block' : 'none';
        input.placeholder = mode === 'user' ? 'Filter by username' : 'Filter by keyword';
        input.value = '';
    }
    renderThreadComments();
};

window.updateCommentFilterQuery = function (value = '') {
    commentFilterQuery = value || '';
    renderThreadComments();
};



function renderThreadMainPost(postId) {
    const container = document.getElementById('thread-main-post');
    const post = allPosts.find(function (p) { return p.id === postId; });
    if (!post) return;
    const viewerUid = getViewerUid();

    const isLiked = viewerUid ? (post.likedBy && post.likedBy.includes(viewerUid)) : false;
    const isDisliked = viewerUid ? (post.dislikedBy && post.dislikedBy.includes(viewerUid)) : false;
    const isSaved = viewerUid ? (userProfile.savedPosts && userProfile.savedPosts.includes(postId)) : false;
    const isFollowingUser = followedUsers.has(post.userId);
    const isFollowingTopic = followedCategories.has(post.category);
    const isSelfPost = viewerUid ? post.userId === viewerUid : false;
    const topicClass = post.category.replace(/[^a-zA-Z0-9]/g, '');

    const authorData = userCache[post.userId] || { name: post.author, username: "user" };
    const date = formatDateTime(post.timestamp) || 'Just now';
    const avatarHtml = renderAvatar({ ...authorData, uid: post.userId }, { size: 48 });

    const verifiedBadge = renderVerifiedBadge(authorData);
    const postText = typeof post.content === 'object' && post.content !== null ? (post.content.text || '') : (post.content || '');
    const formattedBody = formatContent(postText, post.tags, post.mentions);
    const tagListHtml = renderTagList(post.tags || []);
    const pollBlock = renderPollBlock(post);
    const locationBadge = renderLocationBadge(post.location);
    const scheduledChip = isPostScheduledInFuture(post) && viewerUid && post.userId === viewerUid ? `<div class="scheduled-chip">Scheduled for ${formatTimestampDisplay(post.scheduledFor)}</div>` : '';
    const verification = getVerificationState(post);
    const verificationBanner = verification && verification.bannerText ? `<div class="verification-banner ${verification.className}"><i class="ph ph-warning"></i><div>${verification.bannerText}</div></div>` : '';
    const verificationChip = verification ? `<span class="verification-chip ${verification.className}">${verification.label}</span>` : '';
    const followButtons = isSelfPost ? '' : `
                                <button class="follow-btn js-follow-user-${post.userId} ${isFollowingUser ? 'following' : ''}" onclick="event.stopPropagation(); window.toggleFollowUser('${post.userId}', event)" style="font-size:0.75rem; padding:6px 12px;">${isFollowingUser ? 'Following' : '<i class="ph-bold ph-plus"></i> User'}</button>
                                <button class="follow-btn js-follow-topic-${topicClass} ${isFollowingTopic ? 'following' : ''}" data-topic="${escapeHtml(post.category)}" onclick="event.stopPropagation(); window.toggleFollow('${post.category}', event)" style="font-size:0.75rem; padding:6px 12px;">${isFollowingTopic ? 'Following' : '<i class="ph-bold ph-plus"></i> Topic'}</button>`;

    let mediaContent = '';
    if (post.mediaUrl) {
        if (post.type === 'video') mediaContent = `<div class="video-container" onclick="window.openFullscreenMedia('${post.mediaUrl}', 'video')"><video src="${post.mediaUrl}" controls class="post-media"></video></div>`;
        else mediaContent = `<img src="${post.mediaUrl}" class="post-media" alt="Post Content" onclick="window.openFullscreenMedia('${post.mediaUrl}', 'image')">`;
    }

    // UPDATE: Trust Badge Logic for Thread View to match Feed
    let trustBadge = "";
    if (post.trustScore > 2) {
        trustBadge = `<div style="font-size:0.75rem; color:#8b949e; display:flex; align-items:center; gap:4px; font-weight:600;"><i class="ph-fill ph-check-circle"></i> Publicly Verified</div>`;
    } else if (post.trustScore < -1) {
        trustBadge = `<div style="font-size:0.75rem; color:#ff3d3d; display:flex; align-items:center; gap:4px; font-weight:600;"><i class="ph-fill ph-warning-circle"></i> Disputed</div>`;
    }

    const myReview = window.myReviewCache ? window.myReviewCache[post.id] : null;
    const reviewDisplay = getReviewDisplay(myReview);
    const mobileView = isMobileViewport();
    const actionsHtml = mobileView
        ? renderDiscussionActionsMobile(post, { isLiked, isDisliked, isSaved, reviewDisplay })
        : renderPostActions(post, { isLiked, isDisliked, isSaved, reviewDisplay, iconSize: '1.2rem', discussionLabel: 'Comment', discussionOnclick: "document.getElementById('thread-input').focus()", extraClass: 'thread-action-row', idPrefix: 'thread' });

    // Updated Layout for Thread Main Post to match Feed Header logic
    container.innerHTML = `
        <div style="padding: 1rem; border-bottom: 1px solid var(--border);">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:15px;">
                <div class="author-wrapper" onclick="window.openUserProfile('${post.userId}')">
                    ${avatarHtml}
                    <div>
                        <div class="author-line" style="font-size:1rem;"><span class="author-name">${escapeHtml(authorData.name)}</span>${verifiedBadge}</div>
                        <div class="post-meta">@${escapeHtml(authorData.username)}</div>
                    </div>
                </div>
                <div style="display:flex; flex-direction:column; align-items:flex-end; gap:6px;">
                    <div style="display:flex; gap:10px; align-items:center; justify-content:flex-end; width:100%;">
                        <div style="display:flex; gap:5px; flex-wrap:wrap; justify-content:flex-end;">${followButtons}</div>
                        ${getPostOptionsButton(post, 'thread', '1.2rem')}
                    </div>
                    ${trustBadge}
                </div>
            </div>
            ${verificationBanner}
            <h2 id="thread-view-title" data-post-id="${post.id}" style="font-size: 1.4rem; font-weight: 800; margin-bottom: 0.5rem; line-height: 1.3;">${escapeHtml(post.title)}</h2>
            <p class="post-body-text thread-body-text" style="font-size: 1.1rem; line-height: 1.5; color: var(--text-main); margin-bottom: 1rem;">${formattedBody}</p>
            ${tagListHtml}
            ${locationBadge}
            ${scheduledChip}
            ${pollBlock}
            ${mediaContent}
            <div style="margin-top: 1rem; padding: 10px 0; border-top: 1px solid var(--border); border-bottom: 1px solid var(--border); color: var(--text-muted); font-size: 0.9rem; display:flex; gap:10px; align-items:center; flex-wrap:wrap;">${date} • <span style="color:var(--text-main); font-weight:700;">${post.category}</span>${verificationChip}</div>
                        ${actionsHtml}
        </div>`;

    const inputPfp = document.getElementById('thread-input-pfp');
    if (inputPfp) applyAvatarToElement(inputPfp, userProfile, { size: 40 });

    const threadReviewBtn = document.getElementById(`thread-review-btn-${post.id}`);
    applyReviewButtonState(threadReviewBtn, myReview);
    applyMyReviewStylesToDOM();
}

window.sendComment = async function () {
    const input = document.getElementById('thread-input');
    const fileInput = document.getElementById('thread-file');
    const text = input.value.trim();

    if (!text && !fileInput.files[0]) return;

    const btn = document.getElementById('thread-send-btn');
    btn.disabled = true;
    btn.textContent = "...";

    let optimisticId = null;
    try {
        let mediaUrl = null;
        if (fileInput.files[0]) {
            const path = `comments/${currentUser.uid}/${Date.now()}_${fileInput.files[0].name}`;
            mediaUrl = await uploadFileToStorage(fileInput.files[0], path);
        }

        const parentCommentId = normalizeReplyTarget(activeReplyId);
        const payload = buildReplyRecord({ text, mediaUrl, parentCommentId, userId: currentUser.uid });

        const optimisticTimestamp = { seconds: Math.floor(Date.now() / 1000) };
        optimisticId = `optimistic-${Date.now()}`;
        const optimisticComment = {
            ...payload,
            id: optimisticId,
            timestamp: optimisticTimestamp,
            likes: 0,
            dislikes: 0,
            likedBy: [],
            dislikedBy: []
        };
        optimisticThreadComments.unshift(optimisticComment);
        renderThreadComments();

        payload.timestamp = serverTimestamp();

        await addDoc(collection(db, 'posts', activePostId, 'comments'), payload);

        const postRef = doc(db, 'posts', activePostId);
        await updateDoc(postRef, {
            previewComment: {
                text: text.substring(0, 80) + (text.length > 80 ? '...' : ''),
                author: userProfile.name,
                likes: 0
            }
        });
        resetInputBox();
        document.getElementById('attach-btn-text').textContent = "📎 Attach";
        document.getElementById('attach-btn-text').style.color = "var(--text-muted)";
        fileInput.value = "";
    } catch (e) {
        console.error(e);
        optimisticThreadComments = optimisticThreadComments.filter(function (c) { return c.id !== optimisticId; });
        renderThreadComments();
    } finally {
        btn.disabled = false;
        btn.textContent = "Reply";
        const defaultSlot = document.getElementById('thread-input-default-slot');
        const inputArea = document.getElementById('thread-input-area');
        if (defaultSlot && inputArea && !defaultSlot.contains(inputArea)) {
            defaultSlot.appendChild(inputArea);
        }
    }
}

function updateCommentReactionUI(commentId, likes, dislikes, isLiked, isDisliked) {
    const likeBtn = document.querySelector(`button[data-role="comment-like"][data-comment-id="${commentId}"]`);
    const dislikeBtn = document.querySelector(`button[data-role="comment-dislike"][data-comment-id="${commentId}"]`);

    if (likeBtn) {
        likeBtn.dataset.liked = isLiked ? 'true' : 'false';
        likeBtn.dataset.disliked = isDisliked ? 'true' : 'false';
        likeBtn.style.color = isLiked ? '#00f2ea' : 'var(--text-muted)';
        likeBtn.innerHTML = `<i class="${isLiked ? 'ph-fill' : 'ph'} ph-thumbs-up"></i> <span class="comment-like-count" id="comment-like-count-${commentId}">${likes}</span>`;
    }

    if (dislikeBtn) {
        dislikeBtn.dataset.liked = isLiked ? 'true' : 'false';
        dislikeBtn.dataset.disliked = isDisliked ? 'true' : 'false';
        dislikeBtn.style.color = isDisliked ? '#ff3d3d' : 'var(--text-muted)';
        dislikeBtn.innerHTML = `<i class="${isDisliked ? 'ph-fill' : 'ph'} ph-thumbs-down"></i> <span class="comment-dislike-count" id="comment-dislike-count-${commentId}">${dislikes}</span>`;
    }
}

window.openCommentMenu = function (commentId, isOwner, event) {
    event?.stopPropagation?.();
    const comment = threadComments.find(function (c) { return c.id === commentId; })
        || optimisticThreadComments.find(function (c) { return c.id === commentId; });
    if (!comment) return;
    const options = [];
    if (isOwner) {
        options.push({ label: 'Edit Comment', action: function () { window.beginEditComment(commentId); } });
        options.push({ label: 'Delete Comment', action: function () { window.confirmDeleteComment(commentId); } });
        options.push({ label: 'Share', action: function () { window.shareComment(commentId); } });
        options.push({ label: 'Report', action: function () { window.reportContent(comment, 'comments'); } });
    } else {
        options.push({ label: 'Share', action: function () { window.shareComment(commentId); } });
        options.push({ label: 'Message Author', action: function () { window.openUserProfile(comment.userId); } });
        options.push({ label: 'Report', action: function () { window.reportContent(comment, 'comments'); } });
    }
    renderCommentMenu(options, event);
};

function renderCommentMenu(options = [], event) {
    let menu = document.getElementById('comment-options-menu');
    if (!menu) {
        menu = document.createElement('div');
        menu.id = 'comment-options-menu';
        menu.className = 'comment-options-menu menu-surface';
        document.body.appendChild(menu);
    }
    menu.innerHTML = options.map(function (opt, idx) {
        return `<button type="button" data-comment-menu-idx="${idx}">${escapeHtml(opt.label)}</button>`;
    }).join('');

    menu.querySelectorAll('button').forEach(function (btn) {
        const idx = Number(btn.dataset.commentMenuIdx || 0);
        btn.onclick = function () {
            closeCommentMenu();
            options[idx]?.action?.();
        };
    });

    if (event?.currentTarget) {
        const rect = event.currentTarget.getBoundingClientRect();
        menu.style.top = `${rect.bottom + window.scrollY + 6}px`;
        menu.style.left = `${Math.max(10, rect.right + window.scrollX - menu.offsetWidth)}px`;
    }
    menu.classList.add('open');
    document.addEventListener('click', handleCommentMenuOutside, true);
    document.addEventListener('keydown', handleCommentMenuEscape, true);
}

function closeCommentMenu() {
    const menu = document.getElementById('comment-options-menu');
    if (menu) menu.classList.remove('open');
    document.removeEventListener('click', handleCommentMenuOutside, true);
    document.removeEventListener('keydown', handleCommentMenuEscape, true);
}

function handleCommentMenuOutside(event) {
    const menu = document.getElementById('comment-options-menu');
    if (!menu) return;
    if (menu.contains(event.target)) return;
    closeCommentMenu();
}

function handleCommentMenuEscape(event) {
    if (event.key === 'Escape') closeCommentMenu();
}

window.beginEditComment = function (commentId) {
    const comment = threadComments.find(function (c) { return c.id === commentId; });
    if (!comment || comment.userId !== currentUser?.uid) {
        return toast('You can only edit your own comment.', 'error');
    }
    const nextText = prompt('Edit your comment:', comment.text || '');
    if (nextText === null) return;
    const trimmed = nextText.trim();
    if (!trimmed) return toast('Comment cannot be empty.', 'error');
    updateDoc(doc(db, 'posts', activePostId, 'comments', commentId), { text: trimmed })
        .then(function () { toast('Comment updated.', 'info'); })
        .catch(function (error) {
            console.error('Comment update failed', error);
            toast('Failed to update comment.', 'error');
        });
};

window.confirmDeleteComment = async function (commentId) {
    const comment = threadComments.find(function (c) { return c.id === commentId; });
    if (!comment || comment.userId !== currentUser?.uid) {
        return toast('You can only delete your own comment.', 'error');
    }
    const ok = confirm('Delete this comment?');
    if (!ok) return;
    try {
        await deleteDoc(doc(db, 'posts', activePostId, 'comments', commentId));
        await refreshTopCommentPreview(activePostId);
        toast('Comment deleted.', 'info');
    } catch (error) {
        console.error('Comment delete failed', error);
        toast('Failed to delete comment.', 'error');
    }
};

async function refreshTopCommentPreview(postId) {
    if (!postId) return;
    const card = document.getElementById(`post-card-${postId}`);
    if (!card) return;
    const cardContent = card.querySelector('.card-content');
    if (!cardContent) return;
    let previewContainer = cardContent.querySelector('.post-comment-preview');
    if (!previewContainer) {
        previewContainer = document.createElement('div');
        previewContainer.className = 'post-comment-preview';
        previewContainer.style.marginTop = '10px';
        previewContainer.style.padding = '8px';
        previewContainer.style.background = 'rgba(255,255,255,0.05)';
        previewContainer.style.borderRadius = '8px';
        previewContainer.style.fontSize = '0.85rem';
        previewContainer.style.color = 'var(--text-muted)';
        previewContainer.style.display = 'flex';
        previewContainer.style.gap = '6px';
        cardContent.appendChild(previewContainer);
    }
    try {
        const commentsRef = collection(db, 'posts', postId, 'comments');
        const commentsSnap = await getDocs(query(commentsRef, orderBy('timestamp', 'desc'), limit(1)));
        if (commentsSnap.empty) {
            previewContainer.style.display = 'none';
            previewContainer.innerHTML = '';
            return;
        }
        const top = commentsSnap.docs[0].data() || {};
        const author = top.userId ? (getCachedUser(top.userId) || {}) : {};
        const authorName = resolveDisplayName(author) || author.username || top.author || top.authorName || 'Someone';
        const text = top.text || '';
        const likes = Number(top.likes || 0);
        previewContainer.style.display = 'flex';
        previewContainer.innerHTML = `
            <span style="font-weight:bold; color:var(--text-main);">${escapeHtml(authorName)}:</span>
            <span>${escapeHtml(text)}</span>
            ${likes ? `<span style="margin-left:auto; font-size:0.75rem; display:flex; align-items:center; gap:3px;"><i class="ph-fill ph-thumbs-up"></i> ${likes}</span>` : ''}
        `;
    } catch (error) {
        console.warn('Failed to refresh comment preview', error);
    }
}

window.shareComment = function (commentId) {
    const url = `${window.location.origin}/view-thread/${encodeURIComponent(activePostId)}#comment-${encodeURIComponent(commentId)}`;
    navigator.clipboard?.writeText(url).then(function () {
        toast('Comment link copied', 'info');
    }).catch(function () {
        toast('Unable to copy link.', 'error');
    });
};

window.reportContent = async function (item, type) {
    if (!requireAuth()) return;
    const reason = prompt('Why are you reporting this?') || '';
    if (!reason.trim()) return;
    const reportsRef = collection(db, 'reports', type);
    try {
        await addDoc(reportsRef, {
            contentId: item.id,
            contentType: type,
            reportedBy: currentUser.uid,
            reason: reason.trim().slice(0, 500),
            createdAt: serverTimestamp()
        });
        toast('Thank you for your report.', 'info');
    } catch (error) {
        console.error('Report failed', error);
        toast('Could not submit report.', 'error');
    }
};

window.toggleCommentLike = async function (commentId, event) {
    if (event) event.stopPropagation();
    if (!activePostId || !currentUser) return;
    const commentRef = doc(db, 'posts', activePostId, 'comments', commentId);
    const btn = event?.currentTarget;
    let comment = threadComments.find(function (c) { return c.id === commentId; });

    if (!comment) {
        try {
            const snap = await getDoc(commentRef);
            if (snap.exists()) comment = { id: commentId, ...snap.data() };
        } catch (e) { console.error(e); }
    }

    let likes = comment?.likes || 0;
    let dislikes = comment?.dislikes || 0;
    let likedBy = comment?.likedBy ? comment.likedBy.slice() : [];
    let dislikedBy = comment?.dislikedBy ? comment.dislikedBy.slice() : [];

    const wasLiked = likedBy.includes(currentUser.uid) || (btn?.dataset.liked === 'true');
    const hadDisliked = dislikedBy.includes(currentUser.uid) || (btn?.dataset.disliked === 'true');

    if (wasLiked) {
        likes = Math.max(0, likes - 1);
        likedBy = likedBy.filter(function (uid) { return uid !== currentUser.uid; });
    } else {
        likes = likes + 1;
        if (!likedBy.includes(currentUser.uid)) likedBy.push(currentUser.uid);
        if (hadDisliked) {
            dislikes = Math.max(0, dislikes - 1);
            dislikedBy = dislikedBy.filter(function (uid) { return uid !== currentUser.uid; });
        }
    }

    if (comment) {
        comment.likes = likes;
        comment.dislikes = dislikes;
        comment.likedBy = likedBy;
        comment.dislikedBy = dislikedBy;
    }

    const isLikedNow = likedBy.includes(currentUser.uid);
    const isDislikedNow = dislikedBy.includes(currentUser.uid);
    updateCommentReactionUI(commentId, likes, dislikes, isLikedNow, isDislikedNow);

    try {
        if (wasLiked) {
            await updateDoc(commentRef, { likes: increment(-1), likedBy: arrayRemove(currentUser.uid) });
        } else {
            const payload = { likes: increment(1), likedBy: arrayUnion(currentUser.uid) };
            if (hadDisliked) {
                payload.dislikes = increment(-1);
                payload.dislikedBy = arrayRemove(currentUser.uid);
            }
            await updateDoc(commentRef, payload);
        }
    } catch (e) { console.error(e); }
}

window.toggleDislike = async function (postId, event) {
    if (event) event.stopPropagation();
    if (!currentUser) return alert("Please log in to dislike posts.");

    const post = allPosts.find(function (p) { return p.id === postId; });
    if (!post) return;

    const wasDisliked = post.dislikedBy && post.dislikedBy.includes(currentUser.uid);
    const hadLiked = post.likedBy && post.likedBy.includes(currentUser.uid);

    if (wasDisliked) {
        post.dislikes = Math.max(0, (post.dislikes || 0) - 1);
        post.dislikedBy = (post.dislikedBy || []).filter(function (uid) { return uid !== currentUser.uid; });
    } else {
        post.dislikes = (post.dislikes || 0) + 1;
        if (!post.dislikedBy) post.dislikedBy = [];
        post.dislikedBy.push(currentUser.uid);
        if (hadLiked) {
            post.likes = Math.max(0, (post.likes || 0) - 1);
            post.likedBy = (post.likedBy || []).filter(function (uid) { return uid !== currentUser.uid; });
        }
    }

    recordTagAffinity(post.tags, wasDisliked ? 1 : -1);

    refreshSinglePostUI(postId);
    const postRef = doc(db, 'posts', postId);
    try {
        if (wasDisliked) {
            await updateDoc(postRef, { dislikes: increment(-1), dislikedBy: arrayRemove(currentUser.uid) });
        } else {
            const updatePayload = { dislikes: increment(1), dislikedBy: arrayUnion(currentUser.uid) };
            if (hadLiked) {
                updatePayload.likes = increment(-1);
                updatePayload.likedBy = arrayRemove(currentUser.uid);
            }
            await updateDoc(postRef, updatePayload);
        }
    } catch (e) { console.error('Dislike error:', e); }
}

window.toggleCommentDislike = async function (commentId, event) {
    if (event) event.stopPropagation();
    if (!activePostId || !currentUser) return;

    const commentRef = doc(db, 'posts', activePostId, 'comments', commentId);
    const btn = event?.currentTarget;

    let comment = threadComments.find(function (c) { return c.id === commentId; });

    if (!comment) {
        try {
            const snap = await getDoc(commentRef);
            if (snap.exists()) comment = { id: commentId, ...snap.data() };
        } catch (e) { console.error(e); }
    }

    let likes = comment?.likes || 0;
    let dislikes = comment?.dislikes || 0;
    let likedBy = Array.isArray(comment?.likedBy) ? comment.likedBy.slice() : [];
    let dislikedBy = Array.isArray(comment?.dislikedBy) ? comment.dislikedBy.slice() : [];

    const wasDisliked = dislikedBy.includes(currentUser.uid) || (btn?.dataset.disliked === 'true');
    const hadLiked = likedBy.includes(currentUser.uid) || (btn?.dataset.liked === 'true');

    if (wasDisliked) {
        dislikes = Math.max(0, dislikes - 1);
        dislikedBy = dislikedBy.filter(function (uid) { return uid !== currentUser.uid; });
    } else {
        dislikes = dislikes + 1;
        if (!dislikedBy.includes(currentUser.uid)) dislikedBy.push(currentUser.uid);

        if (hadLiked) {
            likes = Math.max(0, likes - 1);
            likedBy = likedBy.filter(function (uid) { return uid !== currentUser.uid; });
        }
    }

    // update local cache + UI immediately
    if (comment) {
        comment.likes = likes;
        comment.dislikes = dislikes;
        comment.likedBy = likedBy;
        comment.dislikedBy = dislikedBy;
    }

    const isLikedNow = likedBy.includes(currentUser.uid);
    const isDislikedNow = dislikedBy.includes(currentUser.uid);
    updateCommentReactionUI(commentId, likes, dislikes, isLikedNow, isDislikedNow);

    try {
        if (wasDisliked) {
            await updateDoc(commentRef, {
                dislikes: increment(-1),
                dislikedBy: arrayRemove(currentUser.uid)
            });
        } else {
            const payload = {
                dislikes: increment(1),
                dislikedBy: arrayUnion(currentUser.uid)
            };
            if (hadLiked) {
                payload.likes = increment(-1);
                payload.likedBy = arrayRemove(currentUser.uid);
            }
            await updateDoc(commentRef, payload);
        }
    } catch (e) {
        console.error(e);
    }
};


// --- Discovery & Search ---
function updateSearchQueryParam(value) {
    const url = new URL(window.location.href);
    const normalized = (value || '').trim();
    if (normalized) {
        url.searchParams.set(SEARCH_QUERY_KEY, normalized);
    } else {
        url.searchParams.delete(SEARCH_QUERY_KEY);
    }
    const next = `${url.pathname}${url.searchParams.toString() ? `?${url.searchParams.toString()}` : ''}${url.hash}`;
    if (window.NexeraRouter?.replaceStateSilently) {
        window.NexeraRouter.replaceStateSilently(next);
    } else {
        history.replaceState({}, '', next);
    }
}

function captureInputSelection(input) {
    if (!input) return null;
    return {
        start: input.selectionStart ?? null,
        end: input.selectionEnd ?? null
    };
}

function restoreInputSelection(input, selection) {
    if (!input || !selection || selection.start === null || selection.end === null) return;
    try {
        input.setSelectionRange(selection.start, selection.end);
    } catch (e) {
        // Ignore restore errors for unsupported inputs.
    }
}

function renderDiscoverTopBar() {
    const container = document.getElementById('discover-topbar');
    if (!container) return;
    const topBar = buildTopBar({
        title: 'Discover',
        searchPlaceholder: 'Search users, posts...',
        searchValue: discoverSearchTerm,
        onSearch: function (event) { window.handleSearchInput(event); },
        filters: [
            { label: 'All Results', dataset: { filter: 'All Results' }, active: discoverFilter === 'All Results', onClick: function () { window.setDiscoverFilter('All Results'); } },
            { label: 'Posts', dataset: { filter: 'Posts' }, active: discoverFilter === 'Posts', onClick: function () { window.setDiscoverFilter('Posts'); } },
        { label: 'Topics', dataset: { filter: 'Categories' }, active: discoverFilter === 'Categories', onClick: function () { window.setDiscoverFilter('Categories'); } },
            { label: 'Users', dataset: { filter: 'Users' }, active: discoverFilter === 'Users', onClick: function () { window.setDiscoverFilter('Users'); } },
            { label: 'Videos', dataset: { filter: 'Videos' }, active: discoverFilter === 'Videos', onClick: function () { window.setDiscoverFilter('Videos'); } },
            { label: 'Livestreams', dataset: { filter: 'Livestreams' }, active: discoverFilter === 'Livestreams', onClick: function () { window.setDiscoverFilter('Livestreams'); } }
        ],
        dropdowns: [
            {
                id: 'discover-post-sort',
                className: 'discover-dropdown',
                forId: 'posts-sort-select',
                label: 'Sort:',
                options: [
                    { value: 'recent', label: 'Recent' },
                    { value: 'popular', label: 'Popular' }
                ],
                selected: discoverPostsSort,
                onChange: function (event) { window.handlePostsSortChange(event); },
                show: discoverFilter === 'Posts'
            },
            {
                id: 'discover-category-sort',
                className: 'discover-dropdown',
                forId: 'categories-sort-select',
                label: 'Topics:',
                options: [
                    { value: 'verified_first', label: 'Verified first' },
                    { value: 'verified_only', label: 'Verified only' },
                    { value: 'community_first', label: 'Community first' },
                    { value: 'community_only', label: 'Community only' }
                ],
                selected: discoverCategoriesMode,
                onChange: function (event) { window.handleCategoriesModeChange(event); },
                show: discoverFilter === 'Categories'
            }
        ],
        actions: [
            { element: buildActionButton('Clear filters', 'ph ph-eraser', function () { window.clearDiscoverFilters(); }) },
            { element: buildActionButton('Refresh', 'ph ph-arrow-clockwise', function () { window.handleUiStubAction?.('discover-refresh'); }) }
        ]
    });
    container.innerHTML = '';
    container.appendChild(topBar);
}

async function renderDiscoverResults() {
    const container = document.getElementById('discover-results');
    container.innerHTML = "";

    let hub = document.getElementById('discover-hub');
    if (!hub) {
        hub = document.createElement('div');
        hub.id = 'discover-hub';
        container.appendChild(hub);
    }
    if (!discoverSearchTerm && discoverFilter === 'All Results') {
        renderDiscoverHub(hub);
    } else {
        hub.innerHTML = '';
    }

    const postsSelect = document.getElementById('posts-sort-select');
    if (postsSelect) postsSelect.value = discoverPostsSort;
    const categoriesSelect = document.getElementById('categories-sort-select');
    if (categoriesSelect) categoriesSelect.value = discoverCategoriesMode;

    const categoriesDropdown = function (id = 'section') {
        return `<div class="discover-dropdown"><label for="categories-${id}-select">Topics:</label><select id="categories-${id}-select" class="discover-select" onchange="window.handleCategoriesModeChange(event)">
            <option value="verified_first" ${discoverCategoriesMode === 'verified_first' ? 'selected' : ''}>Verified first</option>
            <option value="verified_only" ${discoverCategoriesMode === 'verified_only' ? 'selected' : ''}>Verified only</option>
            <option value="community_first" ${discoverCategoriesMode === 'community_first' ? 'selected' : ''}>Community first</option>
            <option value="community_only" ${discoverCategoriesMode === 'community_only' ? 'selected' : ''}>Community only</option>
        </select></div>`;
    };

    const renderVideosSection = async function (onlyVideos = false, useCarousels = false) {
        if (!videosCache.length) {
            const snap = await getDocs(query(collection(db, 'videos'), orderBy('createdAt', 'desc')));
            videosCache = snap.docs.map(function (d) { return ({ id: d.id, ...d.data() }); });
        }
        let filteredVideos = videosCache;
        if (discoverSearchTerm) {
            filteredVideos = filteredVideos.filter(function (v) {
                return (v.caption || '').toLowerCase().includes(discoverSearchTerm) ||
                    (v.hashtags || []).some(function (tag) { return (`#${tag}`).toLowerCase().includes(discoverSearchTerm); });
            });
        }
        if (filteredVideos.length === 0) {
            if (onlyVideos) container.innerHTML = `<div class="empty-state"><p>No videos found.</p></div>`;
            return;
        }

        const header = document.createElement('div');
        header.className = 'discover-section-header';
        header.textContent = 'Videos';
        container.appendChild(header);
        const row = document.createElement('div');
        row.className = useCarousels ? 'discover-carousel no-scrollbar' : 'discover-vertical-list';
        filteredVideos.forEach(function (video) {
            const tags = (video.hashtags || []).map(function (t) { return '#' + t; }).join(' ');
            const card = document.createElement('div');
            card.className = 'social-card';
            card.style.cssText = 'padding:1rem; cursor:pointer; display:flex; gap:12px; align-items:flex-start;';
            card.onclick = function () { window.navigateTo('videos'); };
            card.innerHTML = `
                <div style="width:120px; height:70px; background:linear-gradient(135deg, #0f1f3a, #0adfe4); border-radius:10px; display:flex; align-items:center; justify-content:center; color:#aaf; font-weight:700;">
                    <i class="ph-fill ph-play-circle" style="font-size:2rem;"></i>
                </div>
                <div style="flex:1;">
                    <div style="font-weight:800; margin-bottom:4px;">${escapeHtml(video.caption || 'Untitled video')}</div>
                    <div style="color:var(--text-muted); font-size:0.9rem;">${tags}</div>
                    <div style="color:var(--text-muted); font-size:0.8rem; margin-top:4px;">Views: ${video.stats?.views || 0}</div>
                </div>
            `;
            row.appendChild(card);
        });
        container.appendChild(row);
    };

    const renderUsers = function (useCarousels = false) {
        let matches = [];
        if (discoverSearchTerm) {
            matches = Object.values(userCache).filter(function (u) {
                return (u.name && u.name.toLowerCase().includes(discoverSearchTerm)) ||
                    (u.username && u.username.toLowerCase().includes(discoverSearchTerm));
            });
        } else if (discoverFilter === 'All Results') {
            matches = Object.values(userCache).slice(0, 5);
        }

        if (matches.length > 0) {
            const header = document.createElement('div');
            header.className = 'discover-section-header';
            header.textContent = 'Users';
            container.appendChild(header);
            const row = document.createElement('div');
            row.className = useCarousels ? 'discover-carousel no-scrollbar' : 'discover-vertical-list';
            matches.forEach(function (user) {
                const uid = Object.keys(userCache).find(function (key) { return userCache[key] === user; });
                if (!uid) return;
                const avatarHtml = renderAvatar({ ...user, uid }, { size: 40 });

                const card = document.createElement('div');
                card.className = 'social-card';
                card.style.cssText = 'padding:1rem; cursor:pointer; display:flex; align-items:center; gap:10px; border-left: 4px solid var(--border);';
                card.onclick = function () { window.openUserProfile(uid); };
                card.innerHTML = `
                    ${avatarHtml}
                    <div>
                        <div style="font-weight:700;">${escapeHtml(user.name)}</div>
                        <div style="color:var(--text-muted); font-size:0.9rem;">@${escapeHtml(user.username)}</div>
                    </div>
                    <button class="follow-btn" style="margin-left:auto; padding:10px;">View</button>
                `;
                row.appendChild(card);
            });
            container.appendChild(row);
        } else if (discoverFilter === 'Users' && discoverSearchTerm) {
            container.innerHTML = `<div class="empty-state"><p>No users matching "${discoverSearchTerm}"</p></div>`;
        }
    };

    const renderLiveSection = function (useCarousels = false) {
        if (MOCK_LIVESTREAMS.length > 0) {
            const header = document.createElement('div');
            header.className = 'discover-section-header';
            header.textContent = 'Livestreams';
            container.appendChild(header);
            const row = document.createElement('div');
            row.className = useCarousels ? 'discover-carousel no-scrollbar' : 'discover-vertical-list';
            MOCK_LIVESTREAMS.forEach(function (stream) {
                const card = document.createElement('div');
                card.className = 'social-card';
                card.style.cssText = `padding:1rem; display:flex; gap:10px; border-left: 4px solid ${stream.color};`;
                card.innerHTML = `
                    <div style="width:80px; height:50px; background:${stream.color}; border-radius:6px; display:flex; align-items:center; justify-content:center; color:black; font-weight:900; font-size:1.5rem;"><i class="ph-fill ph-broadcast" style="margin-right:8px;"></i> LIVE</div>
                    <div style="padding:1rem;">
                        <h3 style="font-weight:700; font-size:1.1rem; margin-bottom:5px; color:var(--text-main);">${stream.title}</h3>
                        <div style="display:flex; justify-content:space-between; align-items:center;">
                            <div style="font-size:0.9rem; color:var(--text-muted);">@${stream.author}</div>
                            <div style="color:#ff3d3d; font-weight:bold; font-size:0.8rem; display:flex; align-items:center; gap:4px;"><i class="ph-fill ph-circle"></i> ${stream.viewerCount}</div>
                        </div>
                        <div class="category-badge" style="margin-top:10px;">${stream.category}</div>
                    </div>
                `;
                row.appendChild(card);
            });
            container.appendChild(row);
        }
    };

    const renderPostsSection = function (useCarousels = false) {
        let filteredPosts = allPosts;
        filteredPosts = filteredPosts.filter(function (post) {
            if (isPostScheduledInFuture(post) && (!currentUser || post.userId !== currentUser.uid)) return false;
            return true;
        });
        if (discoverSearchTerm) {
            filteredPosts = allPosts.filter(function (p) {
                const body = typeof p.content === 'string' ? p.content : (p.content?.text || '');
                return (p.title || '').toLowerCase().includes(discoverSearchTerm) || body.toLowerCase().includes(discoverSearchTerm);
            });
        }

        if (discoverPostsSort === 'popular') {
            filteredPosts.sort(function (a, b) { return (b.likes || 0) - (a.likes || 0); });
        } else {
            filteredPosts.sort(function (a, b) { return (b.timestamp?.seconds || 0) - (a.timestamp?.seconds || 0); });
        }

        if (filteredPosts.length > 0) {
            const header = document.createElement('div');
            header.className = 'discover-section-header';
            header.textContent = 'Posts';
            container.appendChild(header);
            const row = document.createElement('div');
            row.className = useCarousels ? 'discover-carousel no-scrollbar' : 'discover-vertical-list';
            filteredPosts.forEach(function (post) {
                const author = userCache[post.userId] || { name: post.author };
                const body = typeof post.content === 'string' ? post.content : (post.content?.text || '');
                const isLiked = post.likedBy && currentUser && post.likedBy.includes(currentUser.uid);
                const isDisliked = post.dislikedBy && currentUser && post.dislikedBy.includes(currentUser.uid);
                const isSaved = userProfile.savedPosts && userProfile.savedPosts.includes(post.id);
                const myReview = window.myReviewCache ? window.myReviewCache[post.id] : null;
                const reviewDisplay = getReviewDisplay(myReview);
                const locationBadge = renderLocationBadge(post.location);
                const pollBlock = renderPollBlock(post);
                const accentColor = THEMES[post.category] || THEMES['For You'];
                const mobileView = isMobileViewport();
                const card = document.createElement('div');
                card.className = 'social-card';
                card.style.cssText = `border-left: 2px solid var(--card-accent); --card-accent: ${accentColor}; cursor:pointer;`;
                card.onclick = function () { window.openThread(post.id); };
                card.innerHTML = `
                    <div class="card-content" style="padding:1rem;">
                        <div style="display:flex; align-items:center; justify-content:space-between; gap:10px;">
                            <div class="category-badge">${post.category}</div>
                            <div style="display:flex; align-items:center; gap:8px; font-size:0.8rem; color:var(--text-muted);">
                                <span>by ${escapeHtml(author.name)}</span>
                                ${getPostOptionsButton(post, 'discover', '1rem')}
                            </div>
                        </div>
                        <h3 class="post-title">${escapeHtml(cleanText(post.title))}</h3>
                        <p style="font-size:0.9rem; color:var(--text-muted);">${escapeHtml(cleanText(body).substring(0, 100))}...</p>
                        ${locationBadge}
                        ${pollBlock}
                        ${renderPostActions(post, { isLiked, isDisliked, isSaved, reviewDisplay, iconSize: '1rem', showCounts: !mobileView, showLabels: !mobileView })}
                    </div>
                `;
                row.appendChild(card);
            });
            container.appendChild(row);
        } else if (discoverFilter === 'Posts' && discoverSearchTerm) {
            container.innerHTML = `<div class="empty-state"><p>No posts found.</p></div>`;
        }
    };

    const renderCategoriesSection = function (onlyCategories = false, useCarousels = false) {
        let filteredCategories = categories.slice();
        if (discoverSearchTerm) {
            const term = discoverSearchTerm.toLowerCase();
            filteredCategories = filteredCategories.filter(function (c) {
                return (c.name || '').toLowerCase().includes(term) || (c.slug || '').toLowerCase().includes(term) || (c.description || '').toLowerCase().includes(term);
            });
        }

        if (discoverCategoriesMode === 'verified_only') {
            filteredCategories = filteredCategories.filter(function (c) { return !!c.verified; });
        } else if (discoverCategoriesMode === 'community_only') {
            filteredCategories = filteredCategories.filter(function (c) { return (c.type || 'community') === 'community'; });
        }

        const sorted = filteredCategories.slice().sort(function (a, b) {
            const memberDiff = (b.memberCount || 0) - (a.memberCount || 0);
            if (discoverCategoriesMode === 'verified_first') {
                if (!!a.verified !== !!b.verified) return Number(b.verified) - Number(a.verified);
                return memberDiff;
            }
            if (discoverCategoriesMode === 'community_first') {
                const aComm = (a.type || 'community') === 'community';
                const bComm = (b.type || 'community') === 'community';
                if (aComm !== bComm) return Number(bComm) - Number(aComm);
                return memberDiff;
            }
            return memberDiff;
        });

        const visible = onlyCategories ? sorted : sorted.slice(0, 6);
        if (visible.length > 0) {
            const header = document.createElement('div');
            header.className = 'discover-section-header discover-section-row';
            header.innerHTML = `<span>Topics</span>${categoriesDropdown('section')}`;
            container.appendChild(header);
            const row = document.createElement('div');
            row.className = useCarousels ? 'discover-carousel no-scrollbar' : 'discover-vertical-list';
            visible.forEach(function (cat) {
                const verifiedMark = renderVerifiedBadge({ verified: cat.verified });
                const typeLabel = (cat.type || 'community') === 'community' ? 'Community' : 'Official';
                const memberLabel = typeof cat.memberCount === 'number' ? `${cat.memberCount} members` : '';
                const topicLabel = cat.name || cat.slug || cat.id || 'Topic';
                const topicClass = topicLabel.replace(/[^a-zA-Z0-9]/g, '');
                const isFollowingTopic = followedCategories.has(topicLabel);
                const topicArg = topicLabel.replace(/'/g, "\\'");
                const followLabel = isFollowingTopic ? 'Following' : '<i class="ph-bold ph-plus"></i> Topic';
                const followClass = isFollowingTopic ? 'following' : '';
                const followButton = `<button class="follow-btn js-follow-topic-${topicClass} ${followClass}" data-topic="${escapeHtml(topicLabel)}" onclick="event.stopPropagation(); window.toggleFollow('${topicArg}', event)" style="padding:8px 12px;">${followLabel}</button>`;
                const accentColor = cat.verified ? '#00f2ea' : 'var(--border)';
                const card = document.createElement('div');
                card.className = 'social-card';
                card.style.cssText = `padding:1rem; display:flex; gap:12px; align-items:center; border-left: 2px solid var(--card-accent); --card-accent: ${accentColor};`;
                card.innerHTML = `
                    <div class="user-avatar" style="width:46px; height:46px; background:${getColorForUser(cat.name || 'C')};">${(cat.name || 'C')[0]}</div>
                    <div style="flex:1;">
                        <div style="font-weight:800; display:flex; align-items:center; gap:6px;">${escapeHtml(cat.name || 'Topic')}${verifiedMark}</div>
                        <div style="color:var(--text-muted); font-size:0.9rem; display:flex; gap:10px; align-items:center; flex-wrap:wrap;">${escapeHtml(typeLabel)}${memberLabel ? ' · ' + memberLabel : ''}</div>
                    </div>
                    <div style="display:flex; flex-direction:column; align-items:flex-end; gap:10px;">
                        <div class="category-badge">${escapeHtml(cat.slug || cat.id || '')}</div>
                        ${followButton}
                    </div>
                `;
                row.appendChild(card);
            });
            container.appendChild(row);
        } else if (discoverFilter === 'Categories' && discoverSearchTerm) {
            container.innerHTML = `<div class="empty-state"><p>No categories found.</p></div>`;
        }
    };

    const useCarousels = !discoverSearchTerm;

    if (discoverFilter === 'All Results') {
        renderLiveSection(useCarousels);
        renderUsers(useCarousels);
        renderPostsSection(useCarousels);
        renderCategoriesSection(false, useCarousels);
        await renderVideosSection(false, useCarousels);
        if (container.innerHTML === "") container.innerHTML = `<div class="empty-state"><p>Start typing to search everything.</p></div>`;
    } else if (discoverFilter === 'Users') {
        renderUsers(false);
    } else if (discoverFilter === 'Livestreams') {
        renderLiveSection(false);
    } else if (discoverFilter === 'Videos') {
        await renderVideosSection(true, false);
    } else if (discoverFilter === 'Categories') {
        renderCategoriesSection(true, false);
    } else {
        renderPostsSection(false);
    }

    applyMyReviewStylesToDOM();
}

window.renderDiscover = async function () {
    renderDiscoverTopBar();
    await renderDiscoverResults();
}

// --- Profile Rendering ---
window.openUserProfile = async function (uid, event, pushToStack = true) {
    if (event) event.stopPropagation();
    if (uid === getViewerUid()) {
        window.navigateTo('profile', pushToStack);
        return;
    }

    viewingUserId = uid;
    currentProfileFilter = 'All Results';
    window.navigateTo('public-profile', pushToStack);

    let profile = userCache[uid];
    if (!profile) {
        profile = await resolveUserProfile(uid);
    }
    renderPublicProfile(uid, profile);
}

window.openUserProfileByHandle = async function (handle) {
    const normalized = (handle || '').replace(/^@/, '').toLowerCase();
    const cachedEntry = Object.entries(userCache).find(function ([_, data]) { return (data.username || '').toLowerCase() === normalized; });
    if (cachedEntry) {
        openUserProfile(cachedEntry[0], null, true);
        return;
    }
    const q = query(collection(db, 'profiles'), where('username', '==', normalized));
    const snap = await getDocs(q);
    if (!snap.empty) {
        const docSnap = snap.docs[0];
        storeUserInCache(docSnap.id, docSnap.data());
        openUserProfile(docSnap.id, null, true);
    }
}

const PROFILE_FILTER_OPTIONS = ['All Results', 'Posts', 'Categories', 'Users', 'Videos', 'Livestreams'];

function renderProfileFilterRow(uid, ariaLabel = 'Profile filters') {
    const buttons = PROFILE_FILTER_OPTIONS.map(function (label) {
        const active = currentProfileFilter === label;
        const safeLabel = label.replace(/'/g, "\\'");
        const displayLabel = label === 'Categories' ? 'Topics' : label;
        return `<button class="discover-pill ${active ? 'active' : ''}" role="tab" aria-selected="${active}" onclick="window.setProfileFilter('${safeLabel}', '${uid}')">${displayLabel}</button>`;
    }).join('');
    return `<div class="discover-pill-row profile-filter-row" role="tablist" aria-label="${ariaLabel}">${buttons}</div>`;
}

async function primeProfileMedia(uid, profile, isSelfView, containerId) {
    if (!uid || profileMediaPrefetching[uid] === 'loading' || profileMediaPrefetching[uid] === 'complete') return;
    profileMediaPrefetching[uid] = 'loading';
    try {
        const tasks = [];
        if (!videosCache.some(function (video) { return video.ownerId === uid; })) {
            tasks.push(getDocs(query(collection(db, 'videos'), where('ownerId', '==', uid), orderBy('createdAt', 'desc'), limit(12))).then(function (snap) {
                const newVideos = snap.docs.map(function (d) { return ({ id: d.id, ...d.data() }); });
                videosCache = newVideos.concat(videosCache);
            }));
        }
        if (!liveSessionsCache.some(function (session) { return (session.hostId || session.author) === uid; })) {
            const liveQuery = query(collection(db, 'liveStreams'), where('hostId', '==', uid), limit(12));
            tasks.push(getDocs(liveQuery).then(function (snap) {
                const additions = snap.docs.map(function (d) {
                    const data = d.data();
                    const playbackUrl = data.playbackUrl || data.streamUrl || data.streamEmbedURL || '';
                    return ({
                        id: d.id,
                        ...data,
                        streamUrl: data.streamUrl || playbackUrl,
                        streamEmbedURL: data.streamEmbedURL || playbackUrl,
                        tags: Array.isArray(data.tags) ? data.tags : [],
                    });
                }).sort(function (a, b) { return (b.startedAt?.toMillis?.() || b.createdAt?.toMillis?.() || 0) - (a.startedAt?.toMillis?.() || a.createdAt?.toMillis?.() || 0); });
                const existingIds = new Set(liveSessionsCache.map(function (s) { return s.id; }));
                liveSessionsCache = liveSessionsCache.concat(additions.filter(function (s) { return !existingIds.has(s.id); }));
            }));
        }
        await Promise.all(tasks);
    } catch (e) {
        console.error('Profile media fetch failed', e);
    }
    profileMediaPrefetching[uid] = 'complete';
    const stillViewing = (currentViewId === 'profile' && currentUser?.uid === uid) || (currentViewId === 'public-profile' && viewingUserId === uid);
    if (stillViewing) renderProfileContent(uid, profile, isSelfView, containerId);
}

function getProfileContentSources(uid) {
    const posts = allPosts
        .filter(function (p) { return p.userId === uid; })
        .sort(function (a, b) { return (b.timestamp?.seconds || 0) - (a.timestamp?.seconds || 0); });
    const videos = videosCache.filter(function (video) { return video.ownerId === uid; });
    const liveSessions = liveSessionsCache.filter(function (session) { return (session.hostId || session.author) === uid; });

    const categoriesForProfile = [];
    const seenCategories = new Set();

    if (uid === currentUser?.uid) {
        Object.keys(memberships || {}).forEach(function (catId) {
            const snapshot = getCategorySnapshot(catId);
            if (snapshot && !seenCategories.has(catId)) {
                categoriesForProfile.push({
                    id: catId,
                    name: snapshot.name || snapshot.title || snapshot.slug || 'Topic',
                    color: THEMES[snapshot.name] || THEMES[snapshot.slug] || ''
                });
                seenCategories.add(catId);
            }
        });
    }

    if (!categoriesForProfile.length) {
        posts.forEach(function (post) {
            const cid = post.categoryId || post.category;
            const label = post.category || post.categoryName || cid;
            if (cid && label && !seenCategories.has(cid)) {
                categoriesForProfile.push({ id: cid, name: label, color: THEMES[label] || '' });
                seenCategories.add(cid);
            }
        });
    }

    return { posts, videos, liveSessions, categories: categoriesForProfile };
}

function renderProfilePostCard(post, context = 'profile', { compact = false, idPrefix = 'post' } = {}) {
    const date = formatDateTime(post.timestamp) || 'Just now';
    const isLiked = post.likedBy && post.likedBy.includes(currentUser?.uid);
    const isDisliked = post.dislikedBy && post.dislikedBy.includes(currentUser?.uid);
    const isSaved = userProfile.savedPosts && userProfile.savedPosts.includes(post.id);
    const myReview = window.myReviewCache ? window.myReviewCache[post.id] : null;
    const reviewDisplay = getReviewDisplay(myReview);
    const postText = typeof post.content === 'object' && post.content !== null ? (post.content.text || '') : (post.content || '');
    const formattedBody = compact ? escapeHtml(cleanText(postText)).slice(0, 160) + (postText.length > 160 ? '…' : '') : formatContent(postText, post.tags, post.mentions);
    const tagListHtml = compact ? '' : renderTagList(post.tags || []);
    const pollBlock = compact ? '' : renderPollBlock(post);
    const locationBadge = renderLocationBadge(post.location);
    const scheduledChip = isPostScheduledInFuture(post) && currentUser && post.userId === currentUser.uid ? `<div class="scheduled-chip">Scheduled for ${formatTimestampDisplay(post.scheduledFor)}</div>` : '';
    let mediaContent = '';
    if (post.mediaUrl && compact) {
        mediaContent = `<div class="profile-card-media" style="background-image:url('${post.mediaUrl}')"></div>`;
    } else if (post.mediaUrl) {
        mediaContent = post.type === 'video'
            ? `<div class="video-container" onclick="window.openFullscreenMedia('${post.mediaUrl}', 'video')"><video src="${post.mediaUrl}" controls class="post-media"></video></div>`
            : `<img src="${post.mediaUrl}" class="post-media" alt="Post Content" onclick="window.openFullscreenMedia('${post.mediaUrl}', 'image')">`;
    }

    const cardClass = compact ? 'social-card profile-collage-card' : 'social-card';
    const bodyPreview = compact
        ? `<p class="profile-card-body post-body-text">${formattedBody}</p>`
        : `<p class="post-body-text">${formattedBody}</p>`;
    const accentColor = THEMES[post.category] || THEMES[currentCategory] || THEMES['For You'];
    const mobileView = isMobileViewport();

    return `
        <div class="${cardClass}" style="border-left: 2px solid var(--card-accent); --card-accent: ${accentColor};${compact ? 'min-width:260px;' : ''}">
            <div class="card-content" style="padding-top:1rem; cursor: pointer;" onclick="window.openThread('${post.id}')">
                <div style="display:flex; align-items:center; justify-content:space-between; gap:10px;">
                    <div class="category-badge">${escapeHtml(post.category || '')}</div>
                    <div style="display:flex; align-items:center; gap:8px;">
                        <span style="font-size:0.8rem; color:var(--text-muted);">${date}</span>
                        ${getPostOptionsButton(post, context, compact ? '0.95rem' : '1rem')}
                    </div>
                </div>
                <h3 class="post-title">${escapeHtml(cleanText(post.title))}</h3>
                ${bodyPreview}
                ${tagListHtml}
                ${locationBadge}
                ${scheduledChip}
                ${pollBlock}
                ${mediaContent}
            </div>
            ${renderPostActions(post, { isLiked, isDisliked, isSaved, reviewDisplay, iconSize: compact ? '0.95rem' : '1rem', idPrefix, showCounts: !mobileView, showLabels: !mobileView })}
        </div>
    `;
}

function renderProfileVideoCard(video, { compact = true } = {}) {
    const poster = video.thumbURL || video.videoURL || '';
    const caption = escapeHtml(video.caption || 'Untitled video');
    const views = getVideoViewCount(video);
    const minWidth = compact ? 'min-width:240px;' : '';
    return `<div class="social-card profile-collage-card" style="${minWidth}">
        <div class="profile-video-thumb" style="background-image:url('${poster}')" onclick="window.openVideoDetail('${video.id}')">
            <div class="profile-video-meta">${formatCompactNumber(views)} views</div>
        </div>
        <div class="card-content" style="gap:6px;">
            <div style="font-weight:700;">${caption}</div>
            <div style="color:var(--text-muted); font-size:0.85rem;">${(video.hashtags || []).map(function (t) { return '#' + t; }).join(' ')}</div>
        </div>
    </div>`;
}

function renderProfileLiveCard(session, { compact = true } = {}) {
    const title = escapeHtml(session.title || 'Live session');
    const status = (session.status || 'live').toUpperCase();
    const viewers = session.viewerCount || session.stats?.viewerCount || 0;
    const minWidth = compact ? 'min-width:220px;' : '';
    return `<div class="social-card profile-collage-card" style="${minWidth}">
        <div class="card-content" style="gap:6px;">
            <div style="display:flex; align-items:center; gap:8px;">${status === 'LIVE' ? '<span class="live-dot"></span>' : ''}<span style="font-weight:700;">${title}</span></div>
            <div style="color:var(--text-muted); font-size:0.85rem;">${viewers} watching</div>
        </div>
    </div>`;
}

function renderProfileCategoryChip(category) {
    return `<div class="category-badge" style="min-width:max-content;">${escapeHtml(category.name || 'Topic')}</div>`;
}

function renderProfileCollageRow(label, items, renderer, seeAllAction) {
    if (!items || !items.length) return '';
    const headerAction = seeAllAction ? `<button class="link-btn" onclick="${seeAllAction}">See all</button>` : '';
    return `
        <div class="profile-section">
            <div class="discover-section-row">
                <span>${label}</span>
                ${headerAction}
            </div>
            <div class="profile-h-scroll">${items.map(renderer).join('')}</div>
        </div>`;
}

function renderProfilePostsList(container, posts, context) {
    if (!container) return;
    if (!posts.length) { container.innerHTML = `<div class="empty-state"><p>No posts yet.</p></div>`; return; }
    container.innerHTML = posts.map(function (post) {
        return renderProfilePostCard(post, context, { compact: false, idPrefix: `${context}-post` });
    }).join('');
}

function renderProfileAllResults(container, sources, uid, isSelfView) {
    if (!container) return;
    const sections = [];
    const postContext = isSelfView ? 'profile' : 'public-profile';
    const postPrefix = isSelfView ? 'my-profile-collage' : 'profile-collage';
    sections.push(renderProfileCollageRow('Posts', sources.posts.slice(0, 10), function (post) { return renderProfilePostCard(post, postContext, { compact: true, idPrefix: postPrefix }); }, `window.setProfileFilter('Posts', '${uid}')`));
    sections.push(renderProfileCollageRow('Videos', sources.videos.slice(0, 10), function (video) { return renderProfileVideoCard(video, { compact: true }); }, `window.setProfileFilter('Videos', '${uid}')`));
    sections.push(renderProfileCollageRow('Livestreams', sources.liveSessions.slice(0, 10), function (session) { return renderProfileLiveCard(session, { compact: true }); }, `window.setProfileFilter('Livestreams', '${uid}')`));
    sections.push(renderProfileCollageRow('Topics', sources.categories.slice(0, 12), renderProfileCategoryChip, `window.setProfileFilter('Categories', '${uid}')`));

    container.innerHTML = sections.filter(Boolean).join('');
    if (!container.innerHTML) {
        container.innerHTML = `<div class="empty-state"><p>Nothing to show yet.</p></div>`;
    }
}

function renderProfileContent(uid, profile, isSelfView, containerId) {
    const container = document.getElementById(containerId);
    if (!container) return;
    const sources = getProfileContentSources(uid);

    if ((!sources.videos.length || !sources.liveSessions.length) && profileMediaPrefetching[uid] !== 'loading' && profileMediaPrefetching[uid] !== 'complete') {
        primeProfileMedia(uid, profile, isSelfView, containerId);
    }

    if (currentProfileFilter === 'All Results') return renderProfileAllResults(container, sources, uid, isSelfView);
    if (currentProfileFilter === 'Posts') return renderProfilePostsList(container, sources.posts, isSelfView ? 'profile' : 'public-profile');
    if (currentProfileFilter === 'Videos') {
        if (!sources.videos.length) return container.innerHTML = `<div class="empty-state"><p>No videos yet.</p></div>`;
        container.innerHTML = `<div class="profile-media-grid">${sources.videos.map(function (video) { return renderProfileVideoCard(video, { compact: false }); }).join('')}</div>`;
        return;
    }
    if (currentProfileFilter === 'Livestreams') {
        if (!sources.liveSessions.length) return container.innerHTML = `<div class="empty-state"><p>No livestreams yet.</p></div>`;
        container.innerHTML = `<div class="profile-media-grid">${sources.liveSessions.map(function (session) { return renderProfileLiveCard(session, { compact: false }); }).join('')}</div>`;
        return;
    }
    if (currentProfileFilter === 'Categories') {
        if (!sources.categories.length) return container.innerHTML = `<div class="empty-state"><p>No categories yet.</p></div>`;
        container.innerHTML = `<div class="profile-h-scroll">${sources.categories.map(renderProfileCategoryChip).join('')}</div>`;
        return;
    }
    if (currentProfileFilter === 'Users') {
        container.innerHTML = `<div class="empty-state"><p>People results coming soon.</p></div>`;
    }
}

function applyProfileCover(headerEl, profile = {}, isSelfView = false) {
    if (!headerEl) return;
    const coverUrl = profile.coverUrl || profile.coverURL || profile.bannerUrl || profile.headerImage || profile.coverImage || '';
    const coverStyleRaw = (profile.coverStyle || profile.coverDisplay || 'banner').toString().toLowerCase();
    const coverStyle = coverStyleRaw === 'full' ? 'full' : 'banner';
    headerEl.classList.remove('has-cover', 'profile-cover-banner', 'profile-cover-full');
    headerEl.style.backgroundImage = '';
    if (coverUrl) {
        headerEl.classList.add('has-cover', coverStyle === 'full' ? 'profile-cover-full' : 'profile-cover-banner');
        headerEl.style.backgroundImage = `url('${coverUrl}')`;
    }
    const actionBtn = headerEl.querySelector('.profile-cover-action');
    if (actionBtn) {
        actionBtn.style.display = isSelfView ? 'inline-flex' : 'none';
        actionBtn.textContent = coverUrl ? 'Change cover' : 'Add cover';
    }
}

window.handleProfileCoverAction = function () {
    // TODO: Hook cover uploads into profile settings once backend is wired.
    const input = document.getElementById('profile-cover-input');
    if (input) {
        input.click();
        return;
    }
    toast('Cover updates coming soon.', 'info');
};

window.handleProfileCoverInput = function (event) {
    if (!event?.target?.files?.length) return;
    // UI-only: file selection placeholder until backend wiring.
    toast('Cover selected (save coming soon).', 'info');
};

function renderPublicProfile(uid, profileData = userCache[uid]) {
    if (!profileData) return;
    if (!PROFILE_FILTER_OPTIONS.includes(currentProfileFilter)) currentProfileFilter = 'All Results';
    const normalizedProfile = normalizeUserProfileData(profileData, profileData.uid || profileData.id || currentUser?.uid || '');
    const container = document.getElementById('view-public-profile');
    const sources = getProfileContentSources(uid);

    const avatarHtml = renderAvatar({ ...normalizedProfile, uid }, { size: 100, className: 'profile-pic' });

    const isFollowing = followedUsers.has(uid);
    const isSelfView = currentUser && currentUser.uid === uid;
    const userPosts = sources.posts;
    const likesTotal = userPosts.reduce(function (acc, p) { return acc + (p.likes || 0); }, 0);

    const followCta = isSelfView ? '' : `<button onclick=\"window.toggleFollowUser('${uid}', event)\" class=\"create-btn-sidebar js-follow-user-${uid}\" style=\"width: auto; padding: 0.6rem 2rem; margin-top: 0; background: ${isFollowing ? 'transparent' : 'var(--primary)'}; border: 1px solid var(--primary); color: ${isFollowing ? 'var(--primary)' : 'black'};\">${isFollowing ? 'Following' : 'Follow'}</button>`;

    let linkHtml = '';
    if (normalizedProfile.links) {
        let url = normalizedProfile.links;
        if (!url.startsWith('http')) url = 'https://' + url;
        linkHtml = `<a href="${url}" target="_blank" style="color: var(--primary); font-size: 0.9rem; text-decoration: none; margin-top: 5px; display: inline-block;">🔗 ${escapeHtml(normalizedProfile.links)}</a>`;
    }

    const followersCount = normalizedProfile.followersCount || 0;
    const verifiedBadge = renderVerifiedBadge(normalizedProfile, 'with-gap');

    container.innerHTML = `
        <div class="glass-panel" style="position: sticky; top: 0; z-index: 20; padding: 1rem; display: flex; align-items: center; gap: 15px;">
            <button onclick="window.goBack()" class="back-btn-outline" style="background: none; color: var(--text-main); cursor: pointer; display: flex; align-items: center; gap: 5px;"><span>←</span> Back</button>
            <h2 style="font-weight: 800; font-size: 1.2rem;">${escapeHtml(normalizedProfile.username)}</h2>
        </div>
        <div class="profile-header">
            <button class="profile-cover-action" type="button" onclick="window.handleProfileCoverAction()" style="display:none;">
                <i class="ph ph-image"></i> Add cover
            </button>
            <input id="profile-cover-input" type="file" accept="image/*" style="display:none;" onchange="window.handleProfileCoverInput(event)" />
            <div class="profile-header-content">
                ${avatarHtml}
                <h2 style="font-weight: 800; margin-bottom: 5px; display:flex; align-items:center; gap:6px;">${escapeHtml(normalizedProfile.name)}${verifiedBadge}</h2>
                <p style="color: var(--text-muted);">@${escapeHtml(normalizedProfile.username)}</p>
                <p style="margin-top: 10px; max-width: 400px; margin-left: auto; margin-right: auto;">${escapeHtml(normalizedProfile.bio || "No bio yet.")}</p>
                ${linkHtml}
                <div class="stats-row">
                    <div class="stat-item"><div id="profile-follower-count-${uid}">${followersCount}</div><div>Followers</div></div>
                    <div class="stat-item"><div>${likesTotal}</div><div>Likes</div></div>
                    <div class="stat-item"><div>${userPosts.length}</div><div>Posts</div></div>
                </div>
                <div style="display:flex; gap:10px; justify-content:center; margin-top:1rem;">
                    ${followCta}
                    ${isSelfView ? '' : `<button class=\"create-btn-sidebar\" style=\"width: auto; padding: 0.6rem 2rem; margin-top: 0; background: var(--bg-hover); color: var(--text-main); border: 1px solid var(--border);\" onclick=\"window.openOrStartDirectConversationWithUser('${uid}')\">Message</button>`}
                </div>
            </div>
        </div>
        <div class="profile-filters-bar">${renderProfileFilterRow(uid, 'Public profile filters')}</div>
        <div id="public-profile-content" class="profile-content-region"></div>`;

    renderProfileContent(uid, normalizedProfile, isSelfView, 'public-profile-content');
    applyProfileCover(container.querySelector('.profile-header'), normalizedProfile, isSelfView);
}

function renderProfile() {
    if (!PROFILE_FILTER_OPTIONS.includes(currentProfileFilter)) currentProfileFilter = 'All Results';
    const sources = getProfileContentSources(currentUser?.uid);
    const userPosts = sources.posts;
    const displayName = userProfile.name || userProfile.nickname || "Nexera User";
    const verifiedBadge = renderVerifiedBadge(userProfile, 'with-gap');
    const avatarHtml = renderAvatar({ ...userProfile, uid: currentUser?.uid }, { size: 100, className: 'profile-pic' });
    const showReturnBar = !!profileReturnContext || navStack.length > 0;

    let linkHtml = '';
    if (userProfile.links) {
        let url = userProfile.links;
        if (!url.startsWith('http')) url = 'https://' + url;
        linkHtml = `<a href="${url}" target="_blank" style="color: var(--primary); font-size: 0.9rem; text-decoration: none; margin-top: 5px; display: inline-flex; align-items:center; gap:5px;"> <i class="ph-bold ph-link"></i> ${escapeHtml(userProfile.links)}</a>`;
    }

    const followersCount = userProfile.followersCount || 0;
    const regionHtml = userProfile.region ? `<div class="real-name-subtext"><i class=\"ph ph-map-pin\"></i> ${escapeHtml(userProfile.region)}</div>` : '';
    const realNameHtml = userProfile.realName ? `<div class="real-name-subtext">${escapeHtml(userProfile.realName)}</div>` : '';
    const likesTotal = userPosts.reduce(function (acc, p) { return acc + (p.likes || 0); }, 0);

    document.getElementById('view-profile').innerHTML = `
        ${showReturnBar ? `
        <div class="glass-panel" style="position: sticky; top: 0; z-index: 20; padding: 1rem; display: flex; align-items: center; gap: 15px;">
            <button onclick="window.goBack()" class="back-btn-outline" style="background: none; color: var(--text-main); cursor: pointer; display: flex; align-items: center; gap: 5px;"><span>←</span> Back</button>
            <h2 style="font-weight: 800; font-size: 1.2rem;">${escapeHtml(userProfile.username || 'My Profile')}</h2>
        </div>
        ` : ''}
        <div class="profile-header">
            <button class="profile-cover-action" type="button" onclick="window.handleProfileCoverAction()" style="display:none;">
                <i class="ph ph-image"></i> Add cover
            </button>
            <input id="profile-cover-input" type="file" accept="image/*" style="display:none;" onchange="window.handleProfileCoverInput(event)" />
            <div class="profile-header-content">
                ${avatarHtml}
                <h2 style="font-weight:800; display:flex; align-items:center; gap:6px;">${escapeHtml(displayName)}${verifiedBadge}</h2>
                ${realNameHtml}
                <p style="color:var(--text-muted);">@${escapeHtml(userProfile.username)}</p>
                <p style="margin-top:10px;">${escapeHtml(userProfile.bio)}</p>
                ${regionHtml}
                ${linkHtml}
                <div class="stats-row">
                    <div class="stat-item"><div>${followersCount}</div><div>Followers</div></div>
                    <div class="stat-item"><div>${likesTotal}</div><div>Likes</div></div>
                    <div class="stat-item"><div>${userPosts.length}</div><div>Posts</div></div>
                </div>
                <button onclick="window.toggleSettingsModal(true)" class="create-btn-sidebar" style="width:auto; margin-top:1rem; background:transparent; border:1px solid var(--border); color:var(--text-muted);"><i class="ph ph-gear"></i> Edit Profile & Settings</button>
                <button onclick="window.handleLogout()" class="create-btn-sidebar" style="width:auto; margin-top:10px; background:transparent; border:1px solid var(--border); color:var(--text-muted);"><i class="ph ph-sign-out"></i> Log Out</button>
            </div>
        </div>
        <div class="profile-filters-bar">${renderProfileFilterRow('me', 'Profile filters')}</div>
        <div id="my-profile-content" class="profile-content-region"></div>
    `;

    renderProfileContent(currentUser.uid, userProfile, true, 'my-profile-content');
    applyProfileCover(document.querySelector('#view-profile .profile-header'), userProfile, true);
}

// --- Utils & Helpers ---
function collectFollowedCategoryNames() {
    const names = [];
    const seen = new Set();

    const pushName = function (name) {
        const normalized = typeof name === 'string' ? name.trim() : '';
        if (!normalized || seen.has(normalized)) return;
        seen.add(normalized);
        names.push(normalized);
    };

    (followedCategoryList || []).forEach(pushName);

    if (!names.length) {
        Object.keys(memberships || {}).forEach(function (id) {
            const snapshot = getCategorySnapshot(id);
            const name = snapshot?.name || snapshot?.id || id;
            if ((memberships[id]?.status || 'active') !== 'left') pushName(name);
        });
    }
    return names;
}

function computeTrendingCategories(limit = 8) {
    const counts = {};
    allPosts.forEach(function (post) {
        if (post.category) counts[post.category] = (counts[post.category] || 0) + 1;
    });
    return Object.entries(counts).sort(function (a, b) { return b[1] - a[1]; }).slice(0, limit).map(function (entry) { return entry[0]; });
}

function getTopicHeaderButtons() {
    const header = document.getElementById('category-header');
    if (!header) return [];
    return Array.from(header.querySelectorAll('.category-pill')).filter(function (pill) {
        const label = pill.getAttribute('data-topic');
        return !!label;
    });
}

function moveTopicPillAfterAnchors(topicName) {
    const header = document.getElementById('category-header');
    if (!header) return;
    const pills = getTopicHeaderButtons();
    const anchorLabels = ['For You', 'Following'];
    const anchorPills = pills.filter(function (pill) { return anchorLabels.includes(pill.getAttribute('data-topic')); });
    const target = pills.find(function (pill) { return pill.getAttribute('data-topic') === topicName; });
    if (!target || anchorPills.includes(target)) return;
    const insertAfter = anchorPills[1] || anchorPills[0];
    if (insertAfter && insertAfter.nextSibling) {
        header.insertBefore(target, insertAfter.nextSibling);
    } else {
        header.appendChild(target);
    }
    target.scrollIntoView({ behavior: 'smooth', inline: 'center', block: 'nearest' });
    window.updateCategoryScrollButtons?.();
}

function getAvailableTopicSet() {
    const pills = getTopicHeaderButtons();
    const labels = pills.map(function (pill) { return pill.getAttribute('data-topic'); });
    return new Set(labels
        .filter(function (label) { return label && !['for you', 'following'].includes(label.toLowerCase()); })
        .map(function (label) { return label.toLowerCase(); }));
}

function renderCategoryPills() {
    const header = document.getElementById('category-header');
    if (!header) return;
    header.innerHTML = '';

    const anchors = ['For You', 'Following'];
    const seen = new Set(anchors.map(function (label) { return label.toLowerCase(); }));

    const computeCategoryScore = function (cat) {
        const memberScore = typeof cat.memberCount === 'number' ? cat.memberCount : 0;
        const postScore = typeof cat.postCount === 'number' ? cat.postCount : 0;
        const activityScore = typeof cat.activityScore === 'number' ? cat.activityScore : 0;
        return memberScore + postScore + activityScore;
    };

    const categoryIndex = new Map();
    (categories || []).forEach(function (cat) {
        const label = typeof cat?.name === 'string' && cat.name.trim()
            ? cat.name.trim()
            : (typeof cat?.slug === 'string' && cat.slug.trim() ? cat.slug.trim() : (cat?.id || ''));
        if (!label) return;
        const key = label.toLowerCase();
        if (!categoryIndex.has(key)) {
            categoryIndex.set(key, {
                name: label,
                verified: !!cat.verified,
                type: cat.type || 'community',
                score: computeCategoryScore(cat)
            });
        }
    });

    const addTopic = function (list, topicName, verifiedFlag) {
        const name = typeof topicName === 'string' ? topicName.trim() : '';
        const key = name.toLowerCase();
        if (!name || seen.has(key)) return;
        seen.add(key);
        list.push({ name, verified: !!verifiedFlag });
    };

    anchors.forEach(function (label) {
        const pill = document.createElement('div');
        pill.className = 'category-pill' + (currentCategory === label ? ' active' : '');
        pill.textContent = label;
        pill.dataset.topic = label;
        pill.onclick = function () { window.setCategory(label); };
        header.appendChild(pill);
    });

    const divider = document.createElement('div');
    divider.className = 'category-divider';
    header.appendChild(divider);

    const dynamicTopics = [];
    const followedNames = collectFollowedCategoryNames().filter(function (name) {
        return categoryIndex.has((name || '').toLowerCase());
    });

    followedNames.forEach(function (name) {
        const info = categoryIndex.get((name || '').toLowerCase());
        addTopic(dynamicTopics, info?.name || name, info?.verified);
    });

    const remainingCategories = Array.from(categoryIndex.values()).filter(function (cat) {
        return !seen.has((cat.name || '').toLowerCase());
    });

    const verifiedCategories = remainingCategories.filter(function (cat) { return cat.verified; })
        .sort(function (a, b) {
            if (b.score !== a.score) return b.score - a.score;
            return a.name.localeCompare(b.name);
        });

    const communityCategories = remainingCategories.filter(function (cat) {
        return !cat.verified && (cat.type || 'community') === 'community';
    }).sort(function (a, b) {
        if (b.score !== a.score) return b.score - a.score;
        return a.name.localeCompare(b.name);
    });

    verifiedCategories.forEach(function (cat) { addTopic(dynamicTopics, cat.name, cat.verified); });
    communityCategories.forEach(function (cat) { addTopic(dynamicTopics, cat.name, cat.verified); });

    const dynamicFull = dynamicTopics;
    const dynamic = dynamicFull.slice(0, categoryVisibleCount);

    dynamic.forEach(function (topic) {
        const pill = document.createElement('div');
        pill.className = 'category-pill' + (currentCategory === topic.name ? ' active' : '') + (topic.verified ? ' verified-topic' : '');
        pill.innerHTML = `${escapeHtml(topic.name)}${topic.verified ? `<span class="topic-verified-icon">${getVerifiedIconSvg()}</span>` : ''}`;
        pill.dataset.topic = topic.name;
        pill.onclick = function () { window.setCategory(topic.name); };
        header.appendChild(pill);
    });

    if (dynamicFull.length > categoryVisibleCount) {
        const more = document.createElement('button');
        more.className = 'category-pill load-more';
        more.textContent = 'Load More';
        more.onclick = function () { categoryVisibleCount += 10; renderCategoryPills(); };
        header.appendChild(more);
    }

    // Update scroll button visibility after rendering
    window.updateCategoryScrollButtons();
}

// Category header scroll function
window.scrollCategoryHeader = function (direction) {
    const header = document.getElementById('category-header');
    if (!header) return;

    const scrollAmount = 200;
    header.scrollBy({ left: direction * scrollAmount, behavior: 'smooth' });

    // Update button visibility after scroll
    setTimeout(function () { window.updateCategoryScrollButtons(); }, 300);
};

// Update scroll button visibility based on scroll position
window.updateCategoryScrollButtons = function () {
    const header = document.getElementById('category-header');
    const leftBtn = document.getElementById('category-scroll-left');
    const rightBtn = document.getElementById('category-scroll-right');

    if (!header || !leftBtn || !rightBtn) return;

    const canScrollLeft = header.scrollLeft > 0;
    const canScrollRight = header.scrollLeft < (header.scrollWidth - header.clientWidth - 1);

    leftBtn.style.display = canScrollLeft ? 'block' : 'none';
    rightBtn.style.display = canScrollRight ? 'block' : 'none';
};

// Add scroll event listener to category header
(function () {
    const header = document.getElementById('category-header');
    if (header) {
        header.addEventListener('scroll', window.updateCategoryScrollButtons);
    }
})();


window.setCategory = function (c) {
    currentCategory = c;
    renderCategoryPills();
    document.documentElement.style.setProperty('--primary', '#00f2ea');
    renderFeed();
}

window.renderLive = function () {
    const container = document.getElementById('live-directory-grid') || document.getElementById('live-grid-container');
    if (container) container.innerHTML = "";
    renderLiveDirectoryFromCache();
}

// Helper Utils
function getColorForUser(name) { return ['#FF6B6B', '#4ECDC4', '#45B7D1'][name.length % 3]; }
function escapeHtml(text) {
    if (text === null || text === undefined) return '';

    // If you accidentally pass a structured payload (like post.content),
    // prefer its `.text` field.
    if (typeof text === 'object') {
        if (typeof text.text === 'string') text = text.text;
        else text = JSON.stringify(text);
    } else if (typeof text !== 'string') {
        text = String(text);
    }

    return text
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

function cleanText(text) { if (typeof text !== 'string') return ""; return text.replace(new RegExp(["badword", "hate"].join("|"), "gi"), "🤐"); }

function getPostFeedType(post = {}) {
    const raw = (post.type || post.contentType || post.mediaType || '').toString().toLowerCase();
    if (raw === 'video') return 'videos';
    if (raw === 'livestream' || raw === 'live' || raw === 'stream') return 'livestreams';
    return 'threads';
}

function getFeedItemTimestamp(item) {
    const ts = item?.createdAt || item?.timestamp || item?.updatedAt || item?.publishedAt || item?.startedAt || item?.startTime;
    const date = toDateSafe(ts);
    if (date) return date.getTime();
    if (typeof ts === 'number') return ts;
    if (typeof ts?.seconds === 'number') return ts.seconds * 1000;
    return 0;
}

function matchesCategoryFilter(category) {
    if (currentCategory === 'For You') return true;
    if (currentCategory === 'Following') return !!category && followedCategories.has(category);
    if (currentCategory === 'Saved') return true;
    return !!category && category === currentCategory;
}

function buildHomeLiveCard(session) {
    const card = document.createElement('div');
    card.className = 'social-card live-directory-card';
    card.onclick = function () { if (typeof window.openLiveSession === 'function') window.openLiveSession(session.id); };
    const thumbnail = escapeHtml(resolveLiveThumbnail(session));
    const viewerCount = escapeHtml(session.viewerCount || session.stats?.viewerCount || '0');
    card.innerHTML = `
        <div class="live-directory-thumb">
            <img src="${thumbnail}" alt="Live thumbnail" class="live-thumb-img" loading="lazy" />
            <div class="live-directory-badge">LIVE</div>
            <div class="live-viewers live-directory-viewers"><i class="ph-fill ph-eye"></i> ${viewerCount}</div>
        </div>
        <div class="live-directory-body">
            <div class="live-directory-title">${escapeHtml(session.title || 'Live Session')}</div>
            <div class="live-directory-meta">
                <span class="live-streamer">@${escapeHtml(session.hostId || session.author || 'streamer')}</span>
                <span class="live-viewers"><i class="ph-fill ph-eye"></i> ${viewerCount}</span>
            </div>
            <div class="live-directory-footer">
                <span class="live-directory-category">${escapeHtml(session.category || 'Live')}</span>
                <span class="live-directory-tags">${escapeHtml((session.tags || []).join(', '))}</span>
            </div>
        </div>`;
    return card;
}

function normalizeImageUrl(url) {
    try {
        if (!url || typeof url !== 'string') return '';
        const trimmed = url.trim();
        if (!trimmed) return '';
        if (trimmed.startsWith('http://') || trimmed.startsWith('https://')) return trimmed;
        if (trimmed.startsWith('gs://')) return trimmed;
        return trimmed;
    } catch (e) {
        return '';
    }
}

function getConversationAvatarUrl(convo = {}, fallback = '') {
    try {
        return normalizeImageUrl(convo.avatarUrl || convo.avatarURL || fallback || '');
    } catch (e) {
        return '';
    }
}

function handleSnapshotError(context, error) {
    const code = error?.code || '';
    const message = code === 'permission-denied'
        ? 'You do not have access to this data.'
        : 'We had trouble loading this data.';
    console.warn(`${context} snapshot error`, error?.message || error);
    if (typeof window.toast === 'function') {
        window.toast(message, 'error');
    }
}
function renderSaved() {
    currentCategory = 'Saved';
    const container = document.getElementById('saved-content');
    if (!container) return;

    renderCategoryPills();
    container.innerHTML = '';
    const useCarousels = !savedSearchTerm;

    const savedPostIds = Array.isArray(userProfile.savedPosts) ? userProfile.savedPosts : [];
    let displayPosts = allPosts.filter(function (post) {
        return savedPostIds.includes(post.id);
    });

    if (savedSearchTerm) {
        displayPosts = displayPosts.filter(function (post) {
            return (post.title || '').toLowerCase().includes(savedSearchTerm)
                || (post.content?.text || post.content || '').toLowerCase().includes(savedSearchTerm);
        });
    }

    if (savedFilter === 'Recent') {
        displayPosts.sort(function (a, b) { return savedPostIds.indexOf(b.id) - savedPostIds.indexOf(a.id); });
    } else if (savedFilter === 'Oldest') {
        displayPosts.sort(function (a, b) { return savedPostIds.indexOf(a.id) - savedPostIds.indexOf(b.id); });
    } else if (savedFilter === 'Videos') {
        displayPosts = displayPosts.filter(function (p) { return p.type === 'video'; });
    } else if (savedFilter === 'Images') {
        displayPosts = displayPosts.filter(function (p) { return p.type === 'image'; });
    }

    const savedVideoIds = Array.isArray(userProfile.savedVideos) ? userProfile.savedVideos : [];
    const savedVideoMap = new Map(videosCache.map(function (video) { return [video.id, video]; }));
    let savedVideos = savedVideoIds.map(function (id) { return savedVideoMap.get(id); }).filter(Boolean);

    if (savedSearchTerm) {
        savedVideos = savedVideos.filter(function (video) {
            const caption = (video.caption || '').toLowerCase();
            const tags = (video.hashtags || []).map(function (t) { return `#${t}`.toLowerCase(); }).join(' ');
            return caption.includes(savedSearchTerm) || tags.includes(savedSearchTerm);
        });
    }

    if (savedFilter === 'Recent') {
        savedVideos.sort(function (a, b) { return savedVideoIds.indexOf(b.id) - savedVideoIds.indexOf(a.id); });
    } else if (savedFilter === 'Oldest') {
        savedVideos.sort(function (a, b) { return savedVideoIds.indexOf(a.id) - savedVideoIds.indexOf(b.id); });
    }

    const showVideos = savedFilter !== 'Images';
    const showPosts = true;
    let hasRendered = false;
    let wantsOnlyVideos = savedFilter === 'Videos';

    if (showVideos) {
        if (savedVideos.length) {
            const header = document.createElement('div');
            header.className = 'discover-section-header';
            header.textContent = 'Saved Videos';
            const grid = document.createElement('div');
            grid.className = useCarousels ? 'saved-carousel no-scrollbar' : 'discover-vertical-list';
            savedVideos.forEach(function (video) {
                const card = buildVideoCard(video);
                const animateIn = shouldAnimateItem(`video:${video.id}`);
                if (animateIn) card.classList.add('animate-in');
                grid.appendChild(card);
            });
            container.appendChild(header);
            container.appendChild(grid);
            hasRendered = true;
        }
    }

    if (showPosts) {
        if (displayPosts.length) {
            const header = document.createElement('div');
            header.className = 'discover-section-header';
            header.textContent = 'Saved Posts';
            const stack = document.createElement('div');
            stack.className = useCarousels ? 'saved-carousel no-scrollbar' : 'saved-posts-stack';
            displayPosts.forEach(function (post) {
                const wrapper = document.createElement('div');
                const animateIn = shouldAnimateItem(`threads:${post.id}`);
                wrapper.innerHTML = getPostHTML(post, { animate: animateIn });
                const card = wrapper.firstElementChild;
                if (card) stack.appendChild(card);
            });
            container.appendChild(header);
            container.appendChild(stack);
            displayPosts.forEach(function (post) {
                const reviewBtn = container.querySelector(`#post-card-${post.id} .review-action`);
                const reviewValue = window.myReviewCache ? window.myReviewCache[post.id] : null;
                applyReviewButtonState(reviewBtn, reviewValue);
            });
            applyMyReviewStylesToDOM();
            hasRendered = true;
        }
    }

    if (!hasRendered) {
        const message = wantsOnlyVideos ? 'No saved videos.' : 'No saved items yet.';
        container.innerHTML = `<div class="empty-state"><p>${message}</p></div>`;
    }
}

// Small Interaction Utils
window.setDiscoverFilter = function (filter) {
    discoverFilter = filter;
    document.querySelectorAll('.discover-pill').forEach(function (el) {
        if (el.dataset.filter === filter) el.classList.add('active');
        else el.classList.remove('active');
    });
    const postSort = document.getElementById('discover-post-sort');
    if (postSort) postSort.style.display = filter === 'Posts' ? 'flex' : 'none';
    const categorySort = document.getElementById('discover-category-sort');
    if (categorySort) categorySort.style.display = filter === 'Categories' ? 'flex' : 'none';
    const postsSelect = document.getElementById('posts-sort-select');
    if (postsSelect) postsSelect.value = discoverPostsSort;
    const categoriesSelect = document.getElementById('categories-sort-select');
    if (categoriesSelect) categoriesSelect.value = discoverCategoriesMode;
    renderDiscover();
}
window.clearDiscoverFilters = function () {
    discoverFilter = 'All Results';
    discoverSearchTerm = '';
    const input = document.querySelector('#discover-topbar input[type="text"]');
    if (input) input.value = '';
    updateSearchQueryParam('');
    renderDiscover();
};
window.handlePostsSortChange = function (e) { discoverPostsSort = e.target.value; renderDiscover(); }
window.handleCategoriesModeChange = function (e) { discoverCategoriesMode = e.target.value; renderDiscover(); }
window.handleSearchInput = function (e) {
    const input = e.target;
    const rawValue = input?.value || '';
    const selection = captureInputSelection(input);
    discoverSearchTerm = rawValue.toLowerCase();
    clearTimeout(discoverSearchDebounce);
    discoverSearchDebounce = setTimeout(function () {
        updateSearchQueryParam(rawValue);
        renderDiscoverResults().then(function () {
            restoreInputSelection(input, selection);
        });
    }, SEARCH_DEBOUNCE_MS);
}
window.setSavedFilter = function (filter) { savedFilter = filter; document.querySelectorAll('.saved-pill').forEach(function (el) { if (el.textContent === filter) el.classList.add('active'); else el.classList.remove('active'); }); renderSaved(); }
window.handleSavedSearch = function (e) { savedSearchTerm = e.target.value.toLowerCase(); renderSaved(); }
window.openFullscreenMedia = function (url, type) { const modal = document.getElementById('media-modal'); const content = document.getElementById('media-modal-content'); if (!modal || !content) return; modal.style.display = 'flex'; if (type === 'video') content.innerHTML = `<video src="${url}" controls style="max-width:100%; max-height:90vh; border-radius:8px;" autoplay></video>`; else content.innerHTML = `<img src="${url}" style="max-width:100%; max-height:90vh; border-radius:8px;">`; }
window.closeFullscreenMedia = function () { const modal = document.getElementById('media-modal'); if (modal) modal.style.display = 'none'; const content = document.getElementById('media-modal-content'); if (content) content.innerHTML = ''; }
window.addTagToSaved = async function (postId) { const tag = prompt("Enter a tag for this saved post (e.g. 'Science', 'Read Later'):"); if (!tag) return; userProfile.savedTags = userProfile.savedTags || {}; userProfile.savedTags[postId] = tag; await setDoc(doc(db, "users", currentUser.uid), { savedTags: userProfile.savedTags }, { merge: true }); renderSaved(); }
window.setProfileFilter = function (category, uid) {
    const next = PROFILE_FILTER_OPTIONS.includes(category) ? category : 'All Results';
    currentProfileFilter = next;
    if (uid === 'me') renderProfile(); else renderPublicProfile(uid);
}
window.moveInputToComment = function (commentId, authorName) { activeReplyId = commentId; const slot = document.getElementById(`reply-slot-${commentId}`); const inputArea = document.getElementById('thread-input-area'); const input = document.getElementById('thread-input'); const cancelBtn = document.getElementById('thread-cancel-btn'); if (slot && inputArea) { slot.appendChild(inputArea); input.placeholder = `Replying to ${authorName}...`; if (cancelBtn) cancelBtn.style.display = 'inline-block'; input.focus(); } }
window.resetInputBox = function () { activeReplyId = null; const defaultSlot = document.getElementById('thread-input-default-slot'); const inputArea = document.getElementById('thread-input-area'); const input = document.getElementById('thread-input'); const cancelBtn = document.getElementById('thread-cancel-btn'); if (defaultSlot && inputArea) { defaultSlot.appendChild(inputArea); input.placeholder = "Post your reply"; input.value = ""; if (cancelBtn) cancelBtn.style.display = 'none'; } }
window.triggerFileSelect = function () { document.getElementById('thread-file').click(); }
window.handleFileSelect = function (input) { const btn = document.getElementById('attach-btn-text'); if (input.files && input.files[0]) { btn.innerHTML = `<i class="ph-fill ph-file-image" style="color:var(--primary);"></i> ` + input.files[0].name.substring(0, 15) + "..."; btn.style.color = "var(--primary)"; } else { btn.innerHTML = `<i class="ph ph-paperclip"></i> Attach`; btn.style.color = "var(--text-muted)"; } }
window.previewPostImage = function (input) { if (input.files && input.files[0]) { const reader = new FileReader(); reader.onload = function (e) { document.getElementById('img-preview-tag').src = e.target.result; document.getElementById('img-preview-container').style.display = 'block'; }; reader.readAsDataURL(input.files[0]); } }
window.clearPostImage = function () { document.getElementById('postFile').value = ""; document.getElementById('img-preview-container').style.display = 'none'; document.getElementById('img-preview-tag').src = ""; }
window.togglePostOption = function (type) { const area = document.getElementById('extra-options-area'); const target = document.getElementById('post-opt-' + type);['poll', 'gif', 'schedule', 'location'].forEach(function (t) { if (t !== type) document.getElementById('post-opt-' + t).style.display = 'none'; }); if (target.style.display === 'none') { area.style.display = 'block'; target.style.display = 'block'; } else { target.style.display = 'none'; area.style.display = 'none'; } }
window.closeReview = function () { return document.getElementById('review-modal').style.display = 'none'; };
function closePostOptionsDropdown() {
    const dropdown = document.getElementById('post-options-dropdown');
    if (dropdown) dropdown.style.display = 'none';
    document.removeEventListener('click', handlePostOptionsOutside, true);
    document.removeEventListener('keydown', handlePostOptionsEscape, true);
}

function handlePostOptionsOutside(event) {
    const dropdown = document.getElementById('post-options-dropdown');
    if (!dropdown) return;
    if (dropdown.contains(event.target)) return;
    closePostOptionsDropdown();
}

function handlePostOptionsEscape(event) {
    if (event.key === 'Escape') closePostOptionsDropdown();
}

window.openPostOptions = function (event, postId, ownerId, context = 'feed') {
    if (!requireAuth()) return;
    activeOptionsPost = { id: postId, ownerId, context };
    const dropdown = document.getElementById('post-options-dropdown');
    const deleteBtn = document.getElementById('dropdown-delete-btn');
    const editBtn = document.getElementById('dropdown-edit-btn');
    const shareBtn = document.getElementById('dropdown-share-btn');
    const messageBtn = document.getElementById('dropdown-message-btn');
    if (deleteBtn) deleteBtn.style.display = currentUser && ownerId === currentUser.uid ? 'flex' : 'none';
    if (editBtn) editBtn.style.display = currentUser && ownerId === currentUser.uid ? 'flex' : 'none';
    if (shareBtn) shareBtn.style.display = 'flex';
    if (messageBtn) messageBtn.style.display = currentUser && ownerId !== currentUser.uid ? 'flex' : 'none';
    if (dropdown && event && event.currentTarget) {
        dropdown.style.display = 'block';
        const rect = event.currentTarget.getBoundingClientRect();
        const dropdownRect = dropdown.getBoundingClientRect();
        dropdown.style.top = `${rect.bottom + window.scrollY + 6}px`;
        dropdown.style.left = `${Math.max(10, rect.right + window.scrollX - dropdownRect.width)}px`;
    }
    document.addEventListener('click', handlePostOptionsOutside, true);
    document.addEventListener('keydown', handlePostOptionsEscape, true);
};

window.closePostOptions = function () { const modal = document.getElementById('post-options-modal'); if (modal) modal.style.display = 'none'; closePostOptionsDropdown(); }
window.handlePostOptionSelect = function (action) {
    closePostOptionsDropdown();
    if (action === 'share' && activeOptionsPost?.id) return window.sharePost(activeOptionsPost.id);
    if (action === 'message' && activeOptionsPost?.id) return window.messageAuthor(activeOptionsPost.id);
    if (action === 'report') return window.openReportModal();
    if (action === 'delete') return window.confirmDeletePost();
    if (action === 'edit') return window.beginEditPost();
}
window.openReportModal = function () { closePostOptionsDropdown(); const opts = document.getElementById('post-options-modal'); if (opts) opts.style.display = 'none'; const modal = document.getElementById('report-modal'); if (modal) modal.style.display = 'flex'; }
window.closeReportModal = function () { const modal = document.getElementById('report-modal'); if (modal) modal.style.display = 'none'; }
window.submitReport = async function () { if (!requireAuth()) return; if (!activeOptionsPost || !activeOptionsPost.id || !activeOptionsPost.ownerId) return toast('No post selected', 'error'); const categoryEl = document.getElementById('report-category'); const detailEl = document.getElementById('report-details'); const category = categoryEl ? categoryEl.value : ''; const details = detailEl ? detailEl.value.trim().substring(0, 500) : ''; if (!category) return toast('Please choose a category.', 'error'); try { await addDoc(collection(db, 'reports', 'threads'), { contentId: activeOptionsPost.id, contentType: 'thread', reportedBy: currentUser.uid, reason: details, createdAt: serverTimestamp() }); if (detailEl) detailEl.value = ''; if (categoryEl) categoryEl.value = ''; window.closeReportModal(); toast('Report submitted', 'info'); } catch (e) { console.error(e); toast('Could not submit report.', 'error'); } }
window.confirmDeletePost = async function () { if (!activeOptionsPost || !activeOptionsPost.id) return; if (!currentUser || activeOptionsPost.ownerId !== currentUser.uid) return toast('You can only delete your own post.', 'error'); const ok = confirm('Are you sure?'); if (!ok) return; try { await deleteDoc(doc(db, 'posts', activeOptionsPost.id)); allPosts = allPosts.filter(function (p) { return p.id !== activeOptionsPost.id; }); renderFeed(); if (currentViewId === 'profile') renderProfile(); if (currentViewId === 'public-profile' && viewingUserId) renderPublicProfile(viewingUserId); if (activePostId === activeOptionsPost.id) { activePostId = null; window.navigateTo('feed'); const threadStream = document.getElementById('thread-stream'); if (threadStream) threadStream.innerHTML = ''; } window.closePostOptions(); toast('Post deleted', 'info'); } catch (e) { console.error('Delete error', e); toast('Failed to delete post', 'error'); } }

window.beginEditPost = function () {
    if (!activeOptionsPost || !activeOptionsPost.id) return;
    const post = allPosts.find(function (p) { return p.id === activeOptionsPost.id; });
    if (!post || !currentUser || post.userId !== currentUser.uid) return toast('You can only edit your own post.', 'error');
    currentEditPost = post;
    const title = document.getElementById('postTitle');
    const content = document.getElementById('postContent');
    if (title) title.value = post.title || '';
    if (content) content.value = typeof post.content === 'object' ? (post.content.text || '') : (post.content || '');
    composerTags = Array.isArray(post.tags) ? post.tags.map(normalizeTagValue).filter(Boolean) : [];
    composerMentions = normalizeMentionsField(post.mentions || []);
    composerPoll = post.poll ? { title: post.poll.title || '', options: (post.poll.options || ['', '']).slice(0, 5) } : { title: '', options: ['', ''] };
    composerScheduledFor = formatTimestampForInput(post.scheduledFor);
    composerLocation = post.location || '';
    selectedCategoryId = post.categoryId || selectedCategoryId;
    renderComposerTags();
    renderComposerMentions();
    renderPollOptions();
    const scheduleInput = document.getElementById('schedule-input');
    if (scheduleInput) scheduleInput.value = composerScheduledFor;
    setComposerLocation(composerLocation);
    const tagInput = document.getElementById('tag-input');
    if (tagInput) tagInput.value = '';
    const mentionInput = document.getElementById('mention-input');
    if (mentionInput) mentionInput.value = '';
    if (post.mediaUrl) {
        const preview = document.getElementById('img-preview-tag');
        const previewContainer = document.getElementById('img-preview-container');
        if (preview && previewContainer) {
            preview.src = post.mediaUrl;
            previewContainer.style.display = 'block';
        }
    }
    syncComposerMode();
    window.toggleCreateModal(true);
};

// --- Messaging (DMs) ---
function getDirectConversationId(a, b) {
    return [a, b].sort().join('_');
}

const DM_MEDIA_ALLOWED_PREFIXES = CHAT_ALLOWED_MIME_PREFIXES;

function validateDmAttachment(file) {
    if (!file) return { ok: false, message: 'Attachment missing.' };
    const type = file.type || '';
    const maxBytes = type.startsWith('video/') ? CHAT_VIDEO_MAX_BYTES : CHAT_IMAGE_MAX_BYTES;
    return validateChatAttachment(file, { maxBytes, allowedPrefixes: DM_MEDIA_ALLOWED_PREFIXES });
}

function filterDmAttachments(files = []) {
    const valid = [];
    files.forEach(function (file) {
        const result = validateDmAttachment(file);
        if (!result.ok) {
            toast(result.message, 'error');
            return;
        }
        valid.push(file);
    });
    return valid;
}

function deriveOtherParticipantMeta(participants = [], viewerId, details = {}) {
    const otherIds = participants.filter(function (uid) { return uid !== viewerId; });
    const usernames = otherIds.map(function (uid) {
        const cached = getCachedUser(uid) || {};
        const idx = participants.indexOf(uid);
        return cached.username || (details.participantUsernames || [])[idx] || (details.participantNames || [])[idx] || 'user';
    });
    const names = otherIds.map(function (uid) {
        const cached = getCachedUser(uid) || {};
        const idx = participants.indexOf(uid);
        return resolveDisplayName(cached) || (details.participantNames || [])[idx] || (details.participantUsernames || [])[idx] || '';
    });
    const avatars = otherIds.map(function (uid) {
        const cached = getCachedUser(uid) || {};
        const idx = participants.indexOf(uid);
        return cached.photoURL || (details.participantAvatars || [])[idx] || '';
    });
    const colors = otherIds.map(function (uid) {
        const cached = getCachedUser(uid) || {};
        return cached.avatarColor || computeAvatarColor(cached.username || uid || 'user');
    });
    return { otherIds, usernames, avatars, names, colors };
}

function isConversationRequest(mapping = {}, details = {}) {
    const explicitFlag = mapping.isRequest ?? details.isRequest ?? mapping.requested ?? details.requested;
    if (explicitFlag === true) return true;
    const status = (mapping.requestStatus || details.requestStatus || mapping.status || details.status || '').toString().toLowerCase();
    if (['request', 'requested', 'pending', 'invite', 'invited'].includes(status)) return true;
    if (mapping.accepted === false || details.accepted === false) return true;
    return false;
}

function buildUnknownUserProfile(uid) {
    return storeUserInCache(uid, {
        username: 'user',
        displayName: 'Unknown user',
        name: 'Unknown user',
        photoURL: '',
        avatarColor: computeAvatarColor(uid || 'user')
    });
}

function mapUserDocToProfile(data = {}) {
    const displayName = data.displayName || data.username || 'User';
    const profile = {
        displayName,
        name: displayName,
        username: data.username || '',
        photoURL: data.photoURL || data.avatar || '',
        bio: data.bio || ''
    };
    if (data.followersCount != null) {
        profile.followersCount = data.followersCount;
    } else if (Array.isArray(data.followers)) {
        profile.followersCount = data.followers.length;
    }
    if (data.followingCount != null) {
        profile.followingCount = data.followingCount;
    } else if (Array.isArray(data.following)) {
        profile.followingCount = data.following.length;
    }
    if (data.postsCount != null) {
        profile.postsCount = data.postsCount;
    } else if (data.postCount != null) {
        profile.postsCount = data.postCount;
    }
    return profile;
}

async function resolveUserProfile(uid, options = {}) {
    if (!uid) return buildUnknownUserProfile('user');
    const force = options.force === true;
    const cached = getCachedUser(uid, { allowStale: !force });
    const shouldRefresh = force || isUserCacheStale(cached);
    if (cached && !shouldRefresh) return cached;

    if (userFetchPromises[uid]) {
        try { return await userFetchPromises[uid]; } catch (e) { /* ignore */ }
    }

    const fetchPromise = (async function () {
        try {
            if (!getViewerUid()) {
                return buildUnknownUserProfile(uid);
            }
            const snap = await getDoc(doc(db, 'profiles', uid));
            if (snap.exists()) {
                return storeUserInCache(uid, snap.data());
            }
            const fallbackSnap = await getDoc(doc(db, 'users', uid));
            if (fallbackSnap.exists()) {
                return storeUserInCache(uid, mapUserDocToProfile(fallbackSnap.data()));
            }
            logMissingProfileOnce(uid);
        } catch (e) {
            if (e?.code === 'permission-denied') {
                logPermissionDeniedOnce(`profiles:read:${uid}`);
                try {
                    const fallbackSnap = await getDoc(doc(db, 'users', uid));
                    if (fallbackSnap.exists()) {
                        return storeUserInCache(uid, mapUserDocToProfile(fallbackSnap.data()));
                    }
                    logMissingProfileOnce(uid);
                } catch (fallbackError) {
                    if (fallbackError?.code === 'permission-denied') {
                        logPermissionDeniedOnce(`users:read:${uid}`);
                    } else {
                        console.warn('User fallback fetch failed', uid, fallbackError?.message || fallbackError);
                    }
                }
            } else {
                console.warn('User fetch failed', uid, e?.message || e);
            }
        }
        return buildUnknownUserProfile(uid);
    })();

    userFetchPromises[uid] = fetchPromise;
    const profile = await fetchPromise;
    delete userFetchPromises[uid];
    return profile;
}

async function refreshUserProfiles(userIds = [], options = {}) {
    const unique = Array.from(new Set(userIds.filter(Boolean)));
    const promises = unique.map(function (uid) { return resolveUserProfile(uid, options).catch(function () { return null; }); });
    const results = await Promise.all(promises);
    return results.filter(Boolean);
}

function formatMessagePreview(payload = {}) {
    if (payload.type === 'image') return '[image]';
    if (payload.type === 'video') return '[video]';
    if (payload.type === 'post_ref') return '[post]';
    if (payload.type === 'call_invite') return '[call invite]';
    const text = (payload.text || '').trim();
    if (!text) return '[message]';
    return text.length > 80 ? text.substring(0, 77) + '…' : text;
}

function toDateSafe(ts) {
    if (!ts) return null;
    if (ts instanceof Timestamp) return ts.toDate();
    if (typeof ts.seconds === 'number') return new Date(ts.seconds * 1000);
    const date = new Date(ts);
    return Number.isNaN(date.getTime()) ? null : date;
}

function formatDateTime(ts) {
    const date = toDateSafe(ts);
    if (!date) return '—';
    const formatted = date.toLocaleString([], { month: 'short', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit' });
    return formatted.replace(', ', ' ');
}

function formatMessageHoverTimestamp(ts) {
    const date = toDateSafe(ts);
    if (!date) return '';
    const now = new Date();
    if (isSameDay(date, now)) {
        return date.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });
    }
    return formatDateTime(date);
}

function isSameDay(a, b) {
    return a && b && a.getFullYear() === b.getFullYear() && a.getMonth() === b.getMonth() && a.getDate() === b.getDate();
}

function formatChatDateLabel(date) {
    if (!date) return '';
    const now = new Date();
    const yesterday = new Date(now.getTime() - 86400000);
    if (isSameDay(date, now)) return 'Today';
    if (isSameDay(date, yesterday)) return 'Yesterday';
    return date.toLocaleDateString([], { year: 'numeric', month: 'long', day: 'numeric' });
}

function formatMessageTimeLabel(ts) {
    const date = toDateSafe(ts);
    if (!date) return '';
    return date.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });
}

function needsTimeGapDivider(prevDate, currentDate) {
    if (!prevDate || !currentDate) return false;
    if (!isSameDay(prevDate, currentDate)) return false;
    const diff = currentDate.getTime() - prevDate.getTime();
    return diff >= 3 * 60 * 60 * 1000;
}

function formatTimeGapDivider(currentDate) {
    if (!currentDate) return '';
    return currentDate.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });
}



function isNearBottom(el, threshold = 80) {
    if (!el) return false;
    return el.scrollHeight - el.scrollTop - el.clientHeight <= threshold;
}

function getMessageScrollContainer() {
    const body = document.getElementById('message-thread');
    if (!body) return null;
    return body.closest('.message-scroll-region') || body;
}

function scrollMessagesToBottom() {
    const scrollRegion = getMessageScrollContainer();
    if (!scrollRegion) return;
    scrollRegion.scrollTop = scrollRegion.scrollHeight;
}

function computeConversationTitle(convo = {}, viewerId = currentUser?.uid) {
    const stored = (convo.title || '').trim();
    if (stored) return stored;

    const participants = convo.participants || [];
    const orderedParticipants = viewerId ? participants.filter(function (uid) { return uid !== viewerId; }) : participants.slice();
    if (!orderedParticipants.length) orderedParticipants.push(...participants);

    const names = orderedParticipants.map(function (uid) {
        const meta = resolveParticipantDisplay(convo, uid);
        return meta.displayName || meta.username || 'Participant';
    }).filter(Boolean);

    if (!names.length) return 'Conversation';
    if (participants.length === 2) return names[0] || 'Conversation';
    if (participants.length === 3) return names.slice(0, 2).join(', ') || names[0];
    if (participants.length >= 4) {
        const firstTwo = names.slice(0, 2);
        const remaining = Math.max(0, names.length - firstTwo.length);
        const suffix = remaining > 0 ? `, +${remaining} more` : '';
        return `${firstTwo.join(', ')}${suffix}`;
    }

    return names.join(', ');
}

function resolveParticipantDisplay(convo = {}, uid = '') {
    const idx = (convo.participants || []).indexOf(uid);
    const cached = getCachedUser(uid) || {};
    const username = cached.username || (convo.participantUsernames || [])[idx] || 'user';
    const displayName = resolveDisplayName(cached) || (convo.participantNames || [])[idx] || username || 'Unknown user';
    const avatar = cached.photoURL || (convo.participantAvatars || [])[idx] || '';
    const avatarColor = cached.avatarColor || computeAvatarColor(username || uid || 'user');
    return { username, displayName, avatar, avatarColor, profile: cached };
}

function getMessageTimestampMs(msg = {}) {
    const date = toDateSafe(msg.createdAt);
    return date ? date.getTime() : 0;
}

function normalizeNotificationKeyPart(value = '') {
    return String(value || '').replace(/[\\/]/g, '_').trim();
}

function buildNotificationDocId({ targetUid = '', actorUid = '', entityType = '', entityId = '', actionType = '', type = '' }) {
    const action = actionType || type || '';
    return [
        normalizeNotificationKeyPart(action),
        normalizeNotificationKeyPart(actorUid),
        normalizeNotificationKeyPart(targetUid),
        normalizeNotificationKeyPart(entityType),
        normalizeNotificationKeyPart(entityId)
    ].filter(Boolean).join('_');
}

async function createNotificationOnce(payload = {}) {
    const targetUid = payload.targetUid;
    const actorUid = payload.actorUid;
    if (!targetUid || !actorUid || targetUid === actorUid) return;
    const notificationKey = buildNotificationDocId(payload);
    if (!notificationKey) return;
    const notifRef = doc(db, 'notifications', targetUid, 'items', notificationKey);
    try {
        const resolvedType = payload.type || payload.actionType || 'activity';
        const body = {
            createdAt: serverTimestamp(),
            read: false,
            notificationKey,
            actorUid,
            targetUid,
            entityType: payload.entityType || null,
            entityId: payload.entityId || null,
            contentType: payload.contentType || payload.entityType || null,
            actionType: payload.actionType || payload.type || null,
            type: payload.entityType ? 'content' : resolvedType,
            previewText: payload.previewText || '',
            extra: payload.extra || null
        };
        await setDoc(notifRef, body, { merge: false });
    } catch (err) {
        if (err?.code === 'permission-denied') return;
        console.warn('[notifications] createNotificationOnce failed', err?.message || err);
    }
}

function notifIsRead(notification = {}) {
    return notification?.read === true || notification?.isRead === true;
}

function getPushTokenDocId(token = '') {
    const raw = String(token || '');
    const encoded = btoa(unescape(encodeURIComponent(raw))).replace(/=+$/g, '');
    return encoded.replace(/[^a-zA-Z0-9_-]/g, '_');
}

function getStoredPushToken() {
    return window.localStorage?.getItem(PUSH_TOKEN_STORAGE_KEY) || '';
}

async function registerMessagingServiceWorker() {
    if (!('serviceWorker' in navigator)) return null;
    if (messagingRegistration) return messagingRegistration;
    try {
        messagingRegistration = await navigator.serviceWorker.register('/firebase-messaging-sw.js');
        return messagingRegistration;
    } catch (err) {
        console.warn('Unable to register messaging service worker', err?.message || err);
        return null;
    }
}

async function upsertPushToken(uid, token) {
    if (!uid || !token) return;
    const docId = getPushTokenDocId(token);
    await setDoc(doc(db, `users/${uid}/pushTokens/${docId}`), {
        token,
        platform: 'web',
        createdAt: serverTimestamp(),
        lastSeenAt: serverTimestamp(),
        userAgent: navigator.userAgent || ''
    }, { merge: true });
}

async function syncStoredPushToken(uid) {
    if (!uid || !('Notification' in window)) return;
    if (Notification.permission !== 'granted') return;
    const token = getStoredPushToken();
    if (!token) return;
    try {
        await upsertPushToken(uid, token);
    } catch (err) {
        console.warn('Unable to sync push token', err?.message || err);
    }
}

async function enablePushNotifications() {
    if (!currentUser) return toast('Please log in to enable notifications.', 'info');
    if (!('Notification' in window)) return toast('Notifications are not supported in this browser.', 'error');
    if (!FCM_VAPID_KEY) return toast('Push notifications are not configured yet.', 'error');

    const permission = await Notification.requestPermission();
    updatePushSettingsUI();
    if (permission !== 'granted') {
        return toast('Notifications permission not granted.', 'info');
    }
    const registration = await registerMessagingServiceWorker();
    if (!registration) return toast('Unable to register notifications.', 'error');
    try {
        const token = await getToken(messaging, { vapidKey: FCM_VAPID_KEY, serviceWorkerRegistration: registration });
        if (!token) return toast('Unable to enable notifications.', 'error');
        window.localStorage?.setItem(PUSH_TOKEN_STORAGE_KEY, token);
        await upsertPushToken(currentUser.uid, token);
        toast('Notifications enabled.', 'success');
        updatePushSettingsUI();
    } catch (err) {
        console.warn('Unable to enable push notifications', err?.message || err);
        toast('Unable to enable notifications.', 'error');
    }
}

async function disablePushNotifications() {
    if (!currentUser) return;
    const token = getStoredPushToken();
    if (!token) return;
    const docId = getPushTokenDocId(token);
    try {
        await deleteDoc(doc(db, `users/${currentUser.uid}/pushTokens/${docId}`));
    } catch (err) {
        console.warn('Unable to remove push token doc', err?.message || err);
    }
    try {
        await deleteFcmToken(messaging);
    } catch (err) {
        console.warn('Unable to delete FCM token', err?.message || err);
    }
    window.localStorage?.removeItem(PUSH_TOKEN_STORAGE_KEY);
    updatePushSettingsUI();
}

async function removePushTokenForUser(uid) {
    if (!uid) return;
    const token = getStoredPushToken();
    if (!token) return;
    const docId = getPushTokenDocId(token);
    try {
        await deleteDoc(doc(db, `users/${uid}/pushTokens/${docId}`));
    } catch (err) {
        console.warn('Unable to remove push token for user', err?.message || err);
    }
}

function updatePushSettingsUI() {
    const statusEl = document.getElementById('push-notif-status');
    const btn = document.getElementById('push-notif-enable-btn');
    if (!statusEl && !btn) return;
    if (!('Notification' in window)) {
        if (statusEl) statusEl.textContent = 'Notifications are not supported in this browser.';
        if (btn) btn.disabled = true;
        return;
    }
    if (!FCM_VAPID_KEY) {
        if (statusEl) statusEl.textContent = 'Notifications are not configured yet.';
        if (btn) btn.disabled = true;
        return;
    }
    if (Notification.permission === 'granted') {
        if (statusEl) statusEl.textContent = 'Enabled for this browser.';
        if (btn) btn.disabled = true;
    } else if (Notification.permission === 'denied') {
        if (statusEl) statusEl.textContent = 'Blocked in this browser settings.';
        if (btn) btn.disabled = true;
    } else {
        if (statusEl) statusEl.textContent = 'Disabled. Enable to receive message alerts.';
        if (btn) btn.disabled = false;
    }
}

function ensurePushSettingsUI() {
    const modalContent = document.querySelector('#settings-modal .modal-content');
    if (!modalContent || document.getElementById('push-notif-settings')) return;
    const section = document.createElement('div');
    section.className = 'settings-section';
    section.id = 'push-notif-settings';
    section.innerHTML = `
        <div class="settings-section-header">Notifications</div>
        <div id="push-notif-status" class="settings-hint" style="margin-bottom:10px;">Checking notification status...</div>
        <button type="button" id="push-notif-enable-btn" class="create-btn-sidebar">Enable Notifications</button>
    `;
    modalContent.appendChild(section);
    const btn = section.querySelector('#push-notif-enable-btn');
    if (btn) btn.onclick = function () { enablePushNotifications(); };
    updatePushSettingsUI();
}

function initMessagingForegroundListener() {
    if (messagingListenerReady) return;
    messagingListenerReady = true;
    onMessage(messaging, function (payload) {
        const data = payload?.data || {};
        if (data.kind !== 'dm') return;
        const conversationId = data.conversationId || '';
        if (currentViewId === 'messages' && activeConversationId === conversationId) return;
        const title = payload?.notification?.title || 'New message';
        const body = payload?.notification?.body || 'You have a new message.';
        toast(`${title}: ${body}`, 'info');
    });
}

function getNotificationBucket(notification = {}) {
    const entityType = (notification.entityType || '').toLowerCase();
    const type = (notification.type || notification.actionType || '').toLowerCase();
    if (type === 'dm') return 'account';
    if (entityType === 'video' || entityType === 'videos') return 'videos';
    if (entityType === 'livestream' || entityType === 'live' || entityType === 'stream') return 'livestreams';
    if (entityType === 'post' || entityType === 'posts') return 'posts';
    if (['mention', 'comment', 'reply', 'repost', 'like'].includes(type)) return 'posts';
    return 'account';
}

function getContentNotificationBucket(notification = {}) {
    const contentType = (notification.contentType || notification.entityType || '').toLowerCase();
    if (contentType === 'video' || contentType === 'videos') return 'videos';
    if (contentType === 'livestream' || contentType === 'live' || contentType === 'stream' || contentType === 'livestreams') return 'livestreams';
    if (contentType === 'post' || contentType === 'posts') return 'posts';
    return null;
}

function formatContentTypeLabel(contentType = '') {
    const normalized = (contentType || '').toLowerCase();
    if (normalized === 'video' || normalized === 'videos') return 'video';
    if (normalized === 'livestream' || normalized === 'live' || normalized === 'stream' || normalized === 'livestreams') return 'live stream';
    return 'post';
}

function buildContentNotificationDescription(notification = {}) {
    const actionLabel = formatNotificationAction(notification.actionType || 'activity');
    const contentLabel = formatContentTypeLabel(notification.contentType);
    return `${actionLabel} your ${contentLabel}.`;
}

function formatNotificationAction(action = '') {
    const normalized = (action || '').toLowerCase();
    const map = {
        like: 'liked',
        dislike: 'disliked',
        comment: 'commented on',
        reply: 'replied to',
        mention: 'mentioned you in',
        repost: 'reposted',
        follow: 'followed',
        system: 'updated',
        moderation: 'updated',
        verification: 'updated',
        live: 'updated'
    };
    return map[normalized] || 'interacted with';
}

function formatNotificationEntity(entityType = '') {
    const normalized = (entityType || '').toLowerCase();
    const map = {
        post: 'post',
        video: 'video',
        livestream: 'livestream',
        account: 'account'
    };
    return map[normalized] || 'post';
}

function loadInboxModeFromStorage() {
    if (inboxModeRestored) return;
    inboxModeRestored = true;
    try {
        const savedMode = window.localStorage?.getItem('nexera_last_inbox_mode');
        const savedContent = window.localStorage?.getItem('nexera_last_inbox_contentMode');
        const allowedModes = ['messages', 'content', 'account'];
        if (allowedModes.includes(savedMode)) {
            inboxMode = savedMode;
        }
        const allowedContent = ['posts', 'videos', 'livestreams'];
        if (allowedContent.includes(savedContent)) {
            inboxContentPreferred = savedContent;
        }
    } catch (err) {
        console.warn('Unable to read inbox mode', err?.message || err);
    }
}

function computeUnreadMessageTotal() {
    return conversationMappings.reduce(function (sum, mapping) {
        return sum + (mapping.unreadCount || 0);
    }, 0);
}

function computeUnreadNotificationTotal() {
    return Object.values(inboxNotificationCounts).reduce(function (sum, count) {
        return sum + (count || 0);
    }, 0);
}

function updateInboxTabBadges() {
    const counts = {
        messages: computeUnreadMessageTotal(),
        posts: inboxNotificationCounts.posts || 0,
        videos: inboxNotificationCounts.videos || 0,
        livestreams: inboxNotificationCounts.livestreams || 0,
        account: inboxNotificationCounts.account || 0
    };
    counts.content = (counts.posts || 0) + (counts.videos || 0) + (counts.livestreams || 0);
    document.querySelectorAll('.inbox-tab-badge').forEach(function (badge) {
        const mode = badge.dataset.mode;
        const total = counts[mode] || 0;
        if (total <= 0) {
            badge.textContent = '';
            badge.style.display = 'none';
            return;
        }
        badge.textContent = total > 99 ? '99+' : String(total);
        badge.style.display = 'inline-flex';
    });
}

function updateInboxNavBadge() {
    const total = computeUnreadMessageTotal() + computeUnreadNotificationTotal();
    const label = total > 99 ? '99+' : String(total);
    document.querySelectorAll('#nav-inbox-badge').forEach(function (badge) {
        if (!badge) return;
        if (total <= 0) {
            badge.style.display = 'none';
            badge.textContent = '';
            return;
        }
        badge.textContent = label;
        badge.style.display = 'inline-flex';
    });
    updateInboxTabBadges();
}

function updateNavBadge(key, count) {
    if (key !== 'inbox') return;
    const badge = document.querySelector('#nav-inbox-badge');
    if (!badge) return;
    if (count > 0) {
        badge.textContent = count > 99 ? '99+' : String(count);
        badge.style.display = 'inline-flex';
    } else {
        badge.textContent = '';
        badge.style.display = 'none';
    }
}

function safeUpdateNavBadge(key, count) {
    try {
        updateNavBadge(key, count);
    } catch (e) {
        console.warn('[Badges] Failed updating nav badge:', key, e);
    }
}

function updateInboxNotificationCounts() {
    let unreadPosts = 0;
    let unreadVideos = 0;
    let unreadLivestreams = 0;
    let unreadAccount = 0;

    // Content notifications: type is usually "content"; the real category is contentType/entityType.
    contentNotifications.forEach((notif) => {
        if (notifIsRead(notif)) return;
        const bucket = getContentNotificationBucket(notif);
        if (bucket === 'posts') unreadPosts++;
        else if (bucket === 'videos') unreadVideos++;
        else if (bucket === 'livestreams') unreadLivestreams++;
    });
    inboxNotifications.forEach((notif) => {
        if (notifIsRead(notif)) return;
        const bucket = getNotificationBucket(notif);
        if (bucket === 'account') unreadAccount++;
    });

    inboxNotificationCounts = {
        posts: unreadPosts,
        videos: unreadVideos,
        livestreams: unreadLivestreams,
        account: unreadAccount
    };

    updateInboxTabBadges();
    const unreadContent = unreadPosts + unreadVideos + unreadLivestreams;
    const unreadMessages = computeUnreadMessageTotal();
    const totalUnread = unreadAccount + unreadContent + unreadMessages;
    safeUpdateNavBadge('inbox', totalUnread);
    safeUpdateNavBadge('content', unreadContent);
    safeUpdateNavBadge('account', unreadAccount);
    safeUpdateNavBadge('messages', unreadMessages);
}

function syncInboxContentFilters() {
    const toggles = document.querySelectorAll('.inbox-content-toggle');
    toggles.forEach(function (btn) {
        const key = btn.dataset.content;
        btn.classList.toggle('active', !!inboxContentFilters[key]);
    });
    document.querySelectorAll('.inbox-content-section').forEach(function (section) {
        const key = section.dataset.content;
        const enabled = !!inboxContentFilters[key];
        section.style.display = enabled ? 'block' : 'none';
    });
}

function toggleInboxContentFilter(mode) {
    if (!mode || !inboxContentFilters.hasOwnProperty(mode)) return;
    if (inboxMode !== 'content') {
        setInboxMode('content', { skipRouteUpdate: true });
    }
    const wasEnabled = inboxContentFilters[mode];
    inboxContentFilters[mode] = !inboxContentFilters[mode];
    if (!wasEnabled) {
        inboxContentPreferred = mode;
    }
    if (!Object.values(inboxContentFilters).some(Boolean)) {
        inboxContentFilters = { posts: true, videos: true, livestreams: true };
    }
    syncInboxContentFilters();
    renderContentNotificationList(mode);
    void markAllContentNotificationsRead(mode);
    try {
        window.localStorage?.setItem('nexera_last_inbox_mode', 'content');
        window.localStorage?.setItem('nexera_last_inbox_contentMode', inboxContentPreferred || mode);
    } catch (err) {
        console.warn('Unable to persist inbox content preference', err?.message || err);
    }
}

window.toggleInboxContentFilter = toggleInboxContentFilter;

function markNotificationRead(notif) {
    if (!currentUser || !notif || notifIsRead(notif) || !notif.id) return;
    notif.read = true;
    updateInboxNotificationCounts();
    const notifRef = doc(db, 'users', currentUser.uid, 'notifications', notif.id);
    updateDoc(notifRef, { read: true }).catch(function (err) {
        console.warn('Failed to mark notification read', err?.message || err);
    });
}

function markContentNotificationRead(notifId, notif) {
    if (!currentUser || !notifId) return;
    if (notif && notifIsRead(notif)) return;
    if (notif) {
        notif.read = true;
        notif.isRead = true;
    }
    updateInboxNotificationCounts();
    const notifRef = doc(db, 'users', currentUser.uid, 'notifications', notifId);
    updateDoc(notifRef, { isRead: true, read: true }).catch(function (err) {
        console.warn('Failed to mark content notification read', err?.message || err);
    });
}

async function markAllContentNotificationsRead(optionalBucket) {
    if (!currentUser) return;
    const bucket = optionalBucket || null;
    const pending = contentNotifications.filter(function (notif) {
        if (!notif || !notif.id || notifIsRead(notif)) return false;
        if (!bucket) return true;
        return getContentNotificationBucket(notif) === bucket;
    });
    if (!pending.length) return;
    pending.forEach(function (notif) {
        notif.read = true;
        notif.isRead = true;
        notif.readAt = new Date();
    });
    updateInboxNotificationCounts();
    const updates = pending.slice();
    for (let i = 0; i < updates.length; i += 450) {
        const batch = writeBatch(db);
        updates.slice(i, i + 450).forEach(function (notif) {
            const notifRef = doc(db, 'users', currentUser.uid, 'notifications', notif.id);
            batch.update(notifRef, {
                read: true,
                isRead: true,
                readAt: serverTimestamp()
            });
        });
        try {
            await batch.commit();
        } catch (err) {
            console.warn('Failed to mark content notifications read', err?.message || err);
        }
    }
    updateInboxNotificationCounts();
}

function renderContentNotificationList(mode = 'posts') {
    const listEl = document.getElementById(`inbox-list-${mode}`);
    const emptyEl = document.getElementById(`inbox-empty-${mode}`);
    if (!listEl) return;
    listEl.innerHTML = '';
    const bucketed = contentNotifications
        .filter(function (notif) { return getContentNotificationBucket(notif) === mode; })
        .sort(function (a, b) {
            const aTs = toDateSafe(a.createdAt)?.getTime() || 0;
            const bTs = toDateSafe(b.createdAt)?.getTime() || 0;
            return bTs - aTs;
        });
    if (!bucketed.length) {
        if (emptyEl) emptyEl.style.display = 'block';
        return;
    }
    if (emptyEl) emptyEl.style.display = 'none';
    let lastTimestamp = null;
    let lastDateDivider = null;
    const fragment = document.createDocumentFragment();
    bucketed.slice(0, 50).forEach(function (notif) {
        const createdDate = toDateSafe(notif.createdAt) || new Date();
        const isNewDay = !lastDateDivider || !isSameDay(lastDateDivider, createdDate);
        if (isNewDay) {
            const dateDivider = document.createElement('div');
            dateDivider.className = 'message-date-divider';
            dateDivider.textContent = formatChatDateLabel(createdDate);
            fragment.appendChild(dateDivider);
            lastDateDivider = createdDate;
        } else if (needsTimeGapDivider(createdDate, lastTimestamp)) {
            const divider = document.createElement('div');
            divider.className = 'message-time-divider';
            divider.textContent = formatTimeGapDivider(createdDate);
            fragment.appendChild(divider);
        }
        lastTimestamp = createdDate;
        const actorId = notif.actorId || notif.actorUid || '';
        const actorName = notif.actorName || 'Someone';
        const description = buildContentNotificationDescription(notif);
        const meta = formatMessageHoverTimestamp(notif.createdAt) || '';
        const title = (notif.contentTitle || '').trim();
        const thumb = (notif.contentThumbnailUrl || '').trim();
        const row = document.createElement('div');
        row.className = 'inbox-notification-item inbox-notification-item--content';
        const openContentTarget = function () {
            markContentNotificationRead(notif.id, notif);
            const contentType = (notif.contentType || '').toLowerCase();
            if ((contentType === 'post' || contentType === 'posts') && notif.contentId) {
                window.openThread(notif.contentId);
            } else if ((contentType === 'video' || contentType === 'videos') && notif.contentId && typeof window.openVideoDetail === 'function') {
                window.openVideoDetail(notif.contentId);
            } else if ((contentType === 'livestream' || contentType === 'live' || contentType === 'stream' || contentType === 'livestreams') && notif.contentId && typeof window.openLiveSession === 'function') {
                window.openLiveSession(notif.contentId);
            }
        };
        row.innerHTML = `
            <div class="inbox-notification-actor">
                <div class="conversation-avatar-slot">${renderAvatar({
                    uid: actorId || 'actor',
                    username: actorName,
                    displayName: actorName,
                    photoURL: notif.actorPhotoUrl || '',
                    avatarColor: computeAvatarColor(actorName)
                }, { size: 42 })}</div>
                <div class="inbox-notification-text">
                    <div><strong>${escapeHtml(actorName)}</strong> ${escapeHtml(description)}</div>
                    ${meta ? `<div class=\"inbox-notification-meta\">${escapeHtml(meta)}</div>` : ''}
                </div>
            </div>
            ${(thumb || title) ? `<div class="inbox-notification-preview" role="button" tabindex="0">
                <div class="inbox-notification-media">${thumb ? `<img src="${escapeHtml(thumb)}" alt="${escapeHtml(title || 'Content preview')}" loading="lazy" />` : `<div class="inbox-notification-title">${escapeHtml(title || 'View content')}</div>`}</div>
            </div>` : ''}
        `;
        row.onclick = openContentTarget;
        const actorEl = row.querySelector('.inbox-notification-actor');
        if (actorEl) {
            actorEl.onclick = function (event) {
                event.stopPropagation();
                if (actorId && typeof window.openUserProfile === 'function') {
                    window.openUserProfile(actorId, event);
                }
            };
        }
        const previewEl = row.querySelector('.inbox-notification-preview');
        if (previewEl) {
            previewEl.onclick = function (event) {
                event.stopPropagation();
                openContentTarget();
            };
            previewEl.onkeydown = function (event) {
                if (event.key === 'Enter') {
                    event.preventDefault();
                    event.stopPropagation();
                    openContentTarget();
                }
            };
        }
        fragment.appendChild(row);
    });
    listEl.appendChild(fragment);
}

function renderInboxNotifications(mode = 'posts') {
    const listEl = document.getElementById(`inbox-list-${mode}`);
    const emptyEl = document.getElementById(`inbox-empty-${mode}`);
    if (!listEl) return;
    listEl.innerHTML = '';
    const bucketed = inboxNotifications
        .filter(function (notif) { return getNotificationBucket(notif) === mode; })
        .sort(function (a, b) {
            const aTs = toDateSafe(a.createdAt)?.getTime() || 0;
            const bTs = toDateSafe(b.createdAt)?.getTime() || 0;
            return bTs - aTs;
        });
    if (!bucketed.length) {
        if (emptyEl) emptyEl.style.display = 'block';
        return;
    }
    if (emptyEl) emptyEl.style.display = 'none';
    bucketed.slice(0, 50).forEach(function (notif) {
        const isDm = (notif.type || '').toLowerCase() === 'dm';
        const meta = formatMessageHoverTimestamp(notif.createdAt) || '';
        const row = document.createElement('div');
        row.className = 'inbox-notification-item';

        if (isDm) {
            const senderId = notif.fromUid || notif.actorUid || '';
            const sender = senderId ? (getCachedUser(senderId) || {}) : {};
            const senderName = notif.title || resolveDisplayName(sender) || sender.username || 'New message';
            const preview = (notif.body || '').trim();
            row.innerHTML = `
                <div class="conversation-avatar-slot">${renderAvatar({
                    uid: senderId || 'sender',
                    username: sender.username || senderName,
                    displayName: senderName,
                    photoURL: sender.photoURL || '',
                    avatarColor: sender.avatarColor || computeAvatarColor(sender.username || senderName)
                }, { size: 42 })}</div>
                <div class="inbox-notification-text">
                    <div><strong>${escapeHtml(senderName)}</strong></div>
                    ${preview ? `<div class=\"inbox-notification-meta\">${escapeHtml(preview)}</div>` : ''}
                    ${meta ? `<div class=\"inbox-notification-meta\">${escapeHtml(meta)}</div>` : ''}
                </div>
            `;
            row.onclick = function () {
                markNotificationRead(notif);
                if (notif.conversationId) {
                    openConversation(notif.conversationId);
                } else {
                    window.navigateTo('messages');
                }
            };
        } else {
            const actor = notif.actorUid ? (getCachedUser(notif.actorUid) || {}) : {};
            const actorName = resolveDisplayName(actor) || actor.username || 'Someone';
            const actionLabel = formatNotificationAction(notif.actionType || notif.type);
            const entityLabel = formatNotificationEntity(notif.entityType || 'post');
            const preview = (notif.previewText || '').trim();
            row.innerHTML = `
                <div class="conversation-avatar-slot">${renderAvatar({
                    uid: notif.actorUid || 'actor',
                    username: actor.username || actorName,
                    displayName: actorName,
                    photoURL: actor.photoURL || '',
                    avatarColor: actor.avatarColor || computeAvatarColor(actor.username || actorName)
                }, { size: 42 })}</div>
                <div class="inbox-notification-text">
                    <div><strong>${escapeHtml(actorName)}</strong> ${escapeHtml(actionLabel)} your ${escapeHtml(entityLabel)}.</div>
                    ${preview ? `<div class=\"inbox-notification-meta\">${escapeHtml(preview)}</div>` : ''}
                    ${meta ? `<div class=\"inbox-notification-meta\">${escapeHtml(meta)}</div>` : ''}
                </div>
            `;
            row.onclick = function () {
                markNotificationRead(notif);
                const entityType = (notif.entityType || '').toLowerCase();
                if ((entityType === 'post' || entityType === 'posts') && notif.entityId) {
                    window.openThread(notif.entityId);
                } else if ((entityType === 'video' || entityType === 'videos') && notif.entityId && typeof window.openVideoDetail === 'function') {
                    window.openVideoDetail(notif.entityId);
                } else if ((entityType === 'livestream' || entityType === 'live' || entityType === 'stream') && notif.entityId && typeof window.openLiveSession === 'function') {
                    window.openLiveSession(notif.entityId);
                }
            };
        }
        listEl.appendChild(row);
    });
}

function setInboxMode(mode = 'messages', options = {}) {
    const { skipRouteUpdate = false, routeView = currentViewId } = options;
    const contentModes = ['posts', 'videos', 'livestreams'];
    const allowed = ['content', 'messages', 'account'].concat(contentModes);
    const previousMode = inboxMode;
    if (!allowed.includes(mode)) mode = 'messages';
    if (contentModes.includes(mode)) {
        inboxMode = 'content';
        inboxContentPreferred = mode;
        inboxContentFilters[mode] = true;
    } else {
        inboxMode = mode;
        if (inboxMode === 'content' && !contentModes.includes(inboxContentPreferred)) {
            inboxContentPreferred = 'posts';
        }
    }
    document.querySelectorAll('.inbox-tab').forEach(function (btn) {
        const isContentTab = btn.dataset.mode === 'content';
        btn.classList.toggle('active', isContentTab ? inboxMode === 'content' : btn.dataset.mode === inboxMode);
    });
    const panels = {
        content: document.getElementById('inbox-panel-content'),
        messages: document.getElementById('inbox-panel-messages'),
        account: document.getElementById('inbox-panel-account')
    };
    Object.keys(panels).forEach(function (key) {
        const panel = panels[key];
        if (!panel) return;
        panel.classList.toggle('active', key === inboxMode);
    });
    if (inboxMode === 'content') {
        ['posts', 'videos', 'livestreams'].forEach(function (contentMode) {
            renderContentNotificationList(contentMode);
        });
        syncInboxContentFilters();
        if (previousMode !== 'content') {
            void markAllContentNotificationsRead();
        } else if (contentModes.includes(mode)) {
            void markAllContentNotificationsRead(mode);
        }
    } else if (inboxMode !== 'messages') {
        renderInboxNotifications(inboxMode);
    }
    refreshInboxLayout();
    try {
        window.localStorage?.setItem('nexera_last_inbox_mode', inboxMode);
        if (inboxMode === 'content') {
            window.localStorage?.setItem('nexera_last_inbox_contentMode', inboxContentPreferred || 'posts');
        }
    } catch (err) {
        console.warn('Unable to persist inbox mode', err?.message || err);
    }
    if (!skipRouteUpdate && routeView === 'messages') {
        let nextPath = '/inbox';
        if (inboxMode === 'content') {
            const preferred = inboxContentPreferred || 'posts';
            nextPath = preferred === 'posts' ? '/inbox/content' : `/inbox/content?type=${encodeURIComponent(preferred)}`;
        } else if (inboxMode && inboxMode !== 'messages') {
            nextPath = `/inbox/${inboxMode}`;
        } else if (activeConversationId) {
            nextPath = buildMessagesUrl({ conversationId: activeConversationId });
        } else {
            nextPath = buildMessagesUrl();
        }
        if (window.location.pathname !== nextPath) {
            history.pushState({}, '', nextPath);
        }
    }
}

function renderConversationList() {
    const listEl = document.getElementById('conversation-list');
    if (!listEl) return;
    listEl.innerHTML = '';
    const emptyEl = document.getElementById('conversation-list-empty');
    const pinnedEl = document.getElementById('pinned-conversations');
    const currentUid = currentUser?.uid || '';

    if (conversationMappings.length === 0) {
        const emptyText = conversationListFilter === 'requests'
            ? 'No message requests yet.'
            : 'Start a conversation';
        if (emptyEl) {
            emptyEl.textContent = emptyText;
            emptyEl.style.display = 'block';
        } else {
            listEl.innerHTML = `<div class="empty-state">${emptyText}</div>`;
        }
        updateInboxNavBadge();
        return;
    }
    if (emptyEl) emptyEl.style.display = 'none';

    const orderedMappings = conversationMappings
        .slice()
        .sort(function (a, b) {
            if (!!a.archived !== !!b.archived) return a.archived ? 1 : -1;
            if (!!a.pinned !== !!b.pinned) return a.pinned ? -1 : 1;
            const aTsSource = a.lastMessageAt || a.createdAt;
            const bTsSource = b.lastMessageAt || b.createdAt;
            const aTs = aTsSource?.toMillis ? aTsSource.toMillis() : aTsSource?.seconds ? aTsSource.seconds * 1000 : 0;
            const bTs = bTsSource?.toMillis ? bTsSource.toMillis() : bTsSource?.seconds ? bTsSource.seconds * 1000 : 0;
            return bTs - aTs;
        });

    const search = (conversationListSearchTerm || '').toLowerCase();
    let filtered = orderedMappings.filter(function (mapping) {
        const details = conversationDetailsCache[mapping.id] || {};
        if (conversationListFilter === 'requests' && !isConversationRequest(mapping, details)) return false;
        if (conversationListFilter === 'unread' && !(mapping.unreadCount > 0)) return false;
        if (conversationListFilter === 'pinned' && !mapping.pinned) return false;
        if (conversationListFilter === 'archived' && !mapping.archived) return false;
        return true;
    });

    if (search) {
        filtered = filtered.filter(function (mapping) {
            const details = conversationDetailsCache[mapping.id] || {};
            const participants = details.participants || mapping.otherParticipantIds || [];
            const meta = deriveOtherParticipantMeta(participants, currentUid, details);
            const labels = (details.participantNames || meta.names || details.participantUsernames || meta.usernames || []).join(' ').toLowerCase();
            const preview = (mapping.lastMessagePreview || details.lastMessagePreview || '').toLowerCase();
            return labels.includes(search) || preview.includes(search);
        });
    }

    const pinned = filtered.filter(function (mapping) { return mapping.pinned; });
    const unpinned = filtered.filter(function (mapping) { return !mapping.pinned; });
    const visible = unpinned.slice(0, conversationListVisibleCount);
    uiDebugLog('conversation list', {
        total: orderedMappings.length,
        filtered: filtered.length,
        filter: conversationListFilter,
        searchActive: !!search
    });

    const renderRow = function (mapping, targetEl) {
        const details = conversationDetailsCache[mapping.id] || {};
        const participants = details.participants || mapping.otherParticipantIds || [];
        const meta = deriveOtherParticipantMeta(participants, currentUid, details);
        const otherId = meta.otherIds?.[0] || mapping.otherParticipantIds?.[0];
        if (otherId && !getCachedUser(otherId, { allowStale: false }) && !userFetchPromises[otherId]) {
            resolveUserProfile(otherId).then(function () {
                if (currentViewId === 'messages') renderConversationList();
            });
        }
        const otherProfile = otherId ? getCachedUser(otherId) : null;
        const participantLabels = (details.participantNames || meta.names || details.participantUsernames || meta.usernames || []).filter(Boolean);
        const isGroup = (participants || []).length > 2 || details.type === 'group';
        const mergedConvo = {
            ...details,
            participants,
            participantNames: details.participantNames || details.participantUsernames || participantLabels,
            participantUsernames: details.participantUsernames || mapping.otherParticipantUsernames,
            title: details.title || null
        };
        const name = computeConversationTitle(mergedConvo, currentUser?.uid) || 'Conversation';
        const convoAvatar = getConversationAvatarUrl(details);
        let avatarUser = {
            uid: mapping.id || otherId || 'conversation',
            username: name,
            displayName: name,
            photoURL: convoAvatar,
            avatarColor: computeAvatarColor(name)
        };
        if (!isGroup && otherId) {
            avatarUser = {
                ...otherProfile,
                uid: otherId,
                username: otherProfile?.username || name,
                displayName: resolveDisplayName(otherProfile) || name,
                photoURL: normalizeImageUrl(otherProfile?.photoURL) || convoAvatar || normalizeImageUrl(mapping.otherParticipantAvatars?.[0]) || normalizeImageUrl(meta.avatars?.[0]) || '',
                avatarColor: otherProfile?.avatarColor || meta.colors?.[0] || computeAvatarColor(otherProfile?.username || otherId)
            };
        } else if (convoAvatar || mapping.otherParticipantAvatars?.length || meta.avatars?.length) {
            avatarUser.photoURL = convoAvatar || normalizeImageUrl(mapping.otherParticipantAvatars?.[0]) || normalizeImageUrl(meta.avatars?.[0]) || '';
        }
        const avatarHtml = renderAvatar(avatarUser, { size: 42 });

        const item = document.createElement('div');
        item.className = 'conversation-item' + (activeConversationId === mapping.id ? ' active' : '');
        const unread = mapping.unreadCount || 0;
        const muteState = resolveMuteState(mapping.id, mapping);
        const isMuted = muteState.active || (details.mutedBy || []).includes(currentUid);
        updateConversationMappingState(mapping.id, { muted: isMuted, muteUntil: muteState.until || null });
        const unreadLabel = unread > 10 ? '10+' : `${unread}`;
        const flagHtml = unread > 0 ? `<div class="conversation-flags"><span class="badge">${unreadLabel}</span></div>` : '';
        const previewText = escapeHtml(mapping.lastMessagePreview || details.lastMessagePreview || 'Start a chat');
        const tsSource = mapping.lastMessageAt || mapping.createdAt;
        const titleBadge = (!isGroup && otherProfile) ? renderVerifiedBadge(otherProfile) : '';
        item.innerHTML = `<div class="conversation-avatar-slot">${avatarHtml}</div>
            <div class="conversation-body">
                <div class="conversation-title-row">
                    <div class="conversation-title">${escapeHtml(name)}${titleBadge}</div>
                    ${flagHtml}
                </div>
                <div class="conversation-preview">${previewText}</div>
            </div>`;
        item.onclick = function () { openConversation(mapping.id); };
        targetEl.appendChild(item);
    };

    if (pinnedEl) {
        pinnedEl.innerHTML = '';
        if (pinned.length) {
            pinnedEl.style.display = 'block';
            pinnedEl.innerHTML = '<div class="inbox-section-label">Pinned</div>';
            pinned.forEach(function (mapping) { renderRow(mapping, pinnedEl); });
        } else {
            pinnedEl.style.display = 'none';
        }
    }

    if (!visible.length && !pinned.length) {
        if (emptyEl) {
            emptyEl.textContent = conversationListFilter === 'requests'
                ? 'No message requests yet.'
                : 'No conversations match your filters.';
            emptyEl.style.display = 'block';
        }
        updateInboxNavBadge();
        return;
    }

    visible.forEach(function (mapping) { renderRow(mapping, listEl); });
    if (listEl.dataset.scrollBound !== 'true') {
        listEl.addEventListener('scroll', function () {
            const nearBottom = listEl.scrollTop + listEl.clientHeight >= listEl.scrollHeight - 24;
            if (nearBottom && unpinned.length > conversationListVisibleCount) {
                conversationListVisibleCount = Math.min(unpinned.length, conversationListVisibleCount + 30);
                renderConversationList();
            }
        });
        listEl.dataset.scrollBound = 'true';
    }
    updateInboxNavBadge();
}

function renderPinnedMessages(convo = {}, msgs = []) {
    const pinnedContainer = document.getElementById('pinned-messages');
    if (!pinnedContainer) return;
    const pinnedIds = convo.pinnedMessageIds || [];
    if (!pinnedIds.length) { pinnedContainer.style.display = 'none'; pinnedContainer.innerHTML = ''; return; }
    pinnedContainer.innerHTML = '';
    pinnedContainer.style.display = 'flex';
    pinnedIds.slice(0, 10).forEach(function (id) {
        const target = msgs.find(function (m) { return m.id === id; });
        const snippet = target?.text || target?.replyToSnippet || target?.systemPayload?.text || 'Pinned message';
        const row = document.createElement('div');
        row.className = 'pinned-row';
        row.textContent = (snippet || '').slice(0, 120);
        row.onclick = function () { scrollToMessageById(id); };
        pinnedContainer.appendChild(row);
    });
}

function renderMessageHeader(convo = {}) {
    const header = document.getElementById('message-header');
    if (!header) return;
    const participants = convo.participants || [];
    const meta = deriveOtherParticipantMeta(participants, currentUser?.uid, convo);
    const label = computeConversationTitle(convo, currentUser?.uid) || 'Conversation';
    const cid = convo.id || activeConversationId;
    const primaryOtherId = participants.length === 2 ? participants.find(function (uid) { return uid !== currentUser?.uid; }) : null;
    const targetProfileId = primaryOtherId || meta.otherIds?.[0] || null;
    const fallbackAvatar = meta.avatars?.[0] || '';
    if (targetProfileId && !getCachedUser(targetProfileId, { allowStale: false }) && !userFetchPromises[targetProfileId]) {
        resolveUserProfile(targetProfileId).then(function () {
            if (activeConversationId === (convo.id || activeConversationId)) renderMessageHeader(convo);
        });
    }
    let avatarUser = {
        uid: cid || primaryOtherId || 'conversation',
        username: label,
        displayName: label,
        photoURL: getConversationAvatarUrl(convo, fallbackAvatar),
        avatarColor: convo.avatarColor || computeAvatarColor(label)
    };

    if (primaryOtherId && !getConversationAvatarUrl(convo)) {
        const otherMeta = resolveParticipantDisplay(convo, primaryOtherId);
        avatarUser = {
            ...otherMeta.profile,
            uid: primaryOtherId,
            username: otherMeta.username || label,
            displayName: otherMeta.displayName || label,
            photoURL: normalizeImageUrl(otherMeta.avatar),
            avatarColor: otherMeta.avatarColor
        };
    }

    const avatar = renderAvatar(avatarUser, { size: 36 });
    const targetProfile = targetProfileId ? resolveParticipantDisplay(convo, targetProfileId) : null;
    const participantCountLabel = `${participants.length} participant${participants.length === 1 ? '' : 's'}`;
    const subtitleUsername = targetProfile
        ? targetProfile.username
            ? `@${escapeHtml(targetProfile.username)}`
            : escapeHtml(targetProfile.displayName || '')
        : '';
    const subtitle = subtitleUsername ? `${subtitleUsername} • ${participantCountLabel}` : participantCountLabel;
    const profileBtnAttrs = targetProfileId
        ? `class="message-thread-profile-btn" type="button" onclick="window.openUserProfile('${targetProfileId}', event)"`
        : 'class="message-thread-profile-btn" type="button" disabled';

    const optionsDisabledAttr = cid ? '' : ' disabled aria-disabled="true"';
    const canCall = LIVEKIT_ENABLED && !!cid && participants.length > 0 && !!currentUser?.uid;
    const callDisabledAttr = canCall ? '' : ' disabled aria-disabled="true"';
    const callButtons = LIVEKIT_ENABLED
        ? `<button id="dm-call-audio-btn" class="icon-pill dm-call-btn" type="button" onclick="window.startDmCall('audio')" aria-label="Start audio call"${callDisabledAttr}><i class="ph ph-phone"></i></button>
           <button id="dm-call-video-btn" class="icon-pill dm-call-btn" type="button" onclick="window.startDmCall('video')" aria-label="Start video call"${callDisabledAttr}><i class="ph ph-video-camera"></i></button>`
        : '';

    header.innerHTML = `<div class="message-header-shell">
        <button ${profileBtnAttrs}>
            ${avatar}
            <div>
                <div class="message-thread-title-row">${escapeHtml(label)}</div>
                <div class="message-thread-subtitle">${subtitle}</div>
            </div>
        </button>
        <div class="message-header-actions">
            ${callButtons}
            <button class="icon-pill" onclick="window.openConversationSettings('${cid || ''}')" aria-label="Conversation options"${optionsDisabledAttr}><i class="ph ph-dots-three-outline"></i></button>
        </div>
    </div>`;
}

function getCallOverlayElements() {
    return {
        overlay: document.getElementById('call-overlay'),
        name: document.getElementById('call-overlay-name'),
        status: document.getElementById('call-overlay-status'),
        localVideo: document.getElementById('call-local-video'),
        remoteGrid: document.getElementById('call-remote-grid'),
        toggleMic: document.getElementById('call-toggle-mic'),
        toggleCamera: document.getElementById('call-toggle-camera'),
        hangup: document.getElementById('call-hangup'),
        close: document.getElementById('call-overlay-close')
    };
}

function setCallOverlayVisible(visible) {
    const { overlay } = getCallOverlayElements();
    if (!overlay) return;
    overlay.classList.toggle('is-visible', visible);
    overlay.setAttribute('aria-hidden', visible ? 'false' : 'true');
}

function resetCallOverlayMedia() {
    const elements = getCallOverlayElements();
    if (elements.localVideo) {
        elements.localVideo.srcObject = null;
        elements.localVideo.style.display = 'block';
    }
    if (elements.remoteGrid) {
        elements.remoteGrid.innerHTML = '';
    }
}

function updateCallOverlayMeta(convo = {}, statusText = 'Connecting...') {
    const elements = getCallOverlayElements();
    if (!elements.name || !elements.status) return;
    const label = computeConversationTitle(convo, currentUser?.uid) || 'Conversation';
    elements.name.textContent = label;
    elements.status.textContent = statusText;
}

async function renderCallOverlayStatus(conversationId, statusText) {
    const convo = conversationDetailsCache[conversationId]
        || (await fetchConversation(conversationId).catch(function () { return { id: conversationId }; }));
    updateCallOverlayMeta(convo || {}, statusText);
    setCallOverlayVisible(true);
}

function stopCallDocListener() {
    if (callDocUnsubscribe) {
        callDocUnsubscribe();
        callDocUnsubscribe = null;
    }
}

function clearCallSession() {
    stopCallDocListener();
    activeCallSession = null;
    resetCallOverlayMedia();
    setCallOverlayVisible(false);
}

function attachRemoteTrack(track, participant) {
    const elements = getCallOverlayElements();
    if (!elements.remoteGrid) return;
    const tileId = `remote-${participant.sid}-${track.sid}`;
    if (elements.remoteGrid.querySelector(`[data-track-id="${tileId}"]`)) return;
    const tile = document.createElement('div');
    tile.className = 'call-remote-tile';
    tile.dataset.trackId = tileId;
    const video = document.createElement('video');
    video.autoplay = true;
    video.playsInline = true;
    const label = document.createElement('div');
    label.className = 'call-remote-name';
    label.textContent = participant.name || participant.identity || 'Participant';
    tile.appendChild(video);
    tile.appendChild(label);
    elements.remoteGrid.appendChild(tile);
    track.attach(video);
}

function detachRemoteTrack(track, participant) {
    const elements = getCallOverlayElements();
    if (!elements.remoteGrid) return;
    const tileId = `remote-${participant.sid}-${track.sid}`;
    const tile = elements.remoteGrid.querySelector(`[data-track-id="${tileId}"]`);
    if (tile) tile.remove();
}

async function connectToLiveKitRoom(callSession) {
    if (!LIVEKIT_ENABLED) {
        toast('LiveKit is not configured for this environment.', 'info');
        return;
    }
    if (!callSession) return;
    if (livekitRoom) {
        await leaveLiveKitRoom({ updateStatus: false });
    }

    const elements = getCallOverlayElements();
    if (!LivekitRoom || !createLivekitAudioTrack) {
        throw new Error('LiveKit client unavailable.');
    }
    const room = new LivekitRoom();
    livekitRoom = room;
    if (elements.toggleMic) {
        elements.toggleMic.disabled = false;
        elements.toggleMic.textContent = 'Mute';
    }
    if (elements.toggleCamera) {
        elements.toggleCamera.disabled = callSession.type !== 'video';
        elements.toggleCamera.textContent = callSession.type === 'video' ? 'Camera off' : 'Camera off';
    }

    room.on(LivekitRoomEvent.TrackSubscribed, function (track, publication, participant) {
        if (track.kind === 'video') {
            attachRemoteTrack(track, participant);
        } else if (track.kind === 'audio') {
            const audioEl = track.attach();
            audioEl.style.display = 'none';
            document.body.appendChild(audioEl);
        }
    });
    room.on(LivekitRoomEvent.TrackUnsubscribed, function (track, publication, participant) {
        if (track.kind === 'video') {
            detachRemoteTrack(track, participant);
        }
        track.detach().forEach(function (el) { el.remove(); });
    });
    room.on(LivekitRoomEvent.Disconnected, function () {
        leaveLiveKitRoom({ updateStatus: false });
    });

    const tokenFn = httpsCallable(functions, 'livekitCreateToken');
    const response = await tokenFn({
        conversationId: callSession.conversationId,
        roomName: callSession.roomName
    });
    const tokenPayload = response?.data || {};
    if (!tokenPayload?.token || !tokenPayload?.url) {
        throw new Error('LiveKit token unavailable.');
    }
    await room.connect(tokenPayload.url, tokenPayload.token);

    livekitLocalAudioTrack = await createLivekitAudioTrack();
    await room.localParticipant.publishTrack(livekitLocalAudioTrack);

    if (callSession.type === 'video') {
        if (!createLivekitVideoTrack) {
            throw new Error('LiveKit video unavailable.');
        }
        livekitLocalVideoTrack = await createLivekitVideoTrack();
        await room.localParticipant.publishTrack(livekitLocalVideoTrack);
        if (elements.localVideo) {
            elements.localVideo.style.display = 'block';
            livekitLocalVideoTrack.attach(elements.localVideo);
        }
    } else if (elements.localVideo) {
        elements.localVideo.style.display = 'none';
    }

    await updateDoc(doc(db, 'calls', callSession.callId), {
        status: 'active',
        startedAt: serverTimestamp()
    });
    await renderCallOverlayStatus(callSession.conversationId, 'Live');
}

async function leaveLiveKitRoom({ updateStatus = true } = {}) {
    const elements = getCallOverlayElements();
    if (elements.toggleMic) elements.toggleMic.disabled = true;
    if (elements.toggleCamera) elements.toggleCamera.disabled = true;
    if (livekitLocalAudioTrack) {
        livekitLocalAudioTrack.stop();
        livekitLocalAudioTrack = null;
    }
    if (livekitLocalVideoTrack) {
        livekitLocalVideoTrack.stop();
        livekitLocalVideoTrack = null;
    }
    if (livekitRoom) {
        livekitRoom.disconnect();
        livekitRoom = null;
    }
    if (updateStatus && activeCallSession?.callId) {
        await updateDoc(doc(db, 'calls', activeCallSession.callId), {
            status: 'ended',
            endedAt: serverTimestamp()
        }).catch(function (err) {
            console.warn('Unable to update call status', err?.message || err);
        });
    }
    clearCallSession();
}

async function initCallUi() {
    if (callUiInitialized) return;
    callUiInitialized = true;
    const elements = getCallOverlayElements();
    if (elements.toggleMic) elements.toggleMic.disabled = true;
    if (elements.toggleCamera) elements.toggleCamera.disabled = true;
    if (elements.toggleMic) {
        elements.toggleMic.onclick = function () {
            if (!livekitLocalAudioTrack) return;
            const enabled = livekitLocalAudioTrack.isEnabled;
            livekitLocalAudioTrack.setEnabled(!enabled);
            elements.toggleMic.textContent = enabled ? 'Unmute' : 'Mute';
        };
    }
    if (elements.toggleCamera) {
        elements.toggleCamera.onclick = function () {
            if (!livekitLocalVideoTrack) return;
            const enabled = livekitLocalVideoTrack.isEnabled;
            livekitLocalVideoTrack.setEnabled(!enabled);
            elements.toggleCamera.textContent = enabled ? 'Camera on' : 'Camera off';
        };
    }
    if (elements.hangup) {
        elements.hangup.onclick = function () {
            leaveLiveKitRoom({ updateStatus: true }).catch(function (err) {
                console.warn('Hangup failed', err?.message || err);
            });
        };
    }
    if (elements.close) {
        elements.close.onclick = function () {
            leaveLiveKitRoom({ updateStatus: true }).catch(function (err) {
                console.warn('Close call failed', err?.message || err);
            });
        };
    }
}

function listenToCallStatus(callId, conversationId) {
    stopCallDocListener();
    const callRef = doc(db, 'calls', callId);
    callDocUnsubscribe = ListenerRegistry.register(`call:${callId}`, onSnapshot(callRef, function (snap) {
        if (!snap.exists()) {
            leaveLiveKitRoom({ updateStatus: false }).catch(function () {});
            return;
        }
        const data = snap.data() || {};
        if (data.status === 'ended' || data.status === 'missed') {
            leaveLiveKitRoom({ updateStatus: false }).catch(function () {});
            return;
        }
        renderCallOverlayStatus(conversationId, data.status === 'active' ? 'Live' : 'Ringing');
    }, function (err) {
        handleSnapshotError('Call status', err);
    }));
}

async function startLivekitDmCall(type) {
    if (!LIVEKIT_ENABLED) return;
    if (!activeConversationId || !requireAuth()) return;
    if (!['audio', 'video'].includes(type)) return;
    if (activeCallSession) {
        toast('You already have a call in progress.', 'info');
        return;
    }
    const conversationId = activeConversationId;
    const convo = conversationDetailsCache[conversationId] || (await fetchConversation(conversationId));
    const participants = convo?.participants || [];
    if (!participants.length) return;
    const callRef = doc(collection(db, 'calls'));
    const callId = callRef.id;
    const roomName = `dm_${conversationId}`;
    const callPayload = {
        conversationId,
        roomName,
        createdBy: currentUser.uid,
        participants,
        status: 'ringing',
        type,
        createdAt: serverTimestamp(),
        startedAt: null,
        endedAt: null
    };
    const messageRef = doc(collection(db, 'conversations', conversationId, 'messages'));
    const messagePayload = {
        type: 'call_invite',
        callId,
        callType: type,
        createdAt: serverTimestamp(),
        senderId: currentUser.uid
    };
    const batch = writeBatch(db);
    batch.set(callRef, callPayload);
    batch.set(messageRef, messagePayload);
    await batch.commit();

    activeCallSession = { callId, conversationId, roomName, type };
    await renderCallOverlayStatus(conversationId, 'Calling...');
    listenToCallStatus(callId, conversationId);
    try {
        await connectToLiveKitRoom(activeCallSession);
    } catch (error) {
        await updateDoc(callRef, { status: 'missed', endedAt: serverTimestamp() }).catch(function () {});
        clearCallSession();
        toast('Unable to start call.', 'error');
        console.warn('Call connect failed', error?.message || error);
    }
}

async function joinCallInvite(conversationId, callId) {
    if (!LIVEKIT_ENABLED) return;
    if (!conversationId || !callId || !requireAuth()) return;
    if (activeCallSession && activeCallSession.callId !== callId) {
        toast('You already have a call in progress.', 'info');
        return;
    }
    const callSnap = await getDoc(doc(db, 'calls', callId));
    if (!callSnap.exists()) {
        toast('Call is no longer available.', 'info');
        return;
    }
    const callData = callSnap.data() || {};
    if (callData.status === 'ended' || callData.status === 'missed') {
        toast('Call has ended.', 'info');
        return;
    }
    if (callData.conversationId && callData.conversationId !== conversationId) {
        toast('Call does not match this conversation.', 'info');
        return;
    }
    activeCallSession = {
        callId,
        conversationId,
        roomName: callData.roomName || `dm_${conversationId}`,
        type: callData.type || 'audio'
    };
    await renderCallOverlayStatus(conversationId, 'Connecting...');
    listenToCallStatus(callId, conversationId);
    try {
        await connectToLiveKitRoom(activeCallSession);
    } catch (error) {
        toast('Unable to join call.', 'error');
        console.warn('Join call failed', error?.message || error);
        clearCallSession();
    }
}

window.startDmCall = startLivekitDmCall;
window.joinCallInvite = joinCallInvite;

function renderMessages(msgs = [], convo = {}) {
    const body = document.getElementById('message-thread');
    const scrollRegion = getMessageScrollContainer();
    if (!body || !scrollRegion) return;
    const shouldStickToBottom = forceConversationScroll || isNearBottom(scrollRegion);
    const previousOffset = scrollRegion.scrollHeight - scrollRegion.scrollTop;
    body.innerHTML = '';

    let lastTimestamp = null;
    let lastDateDivider = null;
    let lastSenderId = null;
    let latestSelfMessage = null;
    const missingSenders = new Set();
    const missingMedia = new Set();
    const fragment = document.createDocumentFragment();
    const searchTerm = (conversationSearchTerm || '').toLowerCase();
    conversationSearchHits = [];
    renderPinnedMessages(convo, msgs);

    msgs.forEach(function (msg, idx) {
        const createdDate = toDateSafe(msg.createdAt) || new Date();
        const isNewDay = !lastDateDivider || !isSameDay(lastDateDivider, createdDate);
        if (isNewDay) {
            const dateDivider = document.createElement('div');
            dateDivider.className = 'message-date-divider';
            dateDivider.textContent = formatChatDateLabel(createdDate);
            fragment.appendChild(dateDivider);
            lastDateDivider = createdDate;
        } else if (needsTimeGapDivider(lastTimestamp, createdDate)) {
            const divider = document.createElement('div');
            divider.className = 'message-time-divider';
            divider.textContent = formatTimeGapDivider(createdDate);
            fragment.appendChild(divider);
        }
        lastTimestamp = createdDate;

        const nextMsg = msgs[idx + 1];
        const nextDate = nextMsg ? (toDateSafe(nextMsg.createdAt) || new Date()) : null;
        const nextIsNewDay = nextDate ? !isSameDay(createdDate, nextDate) : false;
        const showAvatar = msg.senderId !== currentUser?.uid && ((nextMsg?.senderId !== msg.senderId) || nextIsNewDay || (nextDate && needsTimeGapDivider(createdDate, nextDate)));
        const isSelf = msg.senderId === currentUser?.uid;
        if (!isSelf && msg.senderId && !getCachedUser(msg.senderId, { allowStale: false }) && !userFetchPromises[msg.senderId]) {
            missingSenders.add(msg.senderId);
        }
        const row = document.createElement('div');
        row.className = 'message-row ' + (isSelf ? 'self' : 'other');
        if (lastSenderId === msg.senderId) row.classList.add('stacked');
        row.dataset.messageId = msg.id;

        const avatarSlot = isSelf ? null : document.createElement('div');
        if (avatarSlot) {
            avatarSlot.className = 'message-avatar-slot' + (showAvatar ? '' : ' placeholder');
            if (showAvatar) {
                const senderMeta = resolveParticipantDisplay(convo, msg.senderId);
                avatarSlot.innerHTML = renderAvatar({
                    uid: msg.senderId,
                    username: senderMeta.username,
                    displayName: senderMeta.displayName,
                    photoURL: senderMeta.avatar,
                    avatarColor: senderMeta.avatarColor
                }, { size: 42 });
            } else {
                avatarSlot.classList.add('placeholder');
            }
        }

        const bubbleWrap = document.createElement('div');
        bubbleWrap.className = 'message-bubble-wrap ' + (isSelf ? 'self' : 'other');

        const bubble = document.createElement('div');
        bubble.className = 'message-bubble ' + (isSelf ? 'self' : 'other');
        bubble.dataset.messageId = msg.id;
        const senderLabel = !isSelf && msg.senderUsername ? `<div class="message-sender-label">${escapeHtml(msg.senderUsername)}</div>` : '';
        const replyHeaderNeeded = msg.replyToMessageId || msg.replyToSnippet;
        const baseTextRaw = msg.text || '';
        const hasSearchMatch = searchTerm && baseTextRaw.toLowerCase().includes(searchTerm);
        let textMarkup = escapeHtml(baseTextRaw);
        if (hasSearchMatch) {
            conversationSearchHits.push(msg.id);
            const regex = new RegExp(`(${searchTerm.replace(/[.*+?^${}()|[\\]\\]/g, '\\$&')})`, 'ig');
            textMarkup = escapeHtml(baseTextRaw).replace(regex, '<mark>$1</mark>');
        }

        let content = textMarkup;
        if (msg.type === 'post_ref') {
            const refBtn = msg.postId ? `<button class="icon-pill" style="margin-top:6px;" onclick="window.openThread('${msg.postId}')"><i class="ph ph-arrow-square-out"></i> View post</button>` : '';
            content = `<div style="display:flex; flex-direction:column; gap:6px;"><div style="font-weight:700;">Shared a post</div><div style="font-size:0.9rem;">${textMarkup}</div>${refBtn}</div>`;
        }
        if (msg.type === 'call_invite') {
            const callType = msg.callType === 'video' ? 'video' : 'audio';
            const joinBtn = (LIVEKIT_ENABLED && msg.callId)
                ? `<button class="call-join-btn" onclick="window.joinCallInvite('${convo.id || activeConversationId}', '${msg.callId}')">Join call</button>`
                : '';
            content = `<div class="message-call-invite">
                <div class="call-invite-title">${escapeHtml(callType.charAt(0).toUpperCase() + callType.slice(1))} call invite</div>
                <div style="color:var(--text-muted); font-size:0.85rem;">Tap to join when ready.</div>
                <div class="call-invite-actions">${joinBtn}</div>
            </div>`;
        }

        const attachments = Array.isArray(msg.attachments) ? msg.attachments.slice() : [];
        const mediaRef = msg.mediaPath || msg.mediaURL;
        if (mediaRef && !attachments.length) {
            attachments.push({ url: msg.mediaURL || null, storagePath: msg.mediaPath || null, type: msg.mediaType || msg.type, name: msg.fileName || 'Attachment' });
        }
        const hasMediaAttachment = attachments.length > 0;

        if (!hasMediaAttachment && msg.type === 'image' && mediaRef) {
            const resolved = resolveDmMediaUrl(mediaRef);
            if (resolved.status === 'denied') {
                content = `<div style="font-size:0.8rem; color:var(--text-muted);">${getDmMediaFallbackText('denied')}</div>`;
            } else if (!resolved.url && mediaRef && !/^https?:\/\//i.test(mediaRef)) {
                missingMedia.add(mediaRef);
                content = `<div style="font-size:0.8rem; color:var(--text-muted);">${getDmMediaFallbackText(resolved.status)}</div>`;
            } else {
                content = `<img src="${resolved.url || mediaRef}" style="max-width:240px; border-radius:12px;">`;
            }
        } else if (!hasMediaAttachment && msg.type === 'video' && mediaRef) {
            const resolved = resolveDmMediaUrl(mediaRef);
            if (resolved.status === 'denied') {
                content = `<div style="font-size:0.8rem; color:var(--text-muted);">${getDmMediaFallbackText('denied')}</div>`;
            } else if (!resolved.url && mediaRef && !/^https?:\/\//i.test(mediaRef)) {
                missingMedia.add(mediaRef);
                content = `<div style="font-size:0.8rem; color:var(--text-muted);">${getDmMediaFallbackText(resolved.status)}</div>`;
            } else {
                content = `<video src="${resolved.url || mediaRef}" controls style="max-width:260px; border-radius:12px;"></video>`;
            }
        }

        const forwardedTag = (msg.forwardedFromSenderId || msg.forwardedFromConversationId)
            ? `<div class="forwarded-tag"><i class="ph ph-arrow-u-up-right"></i>Forwarded${msg.forwardedFromSenderId ? ` from ${escapeHtml(msg.forwardedFromSenderId)}` : ''}</div>`
            : '';

        const contentWrap = document.createElement('div');
        contentWrap.innerHTML = `${senderLabel}${forwardedTag}${content}`;

        if (replyHeaderNeeded) {
            const replyHeader = document.createElement('div');
            replyHeader.className = 'reply-header';
            replyHeader.innerHTML = `<strong>${escapeHtml(msg.replyToSenderId || 'Reply')}</strong> — ${escapeHtml((msg.replyToSnippet || '').slice(0, 140))}`;
            replyHeader.onclick = function (e) { e.stopPropagation(); scrollToMessageById(msg.replyToMessageId); };
            bubble.appendChild(replyHeader);
        }

        if (attachments.length) {
            const attachmentRow = document.createElement('div');
            attachmentRow.className = 'message-attachments';
            attachments.forEach(function (att) {
                const tile = document.createElement('div');
                tile.className = 'message-attachment-tile';
                const isImage = (att.type || '').includes('image');
                const mediaPointer = att.storagePath || att.url || '';
                const resolvedEntry = resolveDmMediaUrl(mediaPointer);
                if (resolvedEntry.status === 'denied') {
                    tile.classList.add('denied');
                    tile.innerHTML = `<div class="attachment-denied">${getDmMediaFallbackText('denied')}</div>`;
                    tile.title = getDmMediaFallbackText('denied');
                    tile.onclick = function (e) {
                        e.stopPropagation();
                        toast(getDmMediaFallbackText('denied'), 'error');
                    };
                    attachmentRow.appendChild(tile);
                    return;
                }
                if (isImage) {
                    const img = document.createElement('img');
                    if (resolvedEntry.url) {
                        img.src = resolvedEntry.url;
                    }
                    img.alt = att.name || 'Attachment';
                    tile.appendChild(img);
                } else if ((att.type || '').includes('video')) {
                    tile.innerHTML = '<div class="attachment-icon"><i class="ph ph-play"></i></div>';
                } else {
                    tile.innerHTML = '<div class="attachment-icon"><i class="ph ph-paperclip"></i></div>';
                }
                if (!resolvedEntry.url && mediaPointer && !/^https?:\/\//i.test(mediaPointer)) {
                    missingMedia.add(mediaPointer);
                }
                tile.onclick = function (e) {
                    e.stopPropagation();
                    const fallbackUrl = resolvedEntry.url || (/^https?:\/\//i.test(mediaPointer) ? mediaPointer : null);
                    if (!fallbackUrl) {
                        toast(getDmMediaFallbackText(resolvedEntry.status), 'error');
                        return;
                    }
                    openFullscreenMedia(fallbackUrl, (att.type || '').includes('video') ? 'video' : 'image');
                };
                attachmentRow.appendChild(tile);
            });
            bubble.appendChild(attachmentRow);
        }

        bubble.appendChild(contentWrap);

        bubble.onclick = function (e) { e.stopPropagation(); showMessageActionsMenu(msg, bubble, convo); };

        const reactions = msg.reactions || {};
        const reactionTotal = Object.keys(reactions || {}).reduce(function (acc, emoji) { return acc + (reactions[emoji]?.length || 0); }, 0);
        if (reactionTotal > 0) {
            const reactionRow = document.createElement('div');
            reactionRow.className = 'reaction-row';
            Object.keys(reactions || {}).forEach(function (emoji) {
                const users = reactions[emoji] || [];
                if (!users.length) return;
                const pill = document.createElement('div');
                const youReacted = users.includes(currentUser?.uid);
                pill.className = 'reaction-pill' + (youReacted ? ' active' : '');
                pill.textContent = `${emoji} ${users.length}`;
                pill.onclick = function (e) { e.stopPropagation(); toggleReaction(convo.id || activeConversationId, msg.id, emoji, youReacted); };
                reactionRow.appendChild(pill);
            });
            bubbleWrap.appendChild(reactionRow);
        }

        const editedLabel = msg.editedAt ? ' · edited' : '';
        const time = document.createElement('div');
        time.className = 'message-meta-time ' + (isSelf ? 'self' : 'other');
        time.textContent = `${formatMessageHoverTimestamp(msg.createdAt) || ''}${editedLabel}`;

        row.oncontextmenu = function (e) { e.preventDefault(); showMessageActionsMenu(msg, bubble, convo); };

        bubbleWrap.appendChild(bubble);
        bubbleWrap.appendChild(time);

        if (isSelf) {
            row.appendChild(bubbleWrap);
            latestSelfMessage = { message: msg, row };
        } else {
            if (avatarSlot) row.appendChild(avatarSlot);
            row.appendChild(bubbleWrap);
        }

        fragment.appendChild(row);
        lastSenderId = msg.senderId;
    });

    if (latestSelfMessage) {
        const statusText = deriveMessageStatus(convo, latestSelfMessage.message);
        if (statusText) {
            const status = document.createElement('div');
            status.className = 'message-status self';
            status.textContent = statusText;
            const nextNode = latestSelfMessage.row.nextSibling;
            if (nextNode) {
                fragment.insertBefore(status, nextNode);
            } else {
                fragment.appendChild(status);
            }
        }
    }

    body.appendChild(fragment);

    if (missingSenders.size && convo.id) {
        refreshUserProfiles(Array.from(missingSenders), { force: true }).then(function () {
            if (activeConversationId === convo.id) {
                renderMessageHeader(convo);
                renderMessages(msgs, convo);
            }
        });
    }

    if (missingMedia.size && convo.id) {
        Promise.all(Array.from(missingMedia).map(fetchDmMediaUrl)).then(function () {
            if (activeConversationId === convo.id) {
                renderMessages(msgs, convo);
            }
        });
    }
    if (shouldStickToBottom) {
        scrollMessagesToBottom();
        const media = scrollRegion.querySelectorAll('img, video');
        media.forEach(function (node) {
            const handler = function () { scrollMessagesToBottom(); };
            if (node.tagName === 'IMG') {
                if (!node.complete) node.addEventListener('load', handler, { once: true });
            } else {
                node.addEventListener('loadedmetadata', handler, { once: true });
            }
        });
    } else {
        scrollRegion.scrollTop = Math.max(0, scrollRegion.scrollHeight - previousOffset);
    }
    if (forceConversationScroll) {
        forceConversationScroll = false;
    }
}

function deriveMessageStatus(convo = {}, message = {}) {
    if (!currentUser) return '';
    const others = (convo.participants || []).filter(function (uid) { return uid !== currentUser.uid; });
    if (others.length === 0) return 'Sent';
    const msgTs = getMessageTimestampMs(message);
    const readMap = convo.lastReadAt || {};
    const deliveredMap = convo.lastDeliveredAt || {};

    const readTimestamps = others.map(function (uid) { return toDateSafe(readMap[uid]); }).filter(Boolean);
    const deliveredTimestamps = others.map(function (uid) { return toDateSafe(deliveredMap[uid]); }).filter(Boolean);

    const allRead = readTimestamps.length === others.length && readTimestamps.every(function (ts) { return ts.getTime() >= msgTs; });
    if (allRead) {
        const latestRead = new Date(Math.max.apply(null, readTimestamps.map(function (ts) { return ts.getTime(); })));
        const label = formatDateTime(latestRead);
        return label ? `Read at ${label}` : 'Read';
    }

    const allDelivered = deliveredTimestamps.length === others.length && deliveredTimestamps.every(function (ts) { return ts.getTime() >= msgTs; });
    if (allDelivered) {
        const latestDelivered = new Date(Math.max.apply(null, deliveredTimestamps.map(function (ts) { return ts.getTime(); })));
        const label = formatDateTime(latestDelivered);
        return label ? `Delivered at ${label}` : 'Delivered';
    }

    return 'Sent';
}

function scrollToMessageById(messageId) {
    if (!messageId) return;
    const body = document.getElementById('message-thread');
    if (!body) return;
    const target = body.querySelector(`[data-message-id="${messageId}"]`);
    if (target) {
        target.scrollIntoView({ behavior: 'smooth', block: 'center' });
        target.classList.add('highlight');
        setTimeout(function () { target.classList.remove('highlight'); }, 1500);
    }
}

function handleConversationSearch(term = '') {
    conversationSearchTerm = term || '';
    conversationSearchIndex = 0;
    const convo = conversationDetailsCache[activeConversationId] || {};
    renderMessages(messageThreadCache[activeConversationId] || [], convo);
    if (conversationSearchHits.length) navigateConversationSearch(0);
}

function handleConversationListSearch(event) {
    conversationListSearchTerm = (event?.target?.value || '').trim();
    conversationListVisibleCount = 30;
    renderConversationList();
}

function setConversationFilter(filter = 'all') {
    conversationListFilter = filter || 'all';
    conversationListVisibleCount = 30;
    document.querySelectorAll('.inbox-filter').forEach(function (btn) {
        btn.classList.toggle('active', btn.dataset.filter === conversationListFilter);
    });
    renderConversationList();
}

function initInboxNotifications(userId) {
    if (inboxNotificationsUnsubscribe) {
        try { inboxNotificationsUnsubscribe(); } catch (err) { }
        inboxNotificationsUnsubscribe = null;
    }
    if (!userId) return;
    const notifRef = query(
        collection(db, 'users', userId, 'notifications'),
        where('type', '==', 'dm'),
        orderBy('createdAt', 'desc'),
        limit(50)
    );
    inboxNotificationsUnsubscribe = onSnapshot(notifRef, function (snap) {
        inboxNotifications = snap.docs.map(function (docSnap) { return ({ id: docSnap.id, ...docSnap.data() }); });
        updateInboxNotificationCounts();
        if (inboxMode === 'content') {
            return;
        }
        if (inboxMode && inboxMode !== 'messages') {
            renderInboxNotifications(inboxMode);
        }
    }, function (err) {
        handleSnapshotError('Inbox notifications', err);
    });
}

function initContentNotifications(userId) {
    if (contentNotificationsUnsubscribe) {
        try { contentNotificationsUnsubscribe(); } catch (err) { }
        contentNotificationsUnsubscribe = null;
    }
    if (!userId) return;
    contentNotificationsLegacyFetched = false;
    const DEBUG_NOTIFS = (location.hostname === 'localhost')
        || window.localStorage?.getItem('debugNotifs') === '1';
    const notifRef = query(
        collection(db, 'users', userId, 'notifications'),
        where('type', '==', 'content'),
        orderBy('createdAt', 'desc'),
        limit(200)
    );
    if (DEBUG_NOTIFS) console.debug('[Notifs] content listener: typed');
    const applyContentNotifications = function (nextNotifications = []) {
        contentNotifications = nextNotifications;
        updateInboxNotificationCounts();
        if (inboxMode === 'content') {
            renderContentNotificationList('posts');
            renderContentNotificationList('videos');
            renderContentNotificationList('livestreams');
            syncInboxContentFilters();
        }
    };
    contentNotificationsUnsubscribe = onSnapshot(notifRef, function (snap) {
        if (DEBUG_NOTIFS) console.debug('[Notifs] content snapshot', snap.size);
        const typedNotifications = snap.docs
            .map(function (docSnap) { return ({ id: docSnap.id, ...docSnap.data() }); });
        applyContentNotifications(typedNotifications);
        if (!snap.empty || contentNotificationsLegacyFetched) return;
        contentNotificationsLegacyFetched = true;
        if (DEBUG_NOTIFS) console.debug('[Notifs] content fallback: legacy getDocs');
        const legacyRef = query(collection(db, 'users', userId, 'notifications'), orderBy('createdAt', 'desc'), limit(200));
        getDocs(legacyRef).then(function (legacySnap) {
            if (contentNotifications.length) return;
            const existingIds = new Set(typedNotifications.map(function (notif) { return notif.id; }));
            const legacyNotifications = legacySnap.docs
                .map(function (docSnap) { return ({ id: docSnap.id, ...docSnap.data() }); })
                .filter(function (notif) {
                    if ((notif.type || '').toLowerCase() === 'dm') return false;
                    return !!getContentNotificationBucket(notif);
                })
                .filter(function (notif) { return !existingIds.has(notif.id); });
            const merged = typedNotifications.concat(legacyNotifications).sort(function (a, b) {
                const aTs = toDateSafe(a.createdAt)?.getTime() || 0;
                const bTs = toDateSafe(b.createdAt)?.getTime() || 0;
                return bTs - aTs;
            });
            applyContentNotifications(merged);
        }).catch(function (err) {
            if (DEBUG_NOTIFS) console.warn('[Notifs] content fallback error', err);
        });
    }, function (err) {
        if (DEBUG_NOTIFS) console.warn('[Notifs] content snapshot error', err);
        handleSnapshotError('Content notifications', err);
    });
}

window.setInboxMode = setInboxMode;

function navigateConversationSearch(step = 0) {
    if (!conversationSearchHits.length) return;
    if (step !== 0) {
        conversationSearchIndex = (conversationSearchIndex + step + conversationSearchHits.length) % conversationSearchHits.length;
    }
    const targetId = conversationSearchHits[conversationSearchIndex];
    scrollToMessageById(targetId);
}

window.mobileMessagesBack = function () {
    if (!isMobileViewport()) return;
    document.body.classList.remove('mobile-thread-open');
};

window.toggleConversationInfoPanel = function () {
    const panel = document.getElementById('message-info-panel');
    if (!panel) return;
    panel.classList.toggle('is-open');
};

function clearReplyContext() {
    activeReplyContext = null;
    editingMessageId = null;
    const bar = document.getElementById('message-reply-preview');
    const compose = document.querySelector('.message-compose');
    if (bar) { bar.style.display = 'none'; bar.innerHTML = ''; }
    if (compose) compose.classList.remove('editing');
}

function setMessageSendBusy(isBusy, label = '') {
    const sendButton = document.querySelector('.message-compose .create-btn-sidebar');
    if (!sendButton) return;
    if (!sendButton.dataset.originalLabel) {
        sendButton.dataset.originalLabel = sendButton.textContent || 'Send';
    }
    sendButton.disabled = isBusy;
    sendButton.textContent = isBusy ? (label || 'Uploading…') : sendButton.dataset.originalLabel;
}

function setMessageUploadState(nextState = {}) {
    messageUploadState = {
        ...messageUploadState,
        ...nextState
    };
    const isUploading = messageUploadState.status === 'uploading';
    setMessageSendBusy(isUploading, isUploading ? `Uploading… ${messageUploadState.progress}%` : '');
    renderAttachmentPreview();
}

function resetMessageUploadState() {
    messageUploadState = {
        status: 'idle',
        progress: 0,
        error: null,
        retries: 0,
        conversationId: null,
        messageId: null,
        files: []
    };
    setMessageSendBusy(false);
}

function clearAttachmentPreview() {
    const preview = document.getElementById('message-attachment-preview');
    pendingMessageAttachments = [];
    if (preview) { preview.innerHTML = ''; preview.style.display = 'none'; }
    resetMessageUploadState();
}

function renderAttachmentPreview() {
    const preview = document.getElementById('message-attachment-preview');
    if (!preview) return;
    preview.innerHTML = '';
    if (!pendingMessageAttachments.length) { preview.style.display = 'none'; return; }
    preview.style.display = 'flex';
    pendingMessageAttachments.forEach(function (file, idx) {
        const tile = document.createElement('div');
        tile.className = 'attachment-preview-chip';
        const label = document.createElement('div');
        label.className = 'attachment-preview-label';
        const isImage = file.type && file.type.startsWith('image');
        const thumb = document.createElement('div');
        thumb.className = 'attachment-preview-thumb';
        if (isImage) {
            const img = document.createElement('img');
            img.src = URL.createObjectURL(file);
            img.alt = file.name;
            thumb.appendChild(img);
        } else {
            thumb.innerHTML = '<i class="ph ph-paperclip"></i>';
        }
        label.textContent = file.name || 'Attachment';
        const removeBtn = document.createElement('button');
        removeBtn.className = 'attachment-remove-btn';
        removeBtn.innerHTML = '<i class="ph ph-x"></i>';
        removeBtn.onclick = function () { removePendingAttachment(idx); };
        tile.appendChild(thumb);
        tile.appendChild(label);
        tile.appendChild(removeBtn);
        preview.appendChild(tile);
    });

    if (messageUploadState.status === 'uploading') {
        const statusRow = document.createElement('div');
        statusRow.className = 'attachment-upload-status';
        statusRow.style.display = 'flex';
        statusRow.style.alignItems = 'center';
        statusRow.style.gap = '0.5rem';
        statusRow.innerHTML = `
            <span class="inline-spinner" aria-hidden="true"></span>
            <span style="font-size:0.85rem; color:var(--text-muted);">Uploading… ${messageUploadState.progress}%</span>
        `;
        preview.appendChild(statusRow);
    } else if (messageUploadState.status === 'error') {
        const statusRow = document.createElement('div');
        statusRow.className = 'attachment-upload-status';
        statusRow.style.display = 'flex';
        statusRow.style.alignItems = 'center';
        statusRow.style.gap = '0.5rem';
        statusRow.innerHTML = `
            <span style="font-size:0.85rem; color:var(--text-muted);">${escapeHtml(messageUploadState.error || 'Upload failed')}</span>
            <button class="icon-pill" type="button" onclick="window.retryMessageAttachmentUpload()">Retry</button>
        `;
        preview.appendChild(statusRow);
    }
}

function removePendingAttachment(index) {
    if (index < 0 || index >= pendingMessageAttachments.length) return;
    pendingMessageAttachments.splice(index, 1);
    renderAttachmentPreview();
}

function handleMessageFileChange(event) {
    const files = filterDmAttachments(Array.from(event?.target?.files || []));
    if (!files.length) return;
    pendingMessageAttachments = pendingMessageAttachments.concat(files);
    if (messageUploadState.status === 'error') {
        resetMessageUploadState();
    }
    renderAttachmentPreview();
    if (event?.target) event.target.value = '';
}

window.handleMessageFileChange = handleMessageFileChange;
window.removePendingAttachment = removePendingAttachment;

window.retryMessageAttachmentUpload = async function () {
    if (messageUploadState.status !== 'error') return;
    const { conversationId, files } = messageUploadState;
    if (!conversationId || !files.length) return;
    await window.sendMessage(conversationId);
};

function renderReplyPreviewBar() {
    const bar = document.getElementById('message-reply-preview');
    const compose = document.querySelector('.message-compose');
    if (!bar) return;
    if (!activeReplyContext) { bar.style.display = 'none'; bar.innerHTML = ''; if (compose) compose.classList.remove('editing'); return; }
    bar.style.display = 'flex';
    const label = activeReplyContext.mode === 'quote' ? 'Quoting' : activeReplyContext.mode === 'forward' ? 'Forwarding' : 'Replying';
    bar.innerHTML = `<div class="reply-meta"><strong>${label}:</strong> ${escapeHtml((activeReplyContext.snippet || '').slice(0, 140))}</div><button class="icon-pill" onclick="window.clearReplyContext()"><i class="ph ph-x"></i></button>`;
    if (compose && activeReplyContext.mode === 'edit') compose.classList.add('editing');
}

function setReplyContext(message, mode = 'reply', convoId = activeConversationId) {
    if (!message) return;
    activeReplyContext = {
        conversationId: convoId,
        targetMessageId: message.id,
        senderId: message.senderId,
        snippet: message.text || message.replyToSnippet || '',
        mode
    };
    if (mode === 'edit') {
        editingMessageId = message.id;
        const input = document.getElementById('message-input');
        if (input) input.value = message.text || '';
    }
    renderReplyPreviewBar();
}

function startForwardFlow(message) {
    if (!message || !conversationMappings.length) { toast('No conversations available to forward', 'info'); return; }
    const options = conversationMappings.map(function (m) { return { id: m.id, label: computeConversationTitle(conversationDetailsCache[m.id] || m, currentUser?.uid) || 'Conversation' }; });
    openConfirmModal({
        title: 'Forward message',
        message: 'Select a conversation to forward this message to.',
        buildContent: function (container) {
            const select = document.createElement('select');
            select.className = 'form-input';
            options.forEach(function (opt) {
                const o = document.createElement('option');
                o.value = opt.id; o.textContent = opt.label;
                select.appendChild(o);
            });
            container.appendChild(select);
            return function () { return { convoId: select.value }; };
        },
        confirmText: 'Forward',
        onConfirm: async function (data) {
            const convoId = data?.convoId;
            if (!convoId) return;
            activeReplyContext = {
                conversationId: convoId,
                targetMessageId: message.id,
                senderId: message.senderId,
                snippet: message.text || '',
                mode: 'forward'
            };
            await sendChatPayload(convoId, { text: message.text || '', type: message.type || 'text', mediaURL: message.mediaURL || null, mediaType: message.mediaType || null });
            clearReplyContext();
        }
    });
}

async function deleteMessage(conversationId, messageId) {
    if (!conversationId || !messageId) return;
    const confirmed = await openConfirmModal({ title: 'Delete message?', message: 'This action cannot be undone.', confirmText: 'Delete' });
    if (!confirmed) return;
    try { await deleteDoc(doc(db, 'conversations', conversationId, 'messages', messageId)); } catch (e) { console.warn('Delete failed', e?.message || e); }
}

async function toggleMessagePin(conversationId, messageId) {
    if (!conversationId || !messageId) return;
    const convo = conversationDetailsCache[conversationId] || {};
    const pinned = convo.pinnedMessageIds || [];
    const isPinned = pinned.includes(messageId);
    const limitReached = !isPinned && pinned.length >= 10;
    if (limitReached) { toast('Pin limit reached', 'error'); return; }
    try {
        await updateDoc(doc(db, 'conversations', conversationId), { pinnedMessageIds: isPinned ? arrayRemove(messageId) : arrayUnion(messageId) });
    } catch (e) { console.warn('Pin toggle failed', e?.message || e); }
}

function showReactionPicker(conversationId, messageId, anchor) {
    const menu = document.createElement('div');
    menu.className = 'message-actions-menu menu-surface';
    REACTION_EMOJIS.forEach(function (emoji) {
        const btn = document.createElement('button');
        btn.textContent = emoji;
        btn.onclick = function () { toggleReaction(conversationId, messageId, emoji); closeMessageActionsMenu(); };
        menu.appendChild(btn);
    });
    closeMessageActionsMenu();
    messageActionsMenuEl = menu;
    document.body.appendChild(menu);
    const rect = anchor?.getBoundingClientRect();
    const menuRect = menu.getBoundingClientRect();
    const top = rect ? (rect.bottom + window.scrollY + 6) : (window.scrollY + (window.innerHeight / 2) - (menuRect.height / 2));
    const desiredLeft = rect ? (rect.left + window.scrollX) : (window.scrollX + (window.innerWidth / 2) - (menuRect.width / 2));
    const boundedLeft = Math.min(window.innerWidth - menuRect.width - 8, Math.max(8, desiredLeft));
    menu.style.top = `${top}px`;
    menu.style.left = `${boundedLeft}px`;
    document.addEventListener('click', closeMessageActionsMenu, { once: true });
}

function toggleReaction(conversationId, messageId, emoji, remove = false) {
    if (!conversationId || !messageId || !emoji || !currentUser) return;
    const msgRef = doc(db, 'conversations', conversationId, 'messages', messageId);
    const key = `reactions.${emoji}`;
    const update = remove ? arrayRemove(currentUser.uid) : arrayUnion(currentUser.uid);
    updateDoc(msgRef, { [key]: update }).catch(function (e) { console.warn('Reaction update failed', e?.message || e); });
}

function closeMessageActionsMenu() {
    if (messageActionsMenuEl) {
        try { messageActionsMenuEl.remove(); } catch (e) { }
        messageActionsMenuEl = null;
    }
}

function resolvePrimaryImageAttachment(message = {}) {
    const mediaRef = message.mediaPath || message.mediaURL;
    if (message.type === 'image' && mediaRef) {
        const resolved = resolveDmMediaUrl(mediaRef);
        if (resolved.status === 'ok' && resolved.url) return { url: resolved.url, type: 'image' };
    }
    const attachments = Array.isArray(message.attachments) ? message.attachments : [];
    const imageAttachment = attachments.find(function (att) { return (att.type || '').includes('image') && (att.storagePath || att.url); });
    if (imageAttachment) {
        const resolved = resolveDmMediaUrl(imageAttachment.storagePath || imageAttachment.url);
        if (resolved.status === 'ok' && resolved.url) return { url: resolved.url, type: 'image' };
    }
    if (mediaRef && ((message.mediaType || message.type || '').includes('image'))) {
        const resolved = resolveDmMediaUrl(mediaRef);
        if (resolved.status === 'ok' && resolved.url) return { url: resolved.url, type: 'image' };
    }
    return null;
}

function showMessageActionsMenu(message, anchor, convo = {}) {
    closeMessageActionsMenu();
    const menu = document.createElement('div');
    menu.className = 'message-actions-menu menu-surface';
    const actions = [];
    const primaryImage = resolvePrimaryImageAttachment(message);
    if (primaryImage) {
        actions.push({ label: 'View image', handler: function () { openFullscreenMedia(primaryImage.url, 'image'); } });
    }
    actions.push(
        { label: 'Reply', handler: function () { setReplyContext(message, 'reply', convo.id); } },
        { label: 'Quote', handler: function () { setReplyContext(message, 'quote', convo.id); } },
        { label: 'Forward', handler: function () { startForwardFlow(message); } },
        { label: 'React', handler: function () { showReactionPicker(convo.id || activeConversationId, message.id, anchor); } }
    );
    if (message.senderId === currentUser?.uid) {
        actions.push({ label: 'Edit', handler: function () { setReplyContext(message, 'edit', convo.id); } });
        actions.push({ label: 'Delete', handler: function () { deleteMessage(convo.id || activeConversationId, message.id); } });
    }
    actions.push({ label: (convo.pinnedMessageIds || []).includes(message.id) ? 'Unpin' : 'Pin', handler: function () { toggleMessagePin(convo.id || activeConversationId, message.id); } });
    actions.forEach(function (action) {
        const btn = document.createElement('button');
        btn.textContent = action.label;
        btn.onclick = function () { action.handler(); closeMessageActionsMenu(); };
        menu.appendChild(btn);
    });
    document.body.appendChild(menu);
    messageActionsMenuEl = menu;
    const rect = anchor?.getBoundingClientRect();
    const top = (rect?.bottom || 0) + 8;
    const left = (rect?.left || 0) - 20;
    menu.style.top = `${top}px`;
    menu.style.left = `${left}px`;
    document.addEventListener('click', closeMessageActionsMenu, { once: true });
}

function renderTypingIndicator(convo = {}) {
    const indicator = document.getElementById('typing-indicator');
    if (!indicator) return;
    const typing = convo.typing || {};
    const active = Object.keys(typing || {}).filter(function (uid) { return uid !== currentUser?.uid && typing[uid]; });
    if (!active.length) {
        indicator.style.display = 'none';
        indicator.innerHTML = '';
        return;
    }

    const names = active.map(function (uid) {
        const meta = resolveParticipantDisplay(convo, uid);
        return meta.displayName || meta.username || 'Someone';
    });
    const label = active.length === 1 ? `${names[0]} is typing...` : 'Several people are typing...';
    indicator.innerHTML = `<div class="typing-dots"><span></span><span></span><span></span></div><span>${escapeHtml(label)}</span>`;
    indicator.style.display = 'flex';
}

function renderConversationUnavailable(message = 'You no longer have access to this conversation.') {
    const header = document.getElementById('message-header');
    const body = document.getElementById('message-thread');
    if (header) header.textContent = message;
    if (body) body.innerHTML = `<div class="empty-state" style="padding:16px;">${escapeHtml(message)}</div>`;
}

function handleConversationAccessLoss(conversationId, message = 'You no longer have access to this conversation.') {
    if (messagesUnsubscribe) { messagesUnsubscribe(); messagesUnsubscribe = null; }
    if (conversationDetailsUnsubscribe) { conversationDetailsUnsubscribe(); conversationDetailsUnsubscribe = null; }
    if (typingUnsubscribe) { typingUnsubscribe(); typingUnsubscribe = null; }
    if (activeConversationId === conversationId) {
        activeConversationId = null;
    }
    renderConversationUnavailable(message);
}

async function setTypingState(conversationId, isTyping) {
    if (!conversationId || !currentUser) return;
    if (typingStateByConversation[conversationId] === isTyping) return;
    typingStateByConversation[conversationId] = isTyping;
    try {
        await updateDoc(doc(db, 'conversations', conversationId), { [`typing.${currentUser.uid}`]: isTyping });
    } catch (e) {
        console.warn('Unable to update typing state', e?.message || e);
    }
}

function attachMessageInputHandlers(conversationId) {
    const input = document.getElementById('message-input');
    if (!input) return;
    input.oninput = function () {
        const hasText = (input.value || '').trim().length > 0;
        if (messageTypingTimer) clearTimeout(messageTypingTimer);
        messageTypingTimer = setTimeout(function () {
            setTypingState(conversationId, hasText);
        }, 200);
    };
    input.onblur = function () { setTypingState(conversationId, false); };
}

function listenToConversationDetails(convoId) {
    if (conversationDetailsUnsubscribe) conversationDetailsUnsubscribe();
    const convoRef = doc(db, 'conversations', convoId);
    conversationDetailsUnsubscribe = ListenerRegistry.register(`conversation:details:${convoId}`, onSnapshot(convoRef, function (snap) {
        if (!snap.exists()) { handleConversationAccessLoss(convoId); return; }
        const data = { id: convoId, ...snap.data() };
        if (!(data.participants || []).includes(currentUser?.uid)) { handleConversationAccessLoss(convoId); return; }
        conversationDetailsCache[convoId] = data;
        refreshConversationUsers(data, { updateUI: true });
        renderMessageHeader(data);
        renderMessages(messageThreadCache[convoId] || [], data);
        renderTypingIndicator(data);
    }, function (err) {
        handleSnapshotError('Conversation details', err);
        handleConversationAccessLoss(convoId);
    }));
}

async function markMessagesDelivered(conversationId, msgs = []) {
    if (!conversationId || !currentUser) return;
    const latestOther = msgs.slice().reverse().find(function (msg) { return msg.senderId !== currentUser.uid; });
    if (!latestOther) return;
    const tsMs = getMessageTimestampMs(latestOther);
    if (!tsMs) return;
    if (lastDeliveredAtLocal[conversationId] && lastDeliveredAtLocal[conversationId] >= tsMs) return;
    lastDeliveredAtLocal[conversationId] = tsMs;
    try {
        await updateDoc(doc(db, 'conversations', conversationId), { [`lastDeliveredAt.${currentUser.uid}`]: Timestamp.fromMillis(tsMs) });
    } catch (e) {
        console.warn('Unable to mark delivered', e?.message || e);
    }
}

async function ensureConversation(convoId, participantId) {
    const convoRef = doc(db, 'conversations', convoId);
    const existingSnap = await getDoc(convoRef);
    const participants = [currentUser.uid, participantId].sort();
    const profiles = await Promise.all(participants.map(resolveUserProfile));
    const participantUsernames = profiles.map(function (p) { return p.username || p.name || 'user'; });
    const participantNames = profiles.map(function (p) { return p.displayName || p.name || p.username || 'User'; });
    const participantAvatars = profiles.map(function (p) { return p.photoURL || ''; });

    if (!existingSnap.exists()) {
        const payload = {
            participants,
            participantUsernames,
            participantNames,
            participantAvatars,
            type: 'direct',
            title: null,
            avatarUrl: null,
            createdAt: serverTimestamp(),
            updatedAt: serverTimestamp(),
            lastMessagePreview: '',
            lastMessageSenderId: '',
            lastMessageAt: serverTimestamp(),
            unreadCounts: participants.reduce(function (acc, uid) { acc[uid] = 0; return acc; }, {}),
            mutedBy: [],
            pinnedBy: [],
            creatorId: currentUser.uid
        };
        await setDoc(convoRef, payload, { merge: true });
        conversationDetailsCache[convoId] = { id: convoId, ...payload };
    } else {
        conversationDetailsCache[convoId] = { id: convoId, ...existingSnap.data() };
    }

    const mappingRef = doc(db, `users/${currentUser.uid}/conversations/${convoId}`);
    let existingMapExists = false;
    try {
        const existingMapSnap = await getDoc(mappingRef);
        existingMapExists = existingMapSnap.exists();
    } catch (e) {
        // We must be able to read our own mapping; fail fast if not.
        throw e;
    }

    const participantMeta = participants.reduce(function (acc, uid) {
        acc[uid] = deriveOtherParticipantMeta(participants, uid, conversationDetailsCache[convoId]);
        return acc;
    }, {});

    const currentPayload = {
        conversationId: convoId,
        otherParticipantIds: participantMeta[currentUser.uid].otherIds,
        otherParticipantUsernames: participantMeta[currentUser.uid].usernames,
        otherParticipantAvatars: participantMeta[currentUser.uid].avatars,
        participants,
        createdAt: serverTimestamp()
    };

    if (!existingMapExists) {
        currentPayload.muted = false;
        currentPayload.pinned = false;
        currentPayload.archived = false;
        currentPayload.lastMessagePreview = '';
        currentPayload.lastMessageAt = serverTimestamp();
        currentPayload.unreadCount = 0;
    }

    await setDoc(mappingRef, currentPayload, { merge: true });

    const optimistic = {
        id: convoId,
        ...currentPayload,
        lastMessageAt: currentPayload.lastMessageAt || Timestamp.now(),
        createdAt: currentPayload.createdAt || Timestamp.now(),
        archived: currentPayload.archived,
        muted: currentPayload.muted,
        pinned: currentPayload.pinned,
        unreadCount: currentPayload.unreadCount
    };
    if (!conversationMappings.find(function (m) { return m.id === convoId; })) {
        conversationMappings.push(optimistic);
    } else {
        updateConversationMappingState(convoId, optimistic);
    }
    renderConversationList();

    return conversationDetailsCache[convoId];
}

async function fetchConversation(conversationId) {
    if (conversationDetailsCache[conversationId]) return conversationDetailsCache[conversationId];
    try {
        const snap = await getDoc(doc(db, 'conversations', conversationId));
        if (snap.exists()) {
            conversationDetailsCache[conversationId] = { id: conversationId, ...snap.data() };
            return conversationDetailsCache[conversationId];
        }
    } catch (e) {
        console.warn('Unable to fetch conversation', conversationId, e?.message || e);
        throw e;
    }
    return null;
}

async function listenToMessages(convoId) {
    if (messagesUnsubscribe) messagesUnsubscribe();
    const msgRef = query(collection(db, 'conversations', convoId, 'messages'), orderBy('createdAt'));
    messagesUnsubscribe = ListenerRegistry.register(`messages:thread:${convoId}`, onSnapshot(msgRef, function (snap) {
        const msgs = snap.docs.map(function (d) { return ({ id: d.id, ...d.data() }); });
        messageThreadCache[convoId] = msgs;
        const details = conversationDetailsCache[convoId] || {};
        renderMessages(msgs, details);
        renderTypingIndicator(details);
        markMessagesDelivered(convoId, msgs);
        markConversationAsRead(convoId);
    }, function (err) {
        handleSnapshotError('Messages thread', err);
        handleConversationAccessLoss(convoId);
    }));
}

async function openConversation(conversationId) {
    if (!conversationId || !requireAuth()) return;
    if (activeConversationId && activeConversationId !== conversationId) {
        setTypingState(activeConversationId, false);
    }
    activeConversationId = conversationId;
    if (window.location.pathname.startsWith('/inbox')) {
        const nextPath = buildMessagesUrl({ conversationId });
        if (window.location.pathname + window.location.search !== nextPath) {
            history.pushState({}, '', nextPath);
        }
    }
    clearReplyContext();
    conversationSearchTerm = '';
    const searchInput = document.getElementById('conversation-search');
    if (searchInput) searchInput.value = '';
    const body = document.getElementById('message-thread');
    if (body) body.innerHTML = '';
    const header = document.getElementById('message-header');
    if (header) header.textContent = 'Loading conversation...';
    forceConversationScroll = true;

    let convo = null;
    try {
        convo = await fetchConversation(conversationId);
    } catch (e) {
        handleConversationAccessLoss(conversationId, 'Unable to open this conversation.');
        return;
    }

    if (!convo || !(convo.participants || []).includes(currentUser.uid)) {
        handleConversationAccessLoss(conversationId, 'You no longer have access to this conversation.');
        return;
    }

    if (isMobileViewport()) {
        document.body.classList.add('mobile-thread-open');
    }
    syncMobileMessagesShell();
    refreshInboxLayout();

    refreshConversationUsers(convo, { force: true, updateUI: true });
    renderMessageHeader(convo);
    renderTypingIndicator(convo);
    listenToConversationDetails(conversationId);
    attachMessageInputHandlers(conversationId);
    setTypingState(conversationId, false);
    await listenToMessages(conversationId);
    refreshInboxLayout();
}

async function initConversations(autoOpen = true) {
    if (!requireAuth()) return;
    bindMobileMessageGestures();
    if (conversationsUnsubscribe) conversationsUnsubscribe();
    const convRef = query(collection(db, `users/${currentUser.uid}/conversations`), orderBy('lastMessageAt', 'desc'));
    conversationsUnsubscribe = ListenerRegistry.register('messages:list', onSnapshot(convRef, async function (snap) {
        conversationMappings = snap.docs.map(function (d) { return ({ id: d.id, ...d.data() }); });

        const missingDetails = conversationMappings
            .map(function (m) { return m.id; })
            .filter(function (id) { return !conversationDetailsCache[id]; });

        await Promise.all(missingDetails.map(async function (id) {
            try { await fetchConversation(id); } catch (e) { console.warn('Unable to prefetch conversation', id, e?.message || e); }
        }));

        const userIds = new Set();
        conversationMappings.forEach(function (mapping) {
            const details = conversationDetailsCache[mapping.id] || {};
            (details.participants || mapping.otherParticipantIds || []).forEach(function (uid) {
                const cached = getCachedUser(uid, { allowStale: true });
                if (!cached || isUserCacheStale(cached)) userIds.add(uid);
            });
        });
        await refreshUserProfiles(Array.from(userIds), { force: true });

        await Promise.all(conversationMappings.map(function (mapping) {
            return reconcileConversationMapping(mapping).catch(function () { });
        }));

        renderConversationList();

        if (activeConversationId && !conversationMappings.some(function (m) { return m.id === activeConversationId; })) {
            handleConversationAccessLoss(activeConversationId, 'You no longer have access to this conversation.');
        }

        if (autoOpen && !activeConversationId && conversationMappings.length > 0) {
            openConversation(conversationMappings[0].id);
        }
    }, function (err) {
        handleSnapshotError('Conversation list', err);
    }));
}

async function updateConversationUnread(conversationId, participants = [], previewPayload = {}) {
    const convoRef = doc(db, 'conversations', conversationId);
    const preview = formatMessagePreview(previewPayload);
    const unreadCounts = {};
    const cachedCounts = (conversationDetailsCache[conversationId] || {}).unreadCounts || {};
    participants.forEach(function (uid) {
        unreadCounts[uid] = uid === currentUser.uid ? 0 : (cachedCounts[uid] || 0) + 1;
    });

    await updateDoc(convoRef, {
        lastMessagePreview: preview,
        lastMessageSenderId: currentUser.uid,
        lastMessageAt: serverTimestamp(),
        updatedAt: serverTimestamp(),
        unreadCounts
    });

    conversationDetailsCache[conversationId] = {
        ...(conversationDetailsCache[conversationId] || {}),
        lastMessagePreview: preview,
        lastMessageSenderId: currentUser.uid,
        unreadCounts
    };

    await Promise.all(participants.map(async function (uid) {
        const details = conversationDetailsCache[conversationId] || { participants };
        const meta = deriveOtherParticipantMeta(participants, uid, details);
        const mappingRef = doc(db, `users/${uid}/conversations/${conversationId}`);
        const update = {
            conversationId,
            lastMessagePreview: preview,
            lastMessageAt: serverTimestamp(),
            otherParticipantIds: meta.otherIds,
            otherParticipantUsernames: meta.usernames,
            otherParticipantAvatars: meta.avatars
        };
        if (uid === currentUser.uid) {
            update.unreadCount = 0;
        } else {
            update.unreadCount = increment(1);
        }
        await setDoc(mappingRef, update, { merge: true });
    }));
}

function updateConversationMappingState(conversationId, data = {}) {
    const idx = conversationMappings.findIndex(function (m) { return m.id === conversationId; });
    if (idx >= 0) {
        conversationMappings[idx] = { ...conversationMappings[idx], ...data };
    }
}

function resolveMuteState(conversationId, mapping = {}) {
    const until = toDateSafe(mapping.muteUntil);
    const now = Date.now();
    const expired = mapping.muted && until && until.getTime() <= now;
    const active = !!mapping.muted && !expired && (!until || until.getTime() > now);
    return { active, until, expired };
}

async function clearMuteState(conversationId) {
    if (!currentUser || !conversationId) return;
    try {
        await setDoc(doc(db, `users/${currentUser.uid}/conversations/${conversationId}`), { muted: false, muteUntil: null }, { merge: true });
    } catch (e) {
        console.warn('Unable to clear mute state', e?.message || e);
    }
    updateConversationMappingState(conversationId, { muted: false, muteUntil: null });
}

function renderConversationAvatarPreview(convo = {}, previewUrl = '', filename = '') {
    const preview = document.getElementById('conversation-avatar-preview');
    if (!preview) return;
    const meta = deriveOtherParticipantMeta(convo.participants || [], currentUser?.uid, convo);
    const src = normalizeImageUrl(previewUrl) || getConversationAvatarUrl(convo, meta.avatars?.[0]) || '';
    if (src) {
        preview.innerHTML = `<img src="${src}" alt="Conversation avatar">${filename ? `<div class="participant-hint">${escapeHtml(filename)}</div>` : ''}`;
    } else {
        preview.innerHTML = '<div class="placeholder">No image</div>';
    }
}

function updateConversationNameHelper(convo = {}, fallbackLabel = '') {
    const hint = document.getElementById('conversation-name-hint');
    if (!hint) return;
    hint.textContent = convo.title
        ? 'Custom name visible to all participants.'
        : `If left blank, this chat will display as "${fallbackLabel || 'Conversation'}".`;
}

function bindConversationAvatarInput(convo = {}) {
    const fileInput = document.getElementById('conversation-avatar-input');
    if (!fileInput) return;
    fileInput.onchange = function () {
        const file = fileInput.files?.[0];
        if (!file) {
            renderConversationAvatarPreview(convo);
            return;
        }
        const reader = new FileReader();
        reader.onload = function (e) {
            renderConversationAvatarPreview(convo, e.target?.result || '', file.name);
        };
        reader.readAsDataURL(file);
    };
}

async function reconcileConversationMapping(mapping = {}) {
    if (!mapping?.id || !currentUser) return;
    const details = conversationDetailsCache[mapping.id] || {};
    const participants = details.participants || mapping.otherParticipantIds || [];
    const otherIds = participants.filter(function (uid) { return uid !== currentUser.uid; });
    if (!otherIds.length) return;

    await refreshUserProfiles(otherIds, { force: true });

    const names = otherIds.map(function (uid) {
        const cached = getCachedUser(uid) || {};
        return resolveDisplayName(cached) || cached.username || 'Unknown user';
    });
    const usernames = otherIds.map(function (uid) { return (getCachedUser(uid) || {}).username || 'user'; });
    const avatars = otherIds.map(function (uid) { return (getCachedUser(uid) || {}).photoURL || ''; });
    const colors = otherIds.map(function (uid) {
        const cached = getCachedUser(uid) || {};
        return cached.avatarColor || computeAvatarColor(cached.username || uid || 'user');
    });

    const update = {};
    if (!arrayShallowEqual(mapping.otherParticipantNames || [], names)) update.otherParticipantNames = names;
    if (!arrayShallowEqual(mapping.otherParticipantUsernames || [], usernames)) update.otherParticipantUsernames = usernames;
    if (!arrayShallowEqual(mapping.otherParticipantAvatars || [], avatars)) update.otherParticipantAvatars = avatars;
    if (!arrayShallowEqual(mapping.otherParticipantColors || [], colors)) update.otherParticipantColors = colors;

    if (Object.keys(update).length > 0) {
        await setDoc(doc(db, `users/${currentUser.uid}/conversations/${mapping.id}`), update, { merge: true });
        updateConversationMappingState(mapping.id, update);
    }
}

async function fetchConversationMapping(conversationId) {
    if (!currentUser) return null;
    const snap = await getDoc(doc(db, `users/${currentUser.uid}/conversations/${conversationId}`));
    if (snap.exists()) {
        const mapping = { id: conversationId, ...snap.data() };
        const muteState = resolveMuteState(conversationId, mapping);
        mapping.muted = muteState.active;
        mapping.muteUntil = muteState.active ? mapping.muteUntil || null : null;
        if (muteState.expired) await clearMuteState(conversationId);
        updateConversationMappingState(conversationId, mapping);
        return mapping;
    }
    return null;
}

function refreshConversationUsers(convo = {}, options = {}) {
    const participants = convo.participants || [];
    if (!participants.length) return Promise.resolve([]);
    return refreshUserProfiles(participants, { force: options.force === true }).then(function (profiles) {
        if (convo.id && profiles.length) {
            const participantNames = profiles.map(function (p) { return p.displayName || p.name || p.username || 'User'; });
            const participantUsernames = profiles.map(function (p) { return p.username || p.name || 'user'; });
            const participantAvatars = profiles.map(function (p) { return p.photoURL || ''; });
            conversationDetailsCache[convo.id] = {
                ...(conversationDetailsCache[convo.id] || convo),
                participantNames,
                participantUsernames,
                participantAvatars
            };
        }
        if (window.localStorage?.getItem('NEXERA_DEBUG_PROFILE') === '1') {
            console.debug('[Profiles] conversation users', {
                id: convo.id,
                participants,
                profiles: profiles.map(function (p) { return ({ uid: p.uid || p.id, displayName: p.displayName, username: p.username, photoURL: p.photoURL }); })
            });
        }
        if (options.updateUI) {
            renderConversationList();
            if (activeConversationId === (convo.id || activeConversationId)) {
                renderMessageHeader(convo);
                renderConversationParticipants(convo);
            }
        }
        return profiles;
    });
}

function renderConversationParticipants(convo = {}) {
    const listEl = document.getElementById('conversation-participant-list');
    if (!listEl) return;
    const participants = convo.participants || [];
    listEl.innerHTML = '';

    participants.forEach(function (uid) {
        const meta = resolveParticipantDisplay(convo, uid);
        const profile = getCachedUser(uid) || {};
        const name = resolveDisplayName(profile) || meta.displayName || meta.username || 'Unknown user';
        const handleValue = profile.username || meta.username;
        const handle = handleValue ? `@${handleValue}` : '';
        const badge = renderVerifiedBadge(profile);
        const avatar = renderAvatar({
            ...profile,
            uid,
            username: handleValue || name,
            displayName: name,
            photoURL: profile.photoURL || meta.avatar,
            avatarColor: profile.avatarColor || meta.avatarColor
        }, { size: 32 });
        const row = document.createElement('div');
        row.className = 'participant-row';
        const nameLine = document.createElement('div');
        nameLine.style.display = 'flex';
        nameLine.style.alignItems = 'center';
        nameLine.style.gap = '6px';
        nameLine.style.fontWeight = '700';
        nameLine.innerHTML = `${escapeHtml(name)}${badge}`;

        const labelWrap = document.createElement('div');
        labelWrap.className = 'participant-labels';
        if (uid === currentUser?.uid) {
            const you = document.createElement('span');
            you.className = 'badge';
            you.textContent = 'You';
            labelWrap.appendChild(you);
        }
        if (uid === convo.creatorId) {
            const owner = document.createElement('span');
            owner.className = 'badge';
            owner.textContent = 'Owner';
            labelWrap.appendChild(owner);
        }
        if (labelWrap.childElementCount) nameLine.appendChild(labelWrap);

        const handleLine = document.createElement('div');
        handleLine.className = 'participant-hint';
        handleLine.textContent = handle || '';
        if (!handle) handleLine.style.display = 'none';

        const infoCol = document.createElement('div');
        infoCol.appendChild(nameLine);
        infoCol.appendChild(handleLine);

        const left = document.createElement('div');
        left.style.display = 'flex';
        left.style.alignItems = 'center';
        left.style.gap = '10px';
        const avatarWrapper = document.createElement('div');
        avatarWrapper.innerHTML = avatar;
        if (avatarWrapper.firstChild) left.appendChild(avatarWrapper.firstChild);
        left.appendChild(infoCol);

        row.appendChild(left);

        const isCreator = convo.creatorId && currentUser?.uid === convo.creatorId;
        const canRemove = isCreator && uid !== currentUser?.uid && uid !== convo.creatorId && participants.length > 2;
        if (canRemove) {
            const removeBtn = document.createElement('button');
            removeBtn.className = 'icon-pill';
            removeBtn.innerHTML = '<i class="ph ph-user-minus"></i>';
            removeBtn.onclick = function () { window.removeConversationParticipant(uid); };
            row.appendChild(removeBtn);
        }
        listEl.appendChild(row);
    });

    if (participants.length === 0) {
        listEl.innerHTML = '<div class="empty-state" style="padding:8px 0;">No participants loaded.</div>';
    }
}

function renderConversationSettings(convo = {}, mapping = {}) {
    const titleEl = document.getElementById('conversation-settings-title');
    const subtitleEl = document.getElementById('conversation-settings-subtitle');
    const avatarEl = document.getElementById('conversation-settings-avatar');
    const nameInput = document.getElementById('conversation-name-input');
    const createdByEl = document.getElementById('conversation-created-by');

    const participants = convo.participants || [];
    const meta = deriveOtherParticipantMeta(participants, currentUser?.uid, convo);
    const label = computeConversationTitle(convo, currentUser?.uid) || 'Conversation';

    refreshConversationUsers(convo, { force: true });

    if (titleEl) titleEl.textContent = label;
    if (subtitleEl) subtitleEl.textContent = `${participants.length} participant${participants.length === 1 ? '' : 's'}`;
    if (createdByEl) {
        const creatorMeta = convo.creatorId ? resolveParticipantDisplay(convo, convo.creatorId) : null;
        createdByEl.textContent = convo.creatorId ? `Created by ${creatorMeta.displayName || creatorMeta.username || 'User'}` : '';
    }
    if (avatarEl) {
        avatarEl.innerHTML = renderAvatar({
            uid: convo.id || 'conversation',
            username: label,
            photoURL: getConversationAvatarUrl(convo, meta.avatars?.[0]),
            avatarColor: computeAvatarColor(label)
        }, { size: 48 });
    }

    if (nameInput) {
        nameInput.disabled = false;
        nameInput.value = convo.title || '';
        nameInput.placeholder = label;
        nameInput.maxLength = 80;
    }

    renderConversationParticipants(convo);
    renderConversationAvatarPreview(convo);
    bindConversationAvatarInput(convo);
    updateConversationNameHelper(convo, label);

    const muteBtn = document.getElementById('conversation-mute-btn');
    if (muteBtn) {
        const muteState = resolveMuteState(convo.id || conversationSettingsId, mapping);
        const muted = muteState.active || (convo.mutedBy || []).includes(currentUser?.uid);
        if (muteState.expired) clearMuteState(convo.id || conversationSettingsId);
        muteBtn.innerHTML = `<i class="ph ph-${muted ? 'bell-slash' : 'bell'}"></i> ${muted ? 'Unmute chat' : 'Mute chat'}`;
        if (muted && muteState.until) {
            muteBtn.title = `Muted until ${muteState.until.toLocaleString()}`;
        } else {
            muteBtn.removeAttribute('title');
        }
    }
    const pinBtn = document.getElementById('conversation-pin-btn');
    if (pinBtn) {
        const pinned = !!mapping.pinned;
        pinBtn.innerHTML = `<i class="ph ph-push-pin"></i> ${pinned ? 'Unpin' : 'Pin'}`;
    }
    const archiveBtn = document.getElementById('conversation-archive-btn');
    if (archiveBtn) {
        const archived = !!mapping.archived;
        archiveBtn.innerHTML = `<i class="ph ph-archive"></i> ${archived ? 'Unarchive' : 'Archive'}`;
    }
}

async function refreshConversationSettings(conversationId = conversationSettingsId || activeConversationId) {
    if (!conversationId || !requireAuth()) return;
    const convo = await fetchConversation(conversationId);
    conversationSettingsId = conversationId;
    const mapping = await fetchConversationMapping(conversationId) || {};
    if (convo) {
        await refreshConversationUsers(convo, { force: true });
        renderConversationSettings(convo, mapping);
    }
}

window.refreshConversationSettings = refreshConversationSettings;

window.openConversationSettings = async function (conversationId = activeConversationId) {
    if (!conversationId || !requireAuth()) return;
    conversationSettingsId = conversationId;
    await refreshConversationSettings(conversationId);
    window.navigateTo('conversation-settings');
};

window.backToConversationFromSettings = function () {
    window.navigateTo('messages');
    if (conversationSettingsId) {
        activeConversationId = conversationSettingsId;
        openConversation(conversationSettingsId);
    }
};

window.searchConversationParticipants = async function (term = '') {
    const resultsEl = document.getElementById('conversation-add-results');
    if (!resultsEl) return;
    const cleaned = term.trim().toLowerCase();
    if (cleaned.length < 2) {
        conversationSettingsSearchResults = [];
        resultsEl.innerHTML = '';
        return;
    }
    const qSnap = await getDocs(query(collection(db, 'users'), where('username', '>=', cleaned), where('username', '<=', cleaned + '~'), limit(15)));
    const existing = (conversationDetailsCache[conversationSettingsId] || {}).participants || [];
    const deduped = new Map();
    qSnap.forEach(function (docSnap) {
        if (docSnap.id === currentUser?.uid) return;
        if (existing.includes(docSnap.id)) return;
        const profile = normalizeUserProfileData(docSnap.data(), docSnap.id);
        storeUserInCache(docSnap.id, profile);
        deduped.set(docSnap.id, { id: docSnap.id, ...profile });
    });
    conversationSettingsSearchResults = Array.from(deduped.values());
    resultsEl.innerHTML = '';
    conversationSettingsSearchResults.forEach(function (user) {
        const row = document.createElement('div');
        row.className = 'conversation-item';
        const avatar = renderAvatar({ uid: user.id, username: user.username || user.displayName || 'user', displayName: user.displayName, photoURL: user.photoURL, avatarColor: user.avatarColor }, { size: 32 });
        row.innerHTML = `<div style="display:flex; align-items:center; gap:10px;">${avatar}<div><strong>@${escapeHtml(user.username || 'user')}</strong><div style="color:var(--text-muted); font-size:0.85rem;">${escapeHtml(user.displayName || 'Nexera User')}</div></div></div>`;
        row.onclick = function () { window.addParticipantToConversation(user.id); };
        resultsEl.appendChild(row);
    });
};

async function updateConversationParticipants(conversationId, updatedParticipants = []) {
    if (!requireAuth()) return;
    const unique = Array.from(new Set(updatedParticipants)).sort();
    if (unique.length < 2) {
        toast('A conversation needs at least 2 participants.', 'error');
        return;
    }

    const profiles = await Promise.all(unique.map(resolveUserProfile));
    const participantUsernames = profiles.map(function (p) { return p.username || p.name || 'user'; });
    const participantNames = profiles.map(function (p) { return p.displayName || p.name || p.username || 'User'; });
    const participantAvatars = profiles.map(function (p) { return p.photoURL || ''; });

    const existing = conversationDetailsCache[conversationId] || (await fetchConversation(conversationId)) || {};
    const unreadCounts = { ...(existing.unreadCounts || {}) };
    unique.forEach(function (uid) { if (!(uid in unreadCounts)) unreadCounts[uid] = 0; });
    Object.keys(unreadCounts).forEach(function (uid) { if (!unique.includes(uid)) delete unreadCounts[uid]; });
    const mutedBy = (existing.mutedBy || []).filter(function (uid) { return unique.includes(uid); });
    const pinnedBy = (existing.pinnedBy || []).filter(function (uid) { return unique.includes(uid); });

    const convoRef = doc(db, 'conversations', conversationId);
    const convoType = unique.length > 2 ? 'group' : 'direct';
    await updateDoc(convoRef, {
        participants: unique,
        participantUsernames,
        participantNames,
        participantAvatars,
        unreadCounts,
        mutedBy,
        pinnedBy,
        type: convoType,
        title: existing.title || null,
        updatedAt: serverTimestamp()
    });

    const removed = (existing.participants || []).filter(function (uid) { return !unique.includes(uid); });
    await Promise.all(removed.map(async function (uid) {
        try { await deleteDoc(doc(db, `users/${uid}/conversations/${conversationId}`)); } catch (e) { console.warn('Remove mapping failed', e?.message || e); }
    }));

    conversationDetailsCache[conversationId] = { ...existing, id: conversationId, participants: unique, participantUsernames, participantNames, participantAvatars, unreadCounts, mutedBy, pinnedBy, type: convoType, title: existing.title || null };
    await Promise.all(unique.map(async function (uid) {
        const meta = deriveOtherParticipantMeta(unique, uid, conversationDetailsCache[conversationId]);
        const mappingRef = doc(db, `users/${uid}/conversations/${conversationId}`);
        await setDoc(mappingRef, {
            conversationId,
            otherParticipantIds: meta.otherIds,
            otherParticipantUsernames: meta.usernames,
            otherParticipantAvatars: meta.avatars,
            archived: false
        }, { merge: true });
    }));

    await refreshConversationSettings(conversationId);
    if (activeConversationId === conversationId) renderMessageHeader(conversationDetailsCache[conversationId]);
}

window.addParticipantToConversation = async function (uid) {
    if (!conversationSettingsId) return;
    const convo = conversationDetailsCache[conversationSettingsId] || (await fetchConversation(conversationSettingsId));
    const participants = Array.from(new Set([...(convo?.participants || []), uid]));
    await updateConversationParticipants(conversationSettingsId, participants);
};

window.removeConversationParticipant = async function (uid) {
    if (!conversationSettingsId) return;
    const convo = conversationDetailsCache[conversationSettingsId] || (await fetchConversation(conversationSettingsId));
    const participants = (convo?.participants || []).filter(function (p) { return p !== uid; });
    const target = resolveParticipantDisplay(convo, uid);
    await openConfirmModal({
        title: 'Remove participant?',
        message: `Remove ${target.displayName || target.username || 'this participant'} from the conversation?`,
        helperText: 'They will lose access to future messages.',
        confirmText: 'Remove',
        onConfirm: async function () {
            await updateConversationParticipants(conversationSettingsId, participants);
        }
    });
};

window.saveConversationName = async function () {
    if (!conversationSettingsId || !requireAuth()) return;
    const convo = conversationDetailsCache[conversationSettingsId] || (await fetchConversation(conversationSettingsId));
    if (!convo) return;
    const nameInput = document.getElementById('conversation-name-input');
    const title = (nameInput?.value || '').trim();
    if (title.length > 80) { toast('Conversation name must be 80 characters or fewer.', 'error'); return; }
    const nextTitle = title || null;
    const fallback = computeConversationTitle(convo, currentUser?.uid) || 'Conversation';
    await openConfirmModal({
        title: 'Save conversation name',
        message: title ? `Save conversation as "${title}"?` : 'Clear the custom name and use the default participant title?',
        helperText: title ? 'All participants will see this name.' : `It will revert to "${fallback}" until renamed.`,
        confirmText: 'Save',
        onConfirm: async function () {
            await updateDoc(doc(db, 'conversations', conversationSettingsId), { title: nextTitle, updatedAt: serverTimestamp() });
            conversationDetailsCache[conversationSettingsId] = { ...convo, title: nextTitle };
            if (activeConversationId === conversationSettingsId) renderMessageHeader(conversationDetailsCache[conversationSettingsId]);
            toast('Conversation name updated', 'info');
        }
    });
};

window.uploadConversationAvatar = async function () {
    if (!conversationSettingsId || !requireAuth()) return;
    const fileInput = document.getElementById('conversation-avatar-input');
    if (!fileInput?.files?.length) { toast('Select an image first.', 'error'); return; }
    const file = fileInput.files[0];
    const path = `conversation_avatars/${conversationSettingsId}/${Date.now()}_${file.name}`;
    await openConfirmModal({
        title: 'Replace conversation image?',
        message: `Upload ${file.name}?`,
        helperText: 'This updates the chat image for all participants.',
        confirmText: 'Upload',
        onConfirm: async function () {
            try {
                const uploadRef = ref(storage, path);
                const snap = await uploadBytes(uploadRef, file);
                const url = await getDownloadURL(snap.ref);
                await updateDoc(doc(db, 'conversations', conversationSettingsId), {
                    avatarUrl: url,
                    avatarURL: deleteField(),
                    updatedAt: serverTimestamp()
                });
                conversationDetailsCache[conversationSettingsId] = {
                    ...(conversationDetailsCache[conversationSettingsId] || {}),
                    avatarUrl: url
                };
                if (activeConversationId === conversationSettingsId) renderMessageHeader(conversationDetailsCache[conversationSettingsId]);
                await refreshConversationSettings(conversationSettingsId);
                toast('Conversation image updated', 'info');
            } catch (error) {
                console.warn('Conversation avatar upload failed', error);
                const message = error?.code === 'storage/unauthorized'
                    ? 'You do not have permission to update this conversation image.'
                    : 'Unable to upload conversation image right now.';
                toast(message, 'error');
            }
        }
    });
};

window.toggleConversationMute = async function () {
    if (!conversationSettingsId || !requireAuth()) return;
    const mapping = await fetchConversationMapping(conversationSettingsId) || {};
    const muteState = resolveMuteState(conversationSettingsId, mapping);
    const mappingRef = doc(db, `users/${currentUser.uid}/conversations/${conversationSettingsId}`);
    const convoRef = doc(db, 'conversations', conversationSettingsId);
    const isMuted = muteState.active || (conversationDetailsCache[conversationSettingsId]?.mutedBy || []).includes(currentUser.uid);

    if (isMuted) {
        await openConfirmModal({
            title: 'Unmute chat?',
            message: 'Resume notifications for this conversation?',
            confirmText: 'Unmute',
            onConfirm: async function () {
                await setDoc(mappingRef, { muted: false, muteUntil: null }, { merge: true });
                await updateDoc(convoRef, { mutedBy: arrayRemove(currentUser.uid) });
                updateConversationMappingState(conversationSettingsId, { muted: false, muteUntil: null });
                await refreshConversationSettings(conversationSettingsId);
            }
        });
        return;
    }

    const muteOptions = [
        { label: '15 minutes', duration: 15 * 60 * 1000 },
        { label: '1 hour', duration: 60 * 60 * 1000 },
        { label: '8 hours', duration: 8 * 60 * 60 * 1000 },
        { label: '1 day', duration: 24 * 60 * 60 * 1000 },
        { label: '7 days', duration: 7 * 24 * 60 * 60 * 1000 },
        { label: 'Permanently', duration: null }
    ];
    await openConfirmModal({
        title: 'Mute chat',
        message: 'Choose how long to mute notifications.',
        helperText: 'Mute applies only to you.',
        confirmText: 'Mute',
        buildContent: function (container) {
            let selected = muteOptions[1];
            const group = document.createElement('div');
            group.className = 'confirm-options';
            muteOptions.forEach(function (opt, idx) {
                const row = document.createElement('label');
                row.className = 'confirm-option';
                row.innerHTML = `<input type="radio" name="mute-duration" value="${idx}" ${idx === 1 ? 'checked' : ''}> <span>${opt.label}</span>`;
                row.querySelector('input').onchange = function () { selected = opt; };
                group.appendChild(row);
            });
            container.appendChild(group);
            return function () { return { option: selected }; };
        },
        onConfirm: async function (data) {
            const option = data?.option || muteOptions[1];
            const untilDate = option.duration ? Timestamp.fromMillis(Date.now() + option.duration) : null;
            await setDoc(mappingRef, { muted: true, muteUntil: untilDate }, { merge: true });
            await updateDoc(convoRef, { mutedBy: arrayUnion(currentUser.uid) });
            updateConversationMappingState(conversationSettingsId, { muted: true, muteUntil: untilDate });
            await refreshConversationSettings(conversationSettingsId);
        }
    });
};

window.toggleConversationPin = async function () {
    if (!conversationSettingsId || !requireAuth()) return;
    const mapping = await fetchConversationMapping(conversationSettingsId) || {};
    const pinned = !!mapping.pinned;
    const mappingRef = doc(db, `users/${currentUser.uid}/conversations/${conversationSettingsId}`);
    await openConfirmModal({
        title: pinned ? 'Unpin conversation?' : 'Pin conversation?',
        message: pinned ? 'Remove this chat from your pinned list?' : 'Keep this chat at the top of your list?',
        confirmText: pinned ? 'Unpin' : 'Pin',
        onConfirm: async function () {
            await setDoc(mappingRef, { pinned: !pinned }, { merge: true });
            updateConversationMappingState(conversationSettingsId, { pinned: !pinned });
            await refreshConversationSettings(conversationSettingsId);
        }
    });
};

window.toggleConversationArchive = async function () {
    if (!conversationSettingsId || !requireAuth()) return;
    const mapping = await fetchConversationMapping(conversationSettingsId) || {};
    const archived = !!mapping.archived;
    const mappingRef = doc(db, `users/${currentUser.uid}/conversations/${conversationSettingsId}`);
    await openConfirmModal({
        title: archived ? 'Unarchive chat?' : 'Archive chat?',
        message: archived ? 'Return this chat to your main list?' : 'Move this chat out of your main list?',
        confirmText: archived ? 'Unarchive' : 'Archive',
        onConfirm: async function () {
            await setDoc(mappingRef, { archived: !archived }, { merge: true });
            updateConversationMappingState(conversationSettingsId, { archived: !archived });
            await refreshConversationSettings(conversationSettingsId);
            if (!archived) toast('Conversation archived', 'info');
        }
    });
};

async function deleteConversationMessages(conversationId) {
    const msgs = await getDocs(collection(db, 'conversations', conversationId, 'messages'));
    await Promise.all(msgs.docs.map(function (d) { return deleteDoc(d.ref).catch(function (e) { console.warn('Delete message failed', e?.message || e); }); }));
}

window.leaveConversation = async function () {
    if (!conversationSettingsId || !requireAuth()) return;
    const convo = conversationDetailsCache[conversationSettingsId] || (await fetchConversation(conversationSettingsId));
    if (!convo) return;
    const participants = convo.participants || [];
    const isDirect = participants.length <= 2;
    const title = computeConversationTitle(convo, currentUser?.uid) || 'Conversation';
    const helper = isDirect
        ? 'Leaving deletes this direct chat and its history for both participants.'
        : 'You will be removed from the group and stop receiving messages.';

    await openConfirmModal({
        title: 'Leave chat?',
        message: `Leave "${title}"?`,
        helperText: helper,
        confirmText: 'Leave chat',
        onConfirm: async function () {
            if (isDirect) {
                await deleteConversationMessages(conversationSettingsId);
                await Promise.all(participants.map(async function (uid) {
                    try { await deleteDoc(doc(db, `users/${uid}/conversations/${conversationSettingsId}`)); } catch (e) { console.warn('Mapping cleanup', e?.message || e); }
                }));
                await deleteDoc(doc(db, 'conversations', conversationSettingsId));
                delete conversationDetailsCache[conversationSettingsId];
            } else {
                const updated = participants.filter(function (uid) { return uid !== currentUser.uid; });
                await updateConversationParticipants(conversationSettingsId, updated);
                try { await deleteDoc(doc(db, `users/${currentUser.uid}/conversations/${conversationSettingsId}`)); } catch (e) { console.warn('Self mapping cleanup', e?.message || e); }
            }

            activeConversationId = null;
            conversationSettingsId = null;
            window.navigateTo('messages');
            await initConversations();
        }
    });
};

window.blockConversationPartner = async function () {
    if (!conversationSettingsId || !requireAuth()) return;
    const convo = conversationDetailsCache[conversationSettingsId] || (await fetchConversation(conversationSettingsId));
    if (!convo || (convo.participants || []).length !== 2) { toast('Blocking is available for direct chats only.', 'error'); return; }
    const otherId = (convo.participants || []).find(function (uid) { return uid !== currentUser.uid; });
    const otherMeta = resolveParticipantDisplay(convo, otherId);
    await openConfirmModal({
        title: 'Block user?',
        message: `Block ${otherMeta.displayName || otherMeta.username || 'this user'}?`,
        helperText: 'They will not be able to message you and this chat may be hidden.',
        confirmText: 'Block',
        onConfirm: async function () {
            await setDoc(doc(db, 'users', currentUser.uid), { blockedUserIds: arrayUnion(otherId) }, { merge: true });
            const blocked = new Set(userProfile.blockedUserIds || []);
            blocked.add(otherId);
            userProfile.blockedUserIds = Array.from(blocked);
            toast('User blocked', 'info');
        }
    });
};

window.reportConversation = async function () {
    if (!conversationSettingsId || !requireAuth()) return;
    const convo = conversationDetailsCache[conversationSettingsId] || (await fetchConversation(conversationSettingsId));
    if (!convo) return;
    const isGroup = (convo.participants || []).length > 2;
    const reportedUserId = isGroup ? null : (convo.participants || []).find(function (uid) { return uid !== currentUser.uid; });
    const title = computeConversationTitle(convo, currentUser?.uid) || 'Conversation';
    await openConfirmModal({
        title: 'Report conversation?',
        message: `Submit a report for "${title}"?`,
        helperText: 'Provide a brief reason so our team can review.',
        confirmText: 'Submit report',
        buildContent: function (container) {
            const input = document.createElement('textarea');
            input.className = 'form-input';
            input.rows = 3;
            input.placeholder = 'Reason (optional)';
            container.appendChild(input);
            return function () { return { reason: input.value.trim() }; };
        },
        onConfirm: async function (data) {
            await addDoc(collection(db, 'reports'), {
                type: isGroup ? 'conversation' : 'user',
                conversationId: conversationSettingsId,
                reportedUserId: reportedUserId || null,
                reporterUserId: currentUser.uid,
                createdAt: serverTimestamp(),
                reason: data?.reason || 'Conversation settings report'
            });
            toast('Report submitted', 'info');
        }
    });
};

async function sendChatPayload(conversationId, payload = {}) {
    if (!conversationId || !requireAuth()) return;
    const messageId = payload.messageId || null;
    const convo = await fetchConversation(conversationId) || conversationDetailsCache[conversationId];
    const participants = (convo && convo.participants) || [];
    const blocked = new Set(userProfile.blockedUserIds || []);
    if (convo && convo.type === 'direct') {
        const otherId = participants.find(function (uid) { return uid !== currentUser.uid; });
        if (otherId && blocked.has(otherId)) { toast('You have blocked this user.', 'error'); return; }
    }

    const replyContext = payload.replyContext || (activeReplyContext && activeReplyContext.conversationId === conversationId ? activeReplyContext : null);
    const attachments = Array.isArray(payload.attachments) ? payload.attachments.filter(Boolean) : [];
    const hasVideoAttachment = attachments.some(function (att) { return (att.type || '').includes('video'); });
    const primaryImage = (!payload.mediaURL && attachments.length === 1 && (attachments[0].type || '').includes('image')) ? attachments[0] : null;

    const message = {
        senderId: currentUser.uid,
        senderUsername: userProfile.username || currentUser.displayName || 'Nexera user',
        text: payload.text || '',
        type: payload.type || (attachments.length ? (hasVideoAttachment ? 'video' : 'image') : 'text'),
        mediaURL: payload.mediaURL || (primaryImage ? primaryImage.url : null),
        mediaPath: payload.mediaPath || (primaryImage ? primaryImage.storagePath : null),
        mediaType: payload.mediaType || (primaryImage ? primaryImage.type : null),
        attachments: attachments.length ? attachments : null,
        postId: payload.postId || null,
        threadId: payload.threadId || null,
        createdAt: serverTimestamp(),
        editedAt: null,
        deletedAt: null,
        replyToMessageId: replyContext && replyContext.mode !== 'forward' ? (payload.replyToMessageId || replyContext.targetMessageId || null) : payload.replyToMessageId || null,
        replyToSenderId: replyContext && replyContext.mode !== 'forward' ? (payload.replyToSenderId || replyContext.senderId || null) : payload.replyToSenderId || null,
        replyToSnippet: replyContext && replyContext.mode !== 'forward' ? ((payload.replyToSnippet || replyContext.snippet || '').slice(0, 200)) : payload.replyToSnippet || null,
        forwardedFromMessageId: replyContext && replyContext.mode === 'forward' ? (payload.forwardedFromMessageId || replyContext.targetMessageId || null) : payload.forwardedFromMessageId || null,
        forwardedFromConversationId: replyContext && replyContext.mode === 'forward' ? (payload.forwardedFromConversationId || conversationId || null) : payload.forwardedFromConversationId || null,
        forwardedFromSenderId: replyContext && replyContext.mode === 'forward' ? (payload.forwardedFromSenderId || replyContext.senderId || null) : payload.forwardedFromSenderId || null,
        forwardedAt: replyContext && replyContext.mode === 'forward' ? serverTimestamp() : payload.forwardedAt || null,
        reactions: payload.reactions || {},
        readBy: [currentUser.uid],
        reported: false,
        reportCount: 0,
        systemPayload: payload.systemPayload || null,
        status: payload.status || 'sent'
    };
    if (messageId) {
        await setDoc(doc(collection(db, 'conversations', conversationId, 'messages'), messageId), message, { merge: false });
    } else {
        await addDoc(collection(db, 'conversations', conversationId, 'messages'), message);
    }
}

function isRetryableUploadError(error) {
    if (!error) return false;
    if (isPermissionDeniedError(error)) return false;
    if (error?.code === 'storage/unauthorized' || error?.code === 'storage/unauthenticated') return false;
    if (error?.code === 'storage/invalid-argument') return false;
    return true;
}

function waitFor(ms) {
    return new Promise(function (resolve) { setTimeout(resolve, ms); });
}

async function uploadAttachmentWithRetry(conversationId, messageId, file, index, onProgress) {
    const maxRetries = 2;
    let attempt = 0;
    while (attempt <= maxRetries) {
        try {
            const stamp = Date.now();
            const safeName = sanitizeFileName(file.name || `attachment_${index}`);
            const storagePath = buildChatMediaPath({
                conversationId,
                messageId,
                timestamp: stamp,
                filename: safeName
            });
            const storageRef = ref(storage, storagePath);
            const uploadTask = uploadBytesResumable(storageRef, file, {
                contentType: file.type || 'application/octet-stream'
            });
            await new Promise(function (resolve, reject) {
                uploadTask.on('state_changed', function (snapshot) {
                    const progress = snapshot.totalBytes
                        ? Math.round((snapshot.bytesTransferred / snapshot.totalBytes) * 100)
                        : 0;
                    if (typeof onProgress === 'function') onProgress(progress);
                }, reject, resolve);
            });
            const urlResult = await guardFirebaseCall('storage:dm_upload_url', function () {
                return getDownloadURL(uploadTask.snapshot.ref);
            }, {
                onPermissionDenied: function () {
                    toast(getDmMediaFallbackText('denied'), 'error');
                }
            });
            if (!urlResult.ok || !urlResult.data) {
                throw urlResult.error || new Error('Unable to fetch attachment URL.');
            }
            return {
                url: urlResult.data,
                storagePath: uploadTask.snapshot.ref.fullPath,
                type: file.type || '',
                name: file.name || 'Attachment',
                size: file.size || 0
            };
        } catch (err) {
            if (!isRetryableUploadError(err) || attempt === maxRetries) {
                throw err;
            }
            const backoff = 500 * Math.pow(2, attempt);
            await waitFor(backoff);
            attempt += 1;
        }
    }
    throw new Error('Upload retries exhausted.');
}

async function uploadAttachments(conversationId, files = [], options = {}) {
    const uploads = [];
    const filteredFiles = filterDmAttachments(files);
    const messageId = options.messageId;
    if (!filteredFiles.length) return uploads;
    if (!messageId) throw new Error('Missing message ID for attachment upload.');
    const progressByIndex = new Map();
    const reportProgress = function () {
        if (typeof options.onProgress !== 'function') return;
        const values = Array.from(progressByIndex.values());
        const total = values.length ? Math.round(values.reduce(function (sum, value) { return sum + value; }, 0) / values.length) : 0;
        options.onProgress(total);
    };
    let idx = 0;
    for (const file of filteredFiles) {
        if (!file) continue;
        try {
            const uploadResult = await uploadAttachmentWithRetry(conversationId, messageId, file, idx, function (progress) {
                progressByIndex.set(idx, progress);
                reportProgress();
            });
            uploads.push({
                url: uploadResult.url,
                storagePath: uploadResult.storagePath,
                type: uploadResult.type,
                name: uploadResult.name,
                size: uploadResult.size
            });
        } catch (err) {
            console.warn('Attachment upload failed', err?.code || err?.message || err);
            toast('Attachment upload failed. Please try again.', 'error');
            throw err;
        }
        idx += 1;
    }
    return uploads;
}

window.sendMessage = async function (conversationId = activeConversationId) {
    if (!conversationId || !requireAuth()) return;
    if (messageUploadState.status === 'uploading') return;
    const input = document.getElementById('message-input');
    const text = (input?.value || '').trim();
    const fileInput = document.getElementById('message-media');
    const directFiles = Array.from(fileInput?.files || []);
    const combinedFiles = filterDmAttachments(pendingMessageAttachments.concat(directFiles));
    if (!text && !combinedFiles.length) return;

    if (editingMessageId && !combinedFiles.length) {
        try {
            await updateDoc(doc(db, 'conversations', conversationId, 'messages', editingMessageId), { text, editedAt: serverTimestamp() });
        } catch (e) { console.warn('Edit failed', e?.message || e); toast('Unable to edit message', 'error'); }
        editingMessageId = null;
    } else {
        let attachments = [];
        const messageId = doc(collection(db, 'conversations', conversationId, 'messages')).id;
        if (combinedFiles.length) {
            setMessageUploadState({
                status: 'uploading',
                progress: 0,
                error: null,
                conversationId,
                messageId,
                files: combinedFiles.slice()
            });
            try {
                attachments = await uploadAttachments(conversationId, combinedFiles, {
                    messageId,
                    onProgress: function (progress) {
                        setMessageUploadState({ progress });
                    }
                });
            } catch (err) {
                setMessageUploadState({
                    status: 'error',
                    error: 'Upload failed. Please retry.'
                });
                return;
            }
        }
        try {
            await sendChatPayload(conversationId, {
                text,
                attachments,
                mediaURL: attachments.length === 1 && (attachments[0].type || '').includes('image')
                    ? (attachments[0].url || null)
                    : null,
                mediaPath: attachments.length === 1 ? (attachments[0].storagePath || null) : null,
                mediaType: attachments.length === 1 ? attachments[0].type : null,
                type: attachments.length ? (attachments.some(function (att) { return (att.type || '').includes('video'); }) ? 'video' : 'image') : 'text',
                messageId
            });
        } catch (err) {
            console.warn('Message send failed', err?.code || err?.message || err);
            toast('Message send failed. Please retry.', 'error');
            setMessageUploadState({
                status: 'error',
                error: 'Message send failed. Please retry.',
                conversationId,
                messageId,
                files: combinedFiles.slice()
            });
            return;
        }
    }
    if (input) input.value = '';
    clearAttachmentPreview();
    if (fileInput) fileInput.value = '';
    setTypingState(conversationId, false);
    clearReplyContext();
};

window.sendMediaMessage = async function (conversationId = activeConversationId, fileInputElementId = 'message-media', caption = '') {
    if (!conversationId || !requireAuth()) return;
    const fileInput = document.getElementById(fileInputElementId);
    if (!fileInput || !fileInput.files || !fileInput.files.length) return;
    const files = filterDmAttachments(Array.from(fileInput.files));
    if (!files.length) return;
    const messageId = doc(collection(db, 'conversations', conversationId, 'messages')).id;
    setMessageUploadState({
        status: 'uploading',
        progress: 0,
        error: null,
        conversationId,
        messageId,
        files: files.slice()
    });
    let uploads = [];
    try {
        uploads = await uploadAttachments(conversationId, files, {
            messageId,
            onProgress: function (progress) {
                setMessageUploadState({ progress });
            }
        });
    } catch (err) {
        setMessageUploadState({
            status: 'error',
            error: 'Upload failed. Please retry.'
        });
        return;
    }
    const hasVideo = uploads.some(function (att) { return (att.type || '').includes('video'); });
    try {
        await sendChatPayload(conversationId, {
            text: caption,
            attachments: uploads,
            mediaURL: uploads.length === 1 && (uploads[0].type || '').includes('image')
                ? (uploads[0].url || null)
                : null,
            mediaPath: uploads.length === 1 ? (uploads[0].storagePath || null) : null,
            mediaType: uploads.length === 1 ? uploads[0].type : null,
            type: uploads.length ? (hasVideo ? 'video' : 'image') : 'text',
            messageId
        });
    } catch (err) {
        console.warn('Message send failed', err?.code || err?.message || err);
        toast('Message send failed. Please retry.', 'error');
        setMessageUploadState({
            status: 'error',
            error: 'Message send failed. Please retry.',
            conversationId,
            messageId,
            files: files.slice()
        });
        return;
    }
    fileInput.value = '';
    clearAttachmentPreview();
    const input = document.getElementById('message-input');
    if (input && caption) input.value = '';
    setTypingState(conversationId, false);
};

window.handleConversationSearch = handleConversationSearch;
window.handleConversationListSearch = handleConversationListSearch;
window.setConversationFilter = setConversationFilter;
window.navigateConversationSearch = navigateConversationSearch;
window.clearReplyContext = clearReplyContext;

window.markConversationAsRead = async function (conversationId = activeConversationId) {
    if (!conversationId || !currentUser) return;
    const messages = messageThreadCache[conversationId] || [];
    const latestMessage = messages[messages.length - 1];
    const latestTs = latestMessage ? getMessageTimestampMs(latestMessage) : Date.now();
    const receiptTs = Timestamp.fromMillis(latestTs || Date.now());
    lastReadAtLocal[conversationId] = latestTs;
    lastDeliveredAtLocal[conversationId] = Math.max(lastDeliveredAtLocal[conversationId] || 0, latestTs);
    try {
        await updateDoc(doc(db, 'conversations', conversationId), {
            [`unreadCounts.${currentUser.uid}`]: 0,
            [`lastReadAt.${currentUser.uid}`]: receiptTs,
            [`lastDeliveredAt.${currentUser.uid}`]: receiptTs
        });
    } catch (e) {
        console.warn('Unable to update unread counts', e);
    }
    try {
        await setDoc(doc(db, `users/${currentUser.uid}/conversations/${conversationId}`), { unreadCount: 0 }, { merge: true });
    } catch (e) {
        console.warn('Unable to update mapping unread', e);
    }
};

function updateChatStartControls() {
    const startBtn = document.getElementById('start-chat-btn');
    const groupNameRow = document.getElementById('chat-group-name-row');
    if (startBtn) {
        startBtn.disabled = newChatSelections.length === 0;
        startBtn.textContent = newChatSelections.length > 1 ? 'Create group chat' : 'Start chat';
    }
    if (groupNameRow) {
        groupNameRow.style.display = newChatSelections.length > 1 ? 'block' : 'none';
        if (newChatSelections.length <= 1) {
            const nameInput = document.getElementById('chat-group-name');
            if (nameInput) nameInput.value = '';
        }
    }
}

function renderSelectedUsers() {
    const listEl = document.getElementById('selected-chat-users');
    if (!listEl) return;
    listEl.innerHTML = '';
    if (newChatSelections.length === 0) {
        listEl.innerHTML = '<div class="empty-state" style="padding:8px 0;">Select users to start a conversation.</div>';
        updateChatStartControls();
        return;
    }

    newChatSelections.forEach(function (user) {
        const row = document.createElement('div');
        row.className = 'conversation-item';
        row.style.alignItems = 'center';
        const avatar = renderAvatar({ uid: user.id, username: user.username || user.displayName || 'user', photoURL: user.photoURL || '' }, { size: 32 });
        row.innerHTML = `<div style="display:flex; align-items:center; gap:8px;">${avatar}<div><div style="font-weight:700;">${escapeHtml(user.displayName || user.name || user.username || 'User')}</div><div style="color:var(--text-muted); font-size:0.85rem;">@${escapeHtml(user.username || 'user')}</div></div></div><button class="icon-pill" style="padding:6px 10px;" onclick="window.removeSelectedChatUser('${user.id}')"><i class="ph ph-x"></i></button>`;
        listEl.appendChild(row);
    });
    updateChatStartControls();
}

function renderChatSearchResults(users = []) {
    const resultsEl = document.getElementById('chat-search-results');
    if (!resultsEl) return;
    resultsEl.innerHTML = '';
    users.forEach(function (user) {
        const row = document.createElement('div');
        row.className = 'conversation-item' + (newChatSelections.some(function (u) { return u.id === user.id; }) ? ' active' : '');
        const avatar = renderAvatar({ uid: user.id, username: user.username || user.displayName || 'user', displayName: user.displayName, photoURL: user.photoURL, avatarColor: user.avatarColor }, { size: 32 });
        row.innerHTML = `<div style="display:flex; align-items:center; gap:10px;">${avatar}<div><strong>@${escapeHtml(user.username || 'user')}</strong><div style="color:var(--text-muted); font-size:0.85rem;">${escapeHtml(user.displayName || user.name || 'Nexera User')}</div></div></div>`;
        row.onclick = function () { window.toggleChatUserSelection(user.id); };
        resultsEl.appendChild(row);
    });
}

window.removeSelectedChatUser = function (userId) {
    newChatSelections = newChatSelections.filter(function (u) { return u.id !== userId; });
    renderSelectedUsers();
    renderChatSearchResults(chatSearchResults);
};

window.toggleChatUserSelection = function (userId) {
    const existing = newChatSelections.find(function (u) { return u.id === userId; });
    if (existing) {
        window.removeSelectedChatUser(userId);
        return;
    }
    const found = chatSearchResults.find(function (u) { return u.id === userId; });
    if (found) {
        newChatSelections.push(found);
        renderSelectedUsers();
        renderChatSearchResults(chatSearchResults);
    }
};

function resetNewChatModalState() {
    newChatSelections = [];
    chatSearchResults = [];
    const searchInput = document.getElementById('chat-search');
    if (searchInput) searchInput.value = '';
    const resultsEl = document.getElementById('chat-search-results');
    if (resultsEl) resultsEl.innerHTML = '';
    renderSelectedUsers();
    updateChatStartControls();
}

window.toggleNewChatModal = function (show = true) {
    const modal = document.getElementById('new-chat-modal');
    if (show) resetNewChatModalState();
    if (modal) modal.style.display = show ? 'flex' : 'none';
};
window.openNewChatModal = function () { if (!requireAuth()) return; return window.toggleNewChatModal(true); };

window.searchChatUsers = async function (term = '') {
    const resultsEl = document.getElementById('chat-search-results');
    if (!resultsEl) return;
    const cleaned = term.trim().toLowerCase();
    if (cleaned.length < 2) {
        chatSearchResults = [];
        renderChatSearchResults(chatSearchResults);
        return;
    }
    const qSnap = await getDocs(query(collection(db, 'users'), where('username', '>=', cleaned), where('username', '<=', cleaned + '~')));
    const deduped = new Map();
    qSnap.forEach(function (docSnap) {
        const data = docSnap.data();
        if (docSnap.id === currentUser?.uid) return;
        const profile = normalizeUserProfileData(data, docSnap.id);
        storeUserInCache(docSnap.id, profile);
        deduped.set(docSnap.id, { id: docSnap.id, ...profile });
    });
    chatSearchResults = Array.from(deduped.values());
    renderChatSearchResults(chatSearchResults);
};

async function createGroupConversation(participantIds = [], title = null) {
    if (!requireAuth()) return null;
    const participants = Array.from(new Set(participantIds.concat([currentUser.uid]))).sort();
    const profiles = await Promise.all(participants.map(resolveUserProfile));
    const participantUsernames = profiles.map(function (p) { return p.username || p.name || 'user'; });
    const participantNames = profiles.map(function (p) { return p.displayName || p.name || p.username || 'User'; });
    const participantAvatars = profiles.map(function (p) { return p.photoURL || ''; });

    const convoRef = doc(collection(db, 'conversations'));
    const payload = {
        participants,
        participantUsernames,
        participantNames,
        participantAvatars,
        type: participants.length > 2 ? 'group' : 'direct',
        title: participants.length > 2 ? (title || null) : null,
        avatarUrl: null,
        createdAt: serverTimestamp(),
        updatedAt: serverTimestamp(),
        lastMessagePreview: '',
        lastMessageSenderId: '',
        lastMessageAt: serverTimestamp(),
        unreadCounts: participants.reduce(function (acc, uid) { acc[uid] = 0; return acc; }, {}),
        mutedBy: [],
        pinnedBy: [],
        creatorId: currentUser.uid
    };

    await setDoc(convoRef, payload, { merge: true });
    conversationDetailsCache[convoRef.id] = { id: convoRef.id, ...payload };

    const participantMeta = participants.reduce(function (acc, uid) {
        acc[uid] = deriveOtherParticipantMeta(participants, uid, conversationDetailsCache[convoRef.id]);
        return acc;
    }, {});

    const currentRef = doc(db, `users/${currentUser.uid}/conversations/${convoRef.id}`);
    const currentPayload = {
        conversationId: convoRef.id,
        otherParticipantIds: participantMeta[currentUser.uid].otherIds,
        otherParticipantUsernames: participantMeta[currentUser.uid].usernames,
        otherParticipantAvatars: participantMeta[currentUser.uid].avatars,
        muted: false,
        pinned: false,
        archived: false,
        lastMessagePreview: '',
        lastMessageAt: payload.lastMessageAt,
        createdAt: payload.createdAt,
        participants,
        unreadCount: 0
    };
    await setDoc(currentRef, currentPayload, { merge: true });

    const optimistic = {
        id: convoRef.id,
        ...currentPayload,
        lastMessageAt: currentPayload.lastMessageAt || Timestamp.now(),
        createdAt: currentPayload.createdAt || Timestamp.now()
    };
    conversationMappings.push(optimistic);
    renderConversationList();

    return conversationDetailsCache[convoRef.id];
}

window.startChatFromSelection = async function () {
    if (!requireAuth() || newChatSelections.length === 0) return;
    const participantIds = newChatSelections.map(function (u) { return u.id; });
    toggleNewChatModal(false);

    try {
        if (participantIds.length === 1) {
            await window.openOrStartDirectConversationWithUser(participantIds[0]);
            return;
        }

        const nameInput = document.getElementById('chat-group-name');
        const title = (nameInput?.value || '').trim();
        const convo = await createGroupConversation(participantIds, title || null);
        if (convo) {
            await window.openMessagesPage();
            await openConversation(convo.id);
        }
    } catch (e) {
        console.warn('Unable to start chat from selection', e?.message || e);
        toast('Unable to start chat. Please try again.', 'error');
    }
};

window.openOrStartDirectConversationWithUser = async function (targetUserId, options = {}) {
    if (!requireAuth() || !targetUserId) return null;
    const blocked = new Set(userProfile.blockedUserIds || []);
    if (blocked.has(targetUserId)) { toast('You have blocked this user.', 'error'); return null; }
    const convoId = getDirectConversationId(currentUser.uid, targetUserId);
    try {
        await ensureConversation(convoId, targetUserId);
        await window.openMessagesPage();
        await openConversation(convoId);

        if (options.postId) {
            await sendChatPayload(convoId, {
                type: 'post_ref',
                postId: options.postId,
                threadId: options.threadId || null,
                text: options.initialText || 'Check out this post'
            });
        }
        return convoId;
    } catch (e) {
        console.warn('Unable to open or start direct conversation', e?.message || e);
        toast('Unable to start chat. Please try again.', 'error');
        return null;
    }
};

window.openConversation = openConversation;

window.openMessagesPage = async function () {
    if (!requireAuth()) return;
    conversationListFilter = 'all';
    conversationListSearchTerm = '';
    conversationListVisibleCount = 30;
    loadInboxModeFromStorage();
    const nextMode = inboxMode || 'messages';
    const searchInput = document.getElementById('conversation-list-search');
    if (searchInput) searchInput.value = '';
    window.navigateTo('messages');
    setConversationFilter('all');
    setTimeout(function () {
        setInboxMode(nextMode);
    }, 0);
    if (nextMode === 'messages') {
        await initConversations();
    }
};

window.sharePost = async function (postId, event) {
    if (event) event.stopPropagation();
    const post = allPosts.find(function (p) { return p.id === postId; });
    const threadId = post?.threadId || postId;
    const threadPath = window.NexeraRouter?.buildUrlForThread
        ? window.NexeraRouter.buildUrlForThread(threadId)
        : `/view-thread/${encodeURIComponent(threadId)}`;
    const url = `${window.location.origin}${threadPath}`;

    try {
        if (navigator.share) {
            await navigator.share({ title: 'Check out this Nexera post', url });
            return;
        }
    } catch (e) {
        console.warn('Native share failed', e);
    }

    try {
        await navigator.clipboard.writeText(url);
        toast('Post link copied', 'info');
    } catch (err) {
        console.error('Copy failed', err);
        toast('Unable to copy link', 'error');
    }
};

window.messageAuthor = async function (postId, event) {
    if (event) event.stopPropagation();
    if (!requireAuth() || !postId) return;
    const post = allPosts.find(function (p) { return p.id === postId; });
    if (!post || !post.userId) return;
    await openOrStartDirectConversationWithUser(post.userId, {
        postId: post.id,
        threadId: post.threadId || null,
        initialText: post.title ? `Regarding: ${post.title}` : 'Shared from your post'
    });
};
// --- Videos ---
function setVideoUploadModalMode(mode, video = null) {
    videoUploadMode = mode === 'edit' ? 'edit' : 'create';
    editingVideoId = videoUploadMode === 'edit' ? video?.id || null : null;
    editingVideoData = videoUploadMode === 'edit' ? video : null;
    if (videoUploadMode === 'create') pendingVideoHasCustomThumbnail = false;
    const titleEl = document.getElementById('video-upload-modal-title');
    const subtitleEl = document.getElementById('video-upload-modal-subtitle');
    const submitBtn = document.getElementById('video-upload-submit');
    if (titleEl) titleEl.textContent = videoUploadMode === 'edit' ? 'Edit Video' : 'Create Video';
    if (subtitleEl) {
        subtitleEl.textContent = videoUploadMode === 'edit'
            ? 'Update your title, description, tags, or thumbnail.'
            : 'Upload a clip, add details, and publish to your Nexera channel.';
    }
    if (submitBtn) submitBtn.textContent = videoUploadMode === 'edit' ? 'Save Changes' : 'Publish';
}

const VIDEO_UPLOAD_DEFAULTS = {
    monetizable: false,
    allowDownload: false,
    allowEmbed: false,
    allowComments: true,
    notifyFollowers: true,
    ageRestricted: false,
    containsSensitiveContent: false,
    category: 'General',
    language: 'en',
    license: 'Standard'
};

function setVideoToggleValue(id, value) {
    const input = document.getElementById(id);
    if (input) input.checked = !!value;
}

function getVideoToggleValue(id, fallback = false) {
    const input = document.getElementById(id);
    return input ? !!input.checked : fallback;
}

function setVideoSelectValue(id, value, fallback = '') {
    const input = document.getElementById(id);
    if (!input) return;
    const finalValue = value || fallback;
    if (finalValue) input.value = finalValue;
}

function findCategoryByName(name) {
    if (!name) return null;
    const target = name.toLowerCase();
    return categories.find(function (cat) { return (cat.name || '').toLowerCase() === target; }) || null;
}

function ensureVideoPostingDestination() {
    if (videoPostingDestinationId) return;
    setVideoPostingDestination({ id: 'no-topic', name: 'No topic' });
}

function getVideoPostingTopicName() {
    if (videoPostingDestinationId === 'no-topic') return 'No topic';
    if (videoPostingDestinationName) return videoPostingDestinationName;
    return resolveCategoryLabelBySlug(videoPostingDestinationId) || 'No topic';
}

function applyVideoUploadDefaults() {
    setVideoToggleValue('video-monetizable', VIDEO_UPLOAD_DEFAULTS.monetizable);
    setVideoToggleValue('video-allow-download', VIDEO_UPLOAD_DEFAULTS.allowDownload);
    setVideoToggleValue('video-allow-embed', VIDEO_UPLOAD_DEFAULTS.allowEmbed);
    setVideoToggleValue('video-allow-comments', VIDEO_UPLOAD_DEFAULTS.allowComments);
    setVideoToggleValue('video-notify-followers', VIDEO_UPLOAD_DEFAULTS.notifyFollowers);
    setVideoToggleValue('video-age-restricted', VIDEO_UPLOAD_DEFAULTS.ageRestricted);
    setVideoToggleValue('video-sensitive-content', VIDEO_UPLOAD_DEFAULTS.containsSensitiveContent);
    setVideoSelectValue('video-category', VIDEO_UPLOAD_DEFAULTS.category);
    setVideoSelectValue('video-language', VIDEO_UPLOAD_DEFAULTS.language);
    setVideoSelectValue('video-license', VIDEO_UPLOAD_DEFAULTS.license);
    videoPostingDestinationId = 'no-topic';
    videoPostingDestinationName = 'No topic';
    ensureVideoPostingDestination();
    renderVideoDestinationField();
    const scheduledInput = document.getElementById('video-scheduled-at');
    if (scheduledInput) scheduledInput.value = '';
}

function parseVideoScheduleTimestamp() {
    const scheduledInput = document.getElementById('video-scheduled-at');
    const rawValue = scheduledInput?.value || '';
    if (!rawValue) return null;
    const date = new Date(rawValue);
    if (Number.isNaN(date.getTime())) return null;
    return Timestamp.fromDate(date);
}

function populateVideoUploadForm(video = {}) {
    const title = document.getElementById('video-title');
    const description = document.getElementById('video-description');
    const visibility = document.getElementById('video-visibility');
    const preview = document.getElementById('video-upload-preview');
    const previewPlayer = document.getElementById('video-preview-player');
    const thumbPreview = document.getElementById('video-thumbnail-preview');

    if (title) title.value = video.title || video.caption || '';
    if (description) description.value = video.description || '';
    if (visibility) visibility.value = video.visibility || 'public';

    videoTags = Array.isArray(video.tags) ? video.tags.slice() : [];
    videoMentions = normalizeMentionsField(video.mentions || []);
    renderVideoTags();
    renderVideoMentions();

    if (previewPlayer && video.videoURL) {
        previewPlayer.src = video.videoURL;
        previewPlayer.load();
    }
    if (preview) preview.classList.add('active');
    if (thumbPreview) thumbPreview.src = resolveVideoThumbnail(video);
    pendingVideoHasCustomThumbnail = !!(video?.hasCustomThumbnail || video?.thumbURL || video?.thumbnail);

    setVideoSelectValue('video-category', video.category, VIDEO_UPLOAD_DEFAULTS.category);
    setVideoSelectValue('video-language', video.language, VIDEO_UPLOAD_DEFAULTS.language);
    setVideoSelectValue('video-license', video.license, VIDEO_UPLOAD_DEFAULTS.license);
    videoPostingDestinationId = video.categorySlug || 'no-topic';
    videoPostingDestinationName = resolveCategoryLabelBySlug(videoPostingDestinationId) || 'No topic';
    ensureVideoPostingDestination();
    renderVideoDestinationField();
    loadVideoCategories().then(function () {
        renderVideoDestinationField();
    });
    const scheduledInput = document.getElementById('video-scheduled-at');
    if (scheduledInput) {
        if (video.scheduledAt && typeof video.scheduledAt.toDate === 'function') {
            const scheduledDate = video.scheduledAt.toDate();
            scheduledInput.value = scheduledDate.toISOString().slice(0, 16);
        } else {
            scheduledInput.value = '';
        }
    }

    setVideoToggleValue('video-monetizable', video.monetizable ?? VIDEO_UPLOAD_DEFAULTS.monetizable);
    setVideoToggleValue('video-allow-download', video.allowDownload ?? VIDEO_UPLOAD_DEFAULTS.allowDownload);
    setVideoToggleValue('video-allow-embed', video.allowEmbed ?? VIDEO_UPLOAD_DEFAULTS.allowEmbed);
    setVideoToggleValue('video-allow-comments', video.allowComments ?? VIDEO_UPLOAD_DEFAULTS.allowComments);
    setVideoToggleValue('video-notify-followers', video.notifyFollowers ?? VIDEO_UPLOAD_DEFAULTS.notifyFollowers);
    setVideoToggleValue('video-age-restricted', video.ageRestricted ?? VIDEO_UPLOAD_DEFAULTS.ageRestricted);
    setVideoToggleValue('video-sensitive-content', video.containsSensitiveContent ?? VIDEO_UPLOAD_DEFAULTS.containsSensitiveContent);
}

window.openVideoUploadModal = function () {
    setVideoUploadModalMode('create');
    applyVideoUploadDefaults();
    loadVideoCategories().then(function () {
        renderVideoDestinationField();
    });
    return window.toggleVideoUploadModal(true);
};
window.toggleVideoUploadModal = function (show = true) {
    const modal = document.getElementById('video-upload-modal');
    if (modal) {
        if (show) {
            modal.style.display = 'flex';
            document.body.classList.add('video-create-open');
        } else {
            modal.style.display = 'none';
            document.body.classList.remove('video-create-open');
        }
    }
    document.body.classList.toggle('modal-open', show);
    if (show && videoUploadMode === 'create') {
        applyVideoUploadDefaults();
    }
    if (!show) {
        const fileInput = document.getElementById('video-file');
        const thumbInput = document.getElementById('video-thumbnail');
        const title = document.getElementById('video-title');
        const description = document.getElementById('video-description');
        const visibility = document.getElementById('video-visibility');
        const preview = document.getElementById('video-upload-preview');
        const previewPlayer = document.getElementById('video-preview-player');
        const thumbPreview = document.getElementById('video-thumbnail-preview');

        if (fileInput) fileInput.value = '';
        if (thumbInput) thumbInput.value = '';
        if (title) title.value = '';
        if (description) description.value = '';
        if (visibility) visibility.value = 'public';
        if (preview) preview.classList.remove('active');
        if (previewPlayer) previewPlayer.src = '';
        if (thumbPreview) thumbPreview.src = '';

        if (pendingVideoPreviewUrl) {
            URL.revokeObjectURL(pendingVideoPreviewUrl);
            pendingVideoPreviewUrl = null;
        }
        if (pendingVideoThumbnailUrl) {
            URL.revokeObjectURL(pendingVideoThumbnailUrl);
            pendingVideoThumbnailUrl = null;
        }
        pendingVideoThumbnailBlob = null;
        pendingVideoHasCustomThumbnail = false;
        pendingVideoDurationSeconds = null;
        resetVideoUploadMeta();
        setVideoUploadModalMode('create');
        applyVideoUploadDefaults();
        if (window.location.pathname === '/videos/create-video' || window.location.pathname === '/create-video') {
            window.NexeraRouter?.replaceStateSilently?.('/videos');
        }
    }
};

function resetVideoUploadForm() {
    window.toggleVideoUploadModal(false);
}

function pauseAllVideos() {
    document.querySelectorAll('#video-feed video').forEach(function (video) {
        video.pause();
    });
    const modalPlayer = document.getElementById('video-modal-player');
    if (modalPlayer && (!miniPlayerState || !modalPlayer.closest('#video-mini-player'))) {
        modalPlayer.pause();
    }
}

const VIDEO_MANAGER_PLACEHOLDER_THUMB = 'data:image/svg+xml;utf8,<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"192\" height=\"112\" viewBox=\"0 0 192 112\"><rect width=\"192\" height=\"112\" rx=\"12\" fill=\"%2322222b\"/><path d=\"M79 35l42 21-42 21V35z\" fill=\"%23555\"/></svg>';

function buildVideoManagerEntries() {
    const uploads = Array.isArray(uploadTasks) ? uploadTasks : [];
    const uploadEntries = uploads.map(function (upload) {
        const uploadId = upload.uploadId || upload.id;
        const cachedVideo = uploadId ? getVideoById(uploadId) : null;
        const statusLabel = getUploadTaskStatusLabel({ status: upload.status || '' });
        const isComplete = isUploadTaskComplete(upload);
        const progress = Math.max(0, Math.min(100, Number(upload.lastProgress ?? upload.progress ?? 0)));
        const sizeLabel = upload.size ? formatUploadFileSize(upload.size) : '';
        const thumbUrl = cachedVideo ? resolveVideoThumbnail(cachedVideo) : (upload.thumbnail || '');
        const title = cachedVideo?.title || cachedVideo?.caption || upload.fileName || upload.title || upload.storagePath || 'Untitled video';
        const sortAt = cachedVideo?.createdAt || upload.updatedAt || upload.startedAt || Date.now();
        return {
            id: uploadId,
            title,
            statusLabel,
            isComplete,
            progress,
            sizeLabel,
            thumbUrl,
            canOpen: !!cachedVideo,
            canEdit: !!cachedVideo,
            sortAt,
            videoId: cachedVideo?.id || null
        };
    });

    const uploadIds = new Set(uploadEntries.map(function (entry) { return entry.id; }));
    const uid = currentUser?.uid;
    const videoEntries = (videosCache || []).filter(function (video) {
        if (!uid) return false;
        if (video.ownerId !== uid) return false;
        return !uploadIds.has(video.id);
    }).map(function (video) {
        return {
            id: video.id,
            title: video.title || video.caption || 'Untitled video',
            statusLabel: 'Done',
            isComplete: true,
            progress: 100,
            sizeLabel: '',
            thumbUrl: resolveVideoThumbnail(video),
            canOpen: true,
            canEdit: true,
            sortAt: video.createdAt || Date.now(),
            videoId: video.id
        };
    });

    return uploadEntries.concat(videoEntries).sort(function (a, b) {
        const aTime = toDateSafe(a.sortAt)?.getTime?.() || Number(a.sortAt) || 0;
        const bTime = toDateSafe(b.sortAt)?.getTime?.() || Number(b.sortAt) || 0;
        return bTime - aTime;
    });
}

function renderVideoManagerList() {
    const list = document.getElementById('video-task-list');
    if (!list) return;
    ensureVideoTaskViewerBindings();
    const entries = buildVideoManagerEntries();
    if (!entries.length) {
        list.innerHTML = '<div class="video-task-empty">No videos yet.</div>';
        return;
    }

    list.innerHTML = entries.map(function (entry) {
        const thumbUrl = entry.thumbUrl || VIDEO_MANAGER_PLACEHOLDER_THUMB;
        const safeTitle = escapeHtml(entry.title || 'Untitled video');
        const openAttr = entry.canOpen && entry.videoId ? `data-video-open=\"${entry.videoId}\"` : '';
        const editAttr = entry.canEdit && entry.videoId ? `data-video-edit=\"${entry.videoId}\"` : '';
        const editDisabled = entry.canEdit ? '' : 'disabled';
        const menuAttr = entry.videoId ? `data-video-menu=\"${entry.videoId}\"` : '';
        const menuDisabled = entry.videoId ? '' : 'disabled';
        const progressClass = entry.isComplete ? 'is-complete' : '';
        return `
            <div class="video-task-row">
                <img class="video-task-thumb" src="${thumbUrl}" alt="Video thumbnail" ${openAttr} />
                <div class="video-task-meta">
                    <div class="video-task-title">${safeTitle}</div>
                    <div class="video-task-status">${entry.statusLabel}${entry.sizeLabel ? ` • ${entry.sizeLabel}` : ''}</div>
                    <div class="video-task-progress">
                        <span class="${progressClass}" style="width:${entry.progress}%;"></span>
                    </div>
                </div>
                <div class="video-task-actions">
                    <button class="video-task-action-btn" type="button" ${editAttr} ${editDisabled} title="Edit video">
                        <i class="ph ph-pencil-simple"></i>
                    </button>
                    <button class="video-task-action-btn" type="button" ${menuAttr} ${menuDisabled} title="More options">
                        <i class="ph ph-dots-three"></i>
                    </button>
                </div>
            </div>
        `;
    }).join('');
}

function renderUploadTasks() {
    renderVideoManagerList();
}

function toggleTaskViewer(show = true) {
    if (show) {
        if (typeof window.openVideoTaskViewer === 'function') {
            window.openVideoTaskViewer();
        } else {
            openVideoTaskViewer();
        }
    } else {
        closeVideoTaskViewer();
    }
    if (show) renderUploadTasks();
}

window.toggleTaskViewer = toggleTaskViewer;

function blobToDataUrl(blob) {
    return new Promise(function (resolve) {
        if (!blob) return resolve('');
        const reader = new FileReader();
        reader.onload = function () { resolve(reader.result || ''); };
        reader.onerror = function () { resolve(''); };
        reader.readAsDataURL(blob);
    });
    const modalPlayer = document.getElementById('video-modal-player');
    if (modalPlayer) modalPlayer.pause();
}

function resetVideoUploadMeta() {
    videoTags = [];
    videoMentions = [];
    renderVideoTags();
    renderVideoMentions();
    updateVideoTagLimit(false);
    videoNewTagNotice = '';
    updateVideoTagHelper('', '', getKnownTags());
    const tagRow = document.getElementById('video-tag-input-row');
    const mentionRow = document.getElementById('video-mention-input-row');
    const tagSuggestions = document.getElementById('video-tag-suggestions');
    const mentionSuggestions = document.getElementById('video-mention-suggestions');
    if (tagRow) tagRow.style.display = 'none';
    if (mentionRow) mentionRow.style.display = 'none';
    if (tagSuggestions) tagSuggestions.style.display = 'none';
    if (mentionSuggestions) mentionSuggestions.style.display = 'none';
}

function toggleVideoTagInput(show) {
    const row = document.getElementById('video-tag-input-row');
    if (!row) return;
    const nextState = show !== undefined ? show : row.style.display !== 'flex';
    row.style.display = nextState ? 'flex' : 'none';
    if (nextState) {
        const input = document.getElementById('video-tag-input');
        if (input) {
            input.focus();
            filterVideoTagSuggestions(input.value);
        }
        updateVideoTagLimit(false);
        videoNewTagNotice = '';
    }
}

function addVideoTag(raw = '') {
    const normalized = normalizeTagValue(raw);
    if (!normalized) return;
    if (videoTags.length >= 10) {
        updateVideoTagLimit(true);
        return;
    }
    if (videoTags.includes(normalized)) return;
    updateVideoTagLimit(false);
    videoTags.push(normalized);
    renderVideoTags();
    videoNewTagNotice = '';
    const input = document.getElementById('video-tag-input');
    if (input) {
        input.focus();
        input.setSelectionRange(input.value.length, input.value.length);
    }
}

function removeVideoTag(tag = '') {
    videoTags = videoTags.filter(function (t) { return t !== tag; });
    renderVideoTags();
    updateVideoTagLimit(false);
}

function handleVideoTagInputKey(event) {
    if (event.key === 'Enter') {
        event.preventDefault();
        const input = event.target;
        const raw = input.value || '';
        const query = getHashtagQuery(raw);
        if (!query) {
            filterVideoTagSuggestions(raw);
            return;
        }
        // Enter adds the normalized tag and shows "creating new tag" when unknown.
        const knownTags = getKnownTags();
        addVideoTag(raw);
        if (query && !knownTags.includes(query)) {
            videoNewTagNotice = `Creating new tag #${query}`;
        } else {
            videoNewTagNotice = '';
        }
        input.value = '';
        updateVideoTagHelper(raw, query, knownTags);
        filterVideoTagSuggestions('');
    } else {
        videoNewTagNotice = '';
        filterVideoTagSuggestions(event.target.value || '');
    }
}

function filterVideoTagSuggestions(term = '') {
    const container = document.getElementById('video-tag-suggestions');
    if (!container) return;
    // Only show suggestions after a valid hashtag prefix (# + 1 char).
    const cleaned = getHashtagQuery(term);
    const known = getKnownTags();
    const matches = cleaned ? rankTagSuggestions(known, cleaned, videoTags) : [];
    if (!matches.length) {
        container.innerHTML = '';
        container.style.display = 'none';
    } else {
        container.style.display = 'flex';
        container.innerHTML = matches.map(function (tag) {
            return `<button type="button" class="tag-suggestion" onmousedown="event.preventDefault()" onclick="window.addVideoTag('${tag}')">#${escapeHtml(tag)}</button>`;
        }).join('');
    }
    updateVideoTagHelper(term, cleaned, known);
}

function renderVideoTags() {
    const container = document.getElementById('video-tags-list');
    if (!container) return;
    if (!videoTags.length) {
        container.innerHTML = '<div class="empty-chip">No tags added</div>';
        return;
    }
    container.innerHTML = videoTags.map(function (tag) {
        return `<span class="tag-chip">#${escapeHtml(tag)}<button type="button" class="chip-remove" onclick="window.removeVideoTag('${tag}')">&times;</button></span>`;
    }).join('');
}

function updateVideoTagHelper(rawValue = '', cleaned = '', known = []) {
    const helper = document.getElementById('video-tag-helper-text');
    if (!helper) return;
    const normalized = cleaned || getHashtagQuery(rawValue);
    if (videoNewTagNotice) {
        helper.textContent = videoNewTagNotice;
        helper.style.display = 'block';
        return;
    }
    if (normalized && !known.includes(normalized)) {
        helper.textContent = `Creating new tag #${normalized}`;
        helper.style.display = 'block';
        return;
    }
    helper.textContent = '';
    helper.style.display = 'none';
}

function updateVideoTagLimit(show) {
    const note = document.getElementById('video-tag-limit-note');
    if (!note) return;
    note.style.display = show ? 'block' : 'none';
}

function addVideoMention(rawUser) {
    const normalized = normalizeMentionEntry(rawUser);
    if (!normalized.username) return;
    if (videoMentions.some(function (m) { return m.username === normalized.username; })) return;
    videoMentions.push(normalized);
    renderVideoMentions();
}

function removeVideoMention(username) {
    videoMentions = videoMentions.filter(function (m) { return m.username !== username; });
    renderVideoMentions();
}

function renderVideoMentions() {
    const container = document.getElementById('video-mentions-list');
    if (!container) return;
    if (!videoMentions.length) {
        container.innerHTML = '<div class="empty-chip">No mentions added</div>';
        return;
    }
    container.innerHTML = videoMentions.map(function (mention) {
        const avatar = renderAvatar({ ...mention, name: mention.displayName || mention.username }, { size: 32, className: 'mention-avatar' });
        const badge = renderVerifiedBadge(mention, 'with-gap');
        return `<div class="mention-card">${avatar}<div class="mention-meta"><div class="mention-name">${escapeHtml(mention.displayName || mention.username)}${badge}</div><div class="mention-handle">@${escapeHtml(mention.username)}</div></div><button type="button" class="chip-remove" onclick="window.removeVideoMention('${mention.username}')">&times;</button></div>`;
    }).join('');
}

async function searchVideoMentionSuggestions(term = '') {
    const listEl = document.getElementById('video-mention-suggestions');
    if (!listEl) return;
    const cleaned = (term || '').trim().replace(/^@/, '').toLowerCase();
    if (!cleaned) {
        listEl.innerHTML = '';
        listEl.style.display = 'none';
        return;
    }
    try {
        const userQuery = query(
            collection(db, 'users'),
            orderBy('username'),
            startAt(cleaned),
            endAt(cleaned + '\uf8ff'),
            limit(5)
        );
        const snap = await getDocs(userQuery);
        const results = snap.docs.map(function (d) {
            return { id: d.id, uid: d.id, ...normalizeUserProfileData(d.data(), d.id) };
        });
        if (!results.length) {
            listEl.innerHTML = '';
            listEl.style.display = 'none';
            return;
        }
        listEl.style.display = 'block';
        listEl.innerHTML = results.map(function (profile) {
            const avatar = renderAvatar({ ...profile, uid: profile.id || profile.uid }, { size: 28 });
            return `<button type="button" class="mention-suggestion" onclick='window.addVideoMention(${JSON.stringify({
                uid: profile.id || profile.uid,
                username: profile.username,
                displayName: profile.name || profile.nickname || profile.displayName || '',
                accountRoles: profile.accountRoles || []
            }).replace(/'/g, "&apos;")})'>
                ${avatar}
                <div class="mention-suggestion-meta">
                    <div class="mention-name">${escapeHtml(profile.name || profile.nickname || profile.displayName || profile.username)}</div>
                    <div class="mention-handle">@${escapeHtml(profile.username || '')}</div>
                </div>
            </button>`;
        }).join('');
    } catch (err) {
        console.warn('Video mention search failed', err);
        listEl.innerHTML = '';
        listEl.style.display = 'none';
    }
}

function handleVideoMentionInput(event) {
    const value = event.target.value;
    if (videoMentionSearchTimer) clearTimeout(videoMentionSearchTimer);
    videoMentionSearchTimer = setTimeout(function () { searchVideoMentionSuggestions(value); }, 200);
}

async function generateThumbnailFromVideo(file) {
    if (!file) return null;
    const useDataUrl = typeof navigator !== 'undefined'
        && /Safari/.test(navigator.userAgent || '')
        && !/Chrome|Chromium|Edg/.test(navigator.userAgent || '');
    const video = document.createElement('video');
    video.preload = 'metadata';
    video.muted = true;
    video.playsInline = true;
    video.setAttribute('playsinline', '');
    video.setAttribute('webkit-playsinline', '');
    const url = useDataUrl ? await blobToDataUrl(file) : URL.createObjectURL(file);

    try {
        return await new Promise(function (resolve) {
            let settled = false;
            const finalize = function (blob) {
                if (settled) return;
                settled = true;
                resolve(blob);
            };
            const captureFrame = function () {
                const canvas = document.createElement('canvas');
                canvas.width = video.videoWidth || 640;
                canvas.height = video.videoHeight || 360;
                const ctx = canvas.getContext('2d');
                if (ctx) ctx.drawImage(video, 0, 0, canvas.width, canvas.height);
                canvas.toBlob(function (blob) {
                    finalize(blob);
                }, 'image/jpeg', 0.85);
            };
            const requestFrame = function () {
                if (typeof video.requestVideoFrameCallback === 'function') {
                    video.requestVideoFrameCallback(function () { captureFrame(); });
                } else {
                    captureFrame();
                }
            };
            const handleReady = function () {
                if (settled) return;
                const targetTime = Math.min(0.1, video.duration || 0);
                if (Number.isFinite(targetTime) && targetTime > 0) {
                    try {
                        video.currentTime = targetTime;
                        return;
                    } catch (err) {
                        console.warn('[VideoUpload] Unable to seek for thumbnail', err);
                    }
                }
                requestFrame();
            };
            video.onloadedmetadata = handleReady;
            video.onloadeddata = handleReady;
            video.onseeked = function () {
                if (settled) return;
                captureFrame();
            };
            video.onerror = function () { finalize(null); };
            video.src = url;
            video.load();
        });
    } finally {
        if (!useDataUrl) URL.revokeObjectURL(url);
    }
}

function resolveVideoDurationFromFile(file, previewUrl = null) {
    if (!file && !previewUrl) return Promise.resolve(null);
    return new Promise(function (resolve) {
        const video = document.createElement('video');
        video.preload = 'metadata';
        video.muted = true;
        video.playsInline = true;
        video.setAttribute('playsinline', '');
        video.setAttribute('webkit-playsinline', '');
        const needsCleanup = !previewUrl;
        const url = previewUrl || URL.createObjectURL(file);

        const finalize = function (value) {
            if (needsCleanup) URL.revokeObjectURL(url);
            resolve(value);
        };

        video.onloadedmetadata = function () {
            const duration = Number(video.duration || 0) || 0;
            finalize(duration ? Math.round(duration) : null);
        };
        video.onerror = function () { finalize(null); };
        video.src = url;
        video.load();
    });
}

window.handleVideoFileChange = async function (event) {
    const input = event?.target;
    const file = input?.files?.[0];
    const preview = document.getElementById('video-upload-preview');
    const previewPlayer = document.getElementById('video-preview-player');
    const thumbPreview = document.getElementById('video-thumbnail-preview');

    if (pendingVideoPreviewUrl) {
        URL.revokeObjectURL(pendingVideoPreviewUrl);
        pendingVideoPreviewUrl = null;
    }

    if (!file) {
        if (preview) preview.classList.remove('active');
        if (previewPlayer) previewPlayer.src = '';
        if (thumbPreview) thumbPreview.src = '';
        pendingVideoThumbnailBlob = null;
        pendingVideoHasCustomThumbnail = false;
        pendingVideoDurationSeconds = null;
        return;
    }

    pendingVideoPreviewUrl = URL.createObjectURL(file);
    if (previewPlayer) {
        previewPlayer.onerror = function () {
            if (pendingVideoPreviewUrl) {
                URL.revokeObjectURL(pendingVideoPreviewUrl);
                pendingVideoPreviewUrl = null;
            }
            blobToDataUrl(file).then(function (dataUrl) {
                if (!dataUrl) return;
                previewPlayer.src = dataUrl;
                previewPlayer.load();
            });
        };
        previewPlayer.src = pendingVideoPreviewUrl;
        previewPlayer.load();
    }
    if (preview) preview.classList.add('active');

    pendingVideoDurationSeconds = await resolveVideoDurationFromFile(file, pendingVideoPreviewUrl);

    if (!pendingVideoHasCustomThumbnail) {
        pendingVideoThumbnailBlob = await generateThumbnailFromVideo(file);
        if (pendingVideoThumbnailUrl) {
            URL.revokeObjectURL(pendingVideoThumbnailUrl);
            pendingVideoThumbnailUrl = null;
        }
        if (pendingVideoThumbnailBlob && thumbPreview) {
            const dataUrl = await blobToDataUrl(pendingVideoThumbnailBlob);
            thumbPreview.src = dataUrl;
        }
    }

};

window.handleThumbnailChange = function (event) {
    const input = event?.target;
    const file = input?.files?.[0];
    const thumbPreview = document.getElementById('video-thumbnail-preview');

    pendingVideoThumbnailBlob = null;
    if (pendingVideoThumbnailUrl) {
        URL.revokeObjectURL(pendingVideoThumbnailUrl);
        pendingVideoThumbnailUrl = null;
    }

    if (!file) return;

    pendingVideoHasCustomThumbnail = true;
    pendingVideoThumbnailBlob = file;
    pendingVideoThumbnailUrl = URL.createObjectURL(file);
    if (thumbPreview) thumbPreview.src = pendingVideoThumbnailUrl;
};

window.handleVideoSubmit = function () {
    if (videoUploadMode === 'edit') {
        window.updateVideoDetails();
        return;
    }
    window.uploadVideo();
};

window.openVideoEditModal = async function (videoId) {
    if (!requireAuth() || !videoId) return;
    closeVideoTaskViewer();
    let video = getVideoById(videoId);
    if (!video) {
        try {
            const snap = await getDoc(doc(db, 'videos', videoId));
            if (snap.exists()) video = { id: snap.id, ...snap.data() };
        } catch (err) {
            console.error('Unable to load video for edit', err);
        }
    }
    if (!video) {
        toast('Unable to load video details.', 'error');
        return;
    }

    const fileInput = document.getElementById('video-file');
    const thumbInput = document.getElementById('video-thumbnail');
    if (fileInput) fileInput.value = '';
    if (thumbInput) thumbInput.value = '';
    if (pendingVideoPreviewUrl) {
        URL.revokeObjectURL(pendingVideoPreviewUrl);
        pendingVideoPreviewUrl = null;
    }
    if (pendingVideoThumbnailUrl) {
        URL.revokeObjectURL(pendingVideoThumbnailUrl);
        pendingVideoThumbnailUrl = null;
    }
    pendingVideoThumbnailBlob = null;
    pendingVideoHasCustomThumbnail = !!(video?.hasCustomThumbnail || video?.thumbURL || video?.thumbnail);

    const videoData = {
        ...video,
        storagePath: video.storagePath || `videos/${currentUser.uid}/${video.id}`
    };
    setVideoUploadModalMode('edit', videoData);
    window.toggleVideoUploadModal(true);
    populateVideoUploadForm(videoData);
};

window.updateVideoDetails = async function () {
    if (!requireAuth()) return;
    if (!editingVideoId) return;
    const submitBtn = document.getElementById('video-upload-submit');
    const thumbInput = document.getElementById('video-thumbnail');
    const title = document.getElementById('video-title')?.value || '';
    const description = document.getElementById('video-description')?.value || '';
    const tags = Array.from(new Set((videoTags || []).map(normalizeTagValue).filter(Boolean)));
    const mentions = normalizeMentionsField(videoMentions || []);
    const visibility = document.getElementById('video-visibility').value || 'public';
    const category = document.getElementById('video-category')?.value || VIDEO_UPLOAD_DEFAULTS.category;
    const categorySlug = videoPostingDestinationId && videoPostingDestinationId !== 'no-topic' ? videoPostingDestinationId : 'no-topic';
    const topic = resolveCategoryLabelBySlug(categorySlug) || 'No topic';
    const language = document.getElementById('video-language')?.value || VIDEO_UPLOAD_DEFAULTS.language;
    const license = document.getElementById('video-license')?.value || VIDEO_UPLOAD_DEFAULTS.license;
    const allowDownload = getVideoToggleValue('video-allow-download', VIDEO_UPLOAD_DEFAULTS.allowDownload);
    const allowEmbed = getVideoToggleValue('video-allow-embed', VIDEO_UPLOAD_DEFAULTS.allowEmbed);
    const allowComments = getVideoToggleValue('video-allow-comments', VIDEO_UPLOAD_DEFAULTS.allowComments);
    const notifyFollowers = getVideoToggleValue('video-notify-followers', VIDEO_UPLOAD_DEFAULTS.notifyFollowers);
    const ageRestricted = getVideoToggleValue('video-age-restricted', VIDEO_UPLOAD_DEFAULTS.ageRestricted);
    const containsSensitiveContent = getVideoToggleValue('video-sensitive-content', VIDEO_UPLOAD_DEFAULTS.containsSensitiveContent);
    const scheduledAt = parseVideoScheduleTimestamp();
    const videoId = editingVideoId;
    let thumbURL = editingVideoData?.thumbURL || editingVideoData?.thumbnail || '';
    let hasCustomThumbnail = !!(editingVideoData?.hasCustomThumbnail || thumbURL);
    const storagePath = editingVideoData?.storagePath || `videos/${currentUser.uid}/${videoId}`;

    try {
        if (submitBtn) {
            submitBtn.disabled = true;
            submitBtn.innerHTML = `<span class="button-spinner" aria-hidden="true"></span> Saving...`;
        }

        let thumbBlob = null;
        if (thumbInput && thumbInput.files && thumbInput.files[0]) {
            thumbBlob = thumbInput.files[0];
        } else if (pendingVideoThumbnailBlob) {
            thumbBlob = pendingVideoThumbnailBlob;
        }

        if (thumbBlob) {
            const thumbRef = ref(storage, `${storagePath}/thumb.jpg`);
            await uploadBytes(thumbRef, thumbBlob);
            thumbURL = await getDownloadURL(thumbRef);
            hasCustomThumbnail = true;
        }

        const monetizable = editingVideoData?.monetizable ?? VIDEO_UPLOAD_DEFAULTS.monetizable;
        const scheduledVisibility = scheduledAt ? visibility : null;
        const effectiveVisibility = scheduledAt ? 'private' : visibility;

        await updateDoc(doc(db, 'videos', videoId), {
            title,
            caption: title,
            description,
            tags,
            mentions,
            visibility: effectiveVisibility,
            scheduledVisibility,
            scheduledAt,
            category,
            categorySlug,
            topic,
            language,
            license,
            monetizable,
            allowDownload,
            allowEmbed,
            allowComments,
            notifyFollowers,
            ageRestricted,
            containsSensitiveContent,
            thumbURL,
            hasCustomThumbnail,
            updatedAt: serverTimestamp()
        });

        const cached = getVideoById(videoId);
        if (cached) {
            cached.title = title;
            cached.caption = title;
            cached.description = description;
            cached.tags = tags;
            cached.mentions = mentions;
            cached.visibility = effectiveVisibility;
            cached.scheduledVisibility = scheduledVisibility;
            cached.scheduledAt = scheduledAt;
            cached.category = category;
            cached.categorySlug = categorySlug;
            cached.topic = topic;
            cached.language = language;
            cached.license = license;
            cached.monetizable = monetizable;
            cached.allowDownload = allowDownload;
            cached.allowEmbed = allowEmbed;
            cached.allowComments = allowComments;
            cached.notifyFollowers = notifyFollowers;
            cached.ageRestricted = ageRestricted;
            cached.containsSensitiveContent = containsSensitiveContent;
            if (thumbURL) cached.thumbURL = thumbURL;
            cached.hasCustomThumbnail = hasCustomThumbnail;
        }

        toast('Video updated.', 'success');
        resetVideoUploadForm();
        renderUploadTasks();
    } catch (err) {
        console.error('Video update failed', err);
        toast('Video update failed. Please try again.', 'error');
    } finally {
        if (submitBtn) {
            submitBtn.disabled = false;
            submitBtn.textContent = videoUploadMode === 'edit' ? 'Save Changes' : 'Publish';
        }
    }
};

function handleVideoSearchInput(event) {
    const input = event.target;
    const rawValue = input?.value || '';
    const selection = captureInputSelection(input);
    videoSearchTerm = rawValue.toLowerCase();
    clearTimeout(videoSearchDebounce);
    videoSearchDebounce = setTimeout(function () {
        updateSearchQueryParam(rawValue);
        refreshVideoFeedWithFilters({ skipTopBar: true });
        restoreInputSelection(input, selection);
    }, SEARCH_DEBOUNCE_MS);
}

function handleVideoSortChange(event) {
    videoSortMode = event.target.value;
    refreshVideoFeedWithFilters();
}

function setVideoFilter(filter) {
    videoFilter = filter;
    refreshVideoFeedWithFilters();
}

function formatUploadFileSize(bytes) {
    const value = Number(bytes) || 0;
    if (value >= 1024 * 1024 * 1024) return `${(value / (1024 * 1024 * 1024)).toFixed(1)} GB`;
    if (value >= 1024 * 1024) return `${(value / (1024 * 1024)).toFixed(1)} MB`;
    if (value >= 1024) return `${Math.round(value / 1024)} KB`;
    return `${value} B`;
}

function getUploadTaskStatusLabel(upload = {}) {
    const status = (upload.status || '').toUpperCase();
    if (status === 'FAILED') return 'Failed';
    if (status === 'PAUSED') return 'Paused';
    if (status === 'UPLOADING') return 'Uploading';
    if (status === 'PROCESSING') return 'Processing';
    if (status === 'READY' || status === 'UPLOADED') return 'Done';
    return 'Uploading';
}

function isUploadTaskComplete(upload = {}) {
    const status = (upload.status || '').toUpperCase();
    return status === 'READY' || status === 'UPLOADED';
}

function setUploadTasks(tasks) {
    uploadTasks = Array.isArray(tasks) ? tasks : [];
    renderUploadTasks();
    renderVideosTopBar();
}

function ensureVideoTaskViewerBindings() {
    const modal = document.getElementById('video-task-viewer');
    if (!modal || videoTaskViewerBound) return;
    videoTaskViewerBound = true;

    modal.addEventListener('click', function (event) {
        const openBtn = event.target.closest('[data-video-open]');
        if (openBtn) {
            const videoId = openBtn.getAttribute('data-video-open');
            if (videoId) {
                closeVideoTaskViewer();
                window.openVideoDetail(videoId);
            }
            return;
        }

        const editBtn = event.target.closest('[data-video-edit]');
        if (editBtn) {
            const videoId = editBtn.getAttribute('data-video-edit');
            if (videoId) {
                closeVideoTaskViewer();
                window.openVideoEditModal(videoId);
            }
            return;
        }

        const menuBtn = event.target.closest('[data-video-menu]');
        if (menuBtn) {
            const videoId = menuBtn.getAttribute('data-video-menu');
            if (videoId) {
                openVideoManagerMenu(event, videoId);
            }
        }
    });
}

function ensureVideoManagerMenu() {
    let dropdown = document.getElementById('video-manager-options-dropdown');
    if (dropdown) return dropdown;
    dropdown = document.createElement('div');
    dropdown.id = 'video-manager-options-dropdown';
    dropdown.className = 'post-options-dropdown menu-surface';
    dropdown.style.display = 'none';
    dropdown.innerHTML = `
        <button type="button" id="video-manager-edit-btn">
            <i class="ph ph-pencil-simple"></i> Edit details
        </button>
        <button type="button" id="video-manager-delete-btn">
            <i class="ph ph-trash"></i> Delete
        </button>
    `;
    document.body.appendChild(dropdown);
    const editBtn = dropdown.querySelector('#video-manager-edit-btn');
    if (editBtn) {
        editBtn.addEventListener('click', function (event) {
            event.stopPropagation();
            const targetId = videoManagerMenuState.videoId;
            closeVideoManagerMenu();
            if (targetId) window.openVideoEditModal(targetId);
        });
    }
    const deleteBtn = dropdown.querySelector('#video-manager-delete-btn');
    if (deleteBtn) {
        deleteBtn.addEventListener('click', function (event) {
            event.stopPropagation();
            const targetId = videoManagerMenuState.videoId;
            closeVideoManagerMenu();
            if (targetId) confirmDeleteVideo(targetId);
        });
    }
    return dropdown;
}

function openVideoManagerMenu(event, videoId) {
    if (!videoId) return;
    const dropdown = ensureVideoManagerMenu();
    const trigger = event?.target?.closest('[data-video-menu]');
    if (!dropdown || !trigger) return;
    const video = getVideoById(videoId);
    const isOwner = !!(currentUser?.uid && video?.ownerId === currentUser.uid);
    const editBtn = dropdown.querySelector('#video-manager-edit-btn');
    const deleteBtn = dropdown.querySelector('#video-manager-delete-btn');
    if (editBtn) editBtn.style.display = isOwner ? 'flex' : 'none';
    if (deleteBtn) deleteBtn.style.display = isOwner ? 'flex' : 'none';
    videoManagerMenuState.videoId = videoId;
    dropdown.style.display = 'block';
    const rect = trigger.getBoundingClientRect();
    const dropdownRect = dropdown.getBoundingClientRect();
    dropdown.style.top = `${rect.bottom + window.scrollY + 6}px`;
    dropdown.style.left = `${Math.max(10, rect.right + window.scrollX - dropdownRect.width)}px`;
    setTimeout(function () {
        document.addEventListener('click', closeVideoManagerMenu, { once: true });
    }, 0);
}

function closeVideoManagerMenu() {
    const dropdown = document.getElementById('video-manager-options-dropdown');
    if (dropdown) dropdown.style.display = 'none';
    videoManagerMenuState.videoId = null;
}

async function confirmDeleteVideo(videoId) {
    if (!requireAuth() || !videoId) return;
    await openConfirmModal({
        title: 'Delete video?',
        message: 'Are you sure? This video will be removed permanently.',
        confirmText: 'Delete',
        onConfirm: async function () {
            const video = getVideoById(videoId);
            const storagePath = video?.storagePath || '';
            try {
                if (storagePath) {
                    await deleteObject(ref(storage, `${storagePath}/source.mp4`)).catch(function (err) {
                        console.warn('Video delete warning (source)', err);
                    });
                    await deleteObject(ref(storage, `${storagePath}/thumb.jpg`)).catch(function (err) {
                        console.warn('Video delete warning (thumb)', err);
                    });
                } else if (video?.videoURL) {
                    await deleteObject(ref(storage, video.videoURL)).catch(function (err) {
                        console.warn('Video delete warning (source url)', err);
                    });
                }
                if (video?.thumbURL || video?.thumbnail) {
                    const thumbRef = video.thumbURL || video.thumbnail;
                    await deleteObject(ref(storage, thumbRef)).catch(function (err) {
                        console.warn('Video delete warning (thumb url)', err);
                    });
                }
            } catch (err) {
                console.warn('Video delete storage warning', err);
            }
            try {
                await deleteDoc(doc(db, 'videos', videoId));
                videosCache = videosCache.filter(function (entry) { return entry.id !== videoId; });
                uploadTasks = uploadTasks.filter(function (task) { return task.id !== videoId; });
                renderUploadTasks();
                if (currentViewId === 'videos') refreshVideoFeedWithFilters();
                toast('Video deleted.', 'info');
            } catch (err) {
                console.error('Video delete failed', err);
                toast('Failed to delete video.', 'error');
            }
        }
    });
}

function openVideoTaskViewer() {
    const modal = document.getElementById('video-task-viewer');
    if (!modal) return;
    modal.style.display = 'flex';
    document.body.classList.add('modal-open');
    document.body.classList.add('video-manager-open');
    renderUploadTasks();
}

window.openVideoTaskViewer = openVideoTaskViewer;

function closeVideoTaskViewer() {
    const modal = document.getElementById('video-task-viewer');
    if (!modal) return;
    modal.style.display = 'none';
    document.body.classList.remove('modal-open');
    document.body.classList.remove('video-manager-open');
    closeVideoManagerMenu();
    if (window.location.pathname === '/videos/video-manager') {
        window.NexeraRouter?.replaceStateSilently?.('/videos');
    }
}

function renderVideosTopBar() {
    const container = document.getElementById('videos-topbar');
    if (!container) return;

    const topBar = buildVideosHeader({
        searchValue: videoSearchTerm,
        onSearch: handleVideoSearchInput,
        filter: videoFilter,
        onFilter: setVideoFilter,
        sort: videoSortMode,
        onSort: handleVideoSortChange,
        onAction: function (action) { window.handleUiStubAction?.(action); }
    });

    const uploadBtn = document.createElement('button');
    uploadBtn.className = 'create-btn-sidebar topbar-action-btn';
    uploadBtn.style.width = 'auto';
    uploadBtn.innerHTML = '<i class="ph ph-upload-simple"></i> Create Video';
    uploadBtn.onclick = function () { window.openVideoUploadModal(); };

    const taskBtn = document.createElement('button');
    taskBtn.className = 'icon-pill topbar-action-btn';
    taskBtn.innerHTML = '<i class="ph ph-list"></i> Video Manager';
    taskBtn.onclick = function () { window.toggleTaskViewer(true); };

    const actionsSlot = topBar.querySelector('.topbar-actions');
    if (actionsSlot) {
        actionsSlot.appendChild(taskBtn);
        actionsSlot.appendChild(uploadBtn);
    }

    container.innerHTML = '';
    container.appendChild(topBar);
}

function refreshVideoFeedWithFilters(options = {}) {
    if (!options.skipTopBar) {
        renderVideosTopBar();
    }
    if (isInlineWatchOpen()) return;
    let filtered = videosCache.slice();

    if (videoSearchTerm) {
        filtered = filtered.filter(function (video) {
            const title = (video.title || '').toLowerCase();
            const caption = (video.caption || '').toLowerCase();
            const tags = (video.hashtags || []).map(function (t) { return (`#${t}`).toLowerCase(); });
            return title.includes(videoSearchTerm) || caption.includes(videoSearchTerm) || tags.some(function (tag) { return tag.includes(videoSearchTerm); });
        });
    }

    if (videoFilter === 'Trending') {
        filtered = filtered.slice().sort(function (a, b) { return (b.stats?.views || 0) - (a.stats?.views || 0); });
    } else if (videoFilter === 'Shorts') {
        filtered = filtered.filter(function (video) { return (video.duration || 0) <= 120 || (video.lengthSeconds || 0) <= 120 || !(video.duration || video.lengthSeconds); });
    } else if (videoFilter === 'Saved') {
        const savedSet = videoEngagementState.saved.size ? videoEngagementState.saved : new Set(userProfile.savedVideos || []);
        filtered = filtered.filter(function (video) { return savedSet.has(video.id); });
    }

    if (videoSortMode === 'popular') {
        filtered = filtered.slice().sort(function (a, b) { return (b.stats?.views || 0) - (a.stats?.views || 0); });
    }

    renderVideoFeed(filtered);
}

async function fetchVideosBatch({ reset = false } = {}) {
    // Lazy-load videos in batches to avoid blocking the UI on first render.
    if (videosPagination.loading || videosPagination.done) return [];
    videosPagination.loading = true;
    try {
        const constraints = [orderBy('createdAt', 'desc'), limit(VIDEOS_BATCH_SIZE)];
        if (!reset && videosPagination.lastDoc) {
            constraints.splice(1, 0, startAfter(videosPagination.lastDoc));
        }
        const snap = await getDocs(query(collection(db, 'videos'), ...constraints));
        videosPagination.lastDoc = snap.docs[snap.docs.length - 1] || videosPagination.lastDoc;
        if (snap.docs.length < VIDEOS_BATCH_SIZE) {
            videosPagination.done = true;
        }
        return snap.docs.map(function (d) { return ({ id: d.id, ...d.data() }); });
    } finally {
        videosPagination.loading = false;
    }
}

function initVideoFeed(options = {}) {
    const force = options.force === true;
    if (videosFeedLoaded && !force) return;
    if (force) {
        videosFeedLoaded = false;
        videosFeedLoading = false;
        videosPagination.lastDoc = null;
        videosPagination.done = false;
        if (videosScrollObserver) {
            videosScrollObserver.disconnect();
        }
    }
    videosFeedLoaded = true;
    videosFeedLoading = true;
    debugVideo('feed-init', { force });
    renderVideosTopBar();
    renderVideoFeed([]);
    loadVideoCategories().then(function () {
        refreshVideoFeedWithFilters({ skipTopBar: true });
    });
    videosPagination.lastDoc = null;
    videosPagination.done = false;
    debugVideo('feed-fetch-start');
    fetchVideosBatch({ reset: true }).then(function (batch) {
        videosCache = batch;
        videosCache.forEach(ensureVideoStats);
        videosFeedLoading = false;
        refreshVideoFeedWithFilters();
        debugVideo('feed-fetch-end', { count: batch.length });
        const modal = document.getElementById('video-detail-modal');
        const activeVideoId = modal?.dataset?.videoId;
        if (activeVideoId) updateVideoModalButtons(activeVideoId);
    }).catch(function (error) {
        console.error('Unable to load videos', error);
        videosFeedLoaded = false;
        videosFeedLoading = false;
        const feed = document.getElementById('video-feed');
        if (feed) {
            feed.innerHTML = `
                <div class="empty-state">
                    <div style="font-weight:700; margin-bottom:6px;">Unable to load videos.</div>
                    <div style="color:var(--text-muted);">Please try refreshing the page.</div>
                </div>`;
        }
    });
}

function formatCompactNumber(value) {
    const num = Number(value) || 0;
    if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(num >= 10_000_000 ? 0 : 1)}M`;
    if (num >= 1_000) return `${(num / 1_000).toFixed(num >= 10_000 ? 0 : 1)}K`;
    return `${num}`;
}

function formatVideoTimestamp(ts) {
    const date = toDateSafe(ts);
    if (!date) return 'Just now';
    const now = new Date();
    const diff = now.getTime() - date.getTime();
    const days = Math.floor(diff / 86400000);
    if (days <= 0) return 'Today';
    if (days === 1) return '1 day ago';
    if (days < 7) return `${days} days ago`;
    if (days < 30) return `${Math.floor(days / 7)}w ago`;
    if (days < 365) return `${Math.floor(days / 30)}mo ago`;
    return `${Math.floor(days / 365)}y ago`;
}

function formatVideoDuration(video = {}) {
    const seconds = Number(video.duration || video.lengthSeconds || 0) || 0;
    const mins = Math.floor(seconds / 60);
    const secs = Math.floor(seconds % 60);
    if (!seconds) return '0:00';
    return `${mins}:${secs.toString().padStart(2, '0')}`;
}

function backfillVideoDuration(video = {}) {
    if (!video || !video.id) return;
    if (Number(video.duration || 0) > 0) return;
    if (videoDurationBackfill.has(video.id)) return;
    const src = video.videoURL || video.url || '';
    if (!src) return;
    videoDurationBackfill.add(video.id);
    const probe = document.createElement('video');
    probe.preload = 'metadata';
    probe.src = src;
    probe.onloadedmetadata = function () {
        const duration = Math.round(Number(probe.duration || 0) || 0);
        if (duration > 0) {
            video.duration = duration;
            const durationEl = document.querySelector(`.video-card[data-video-id="${video.id}"] .video-duration`);
            if (durationEl) durationEl.textContent = formatVideoDuration(video);
            updateDoc(doc(db, 'videos', video.id), { duration }).catch(function (error) {
                console.warn('Unable to backfill video duration', error);
            });
        } else {
            videoDurationBackfill.delete(video.id);
        }
    };
    probe.onerror = function () {
        videoDurationBackfill.delete(video.id);
    };
    probe.load();
}

function resolveVideoThumbnail(video = {}) {
    return video.thumbURL || video.thumbnail || video.previewImage || 'https://images.unsplash.com/photo-1516117172878-fd2c41f4a759?auto=format&fit=crop&w=1200&q=80';
}

function getVideoViewCount(video = {}) {
    const statsViews = typeof video.stats?.views === 'number' ? video.stats.views : null;
    const directViews = typeof video.views === 'number' ? video.views : null;
    return statsViews ?? directViews ?? 0;
}

function ensureVideoStats(video = {}) {
    if (!video.stats || typeof video.stats !== 'object') video.stats = {};
    if (typeof video.stats.likes !== 'number') video.stats.likes = 0;
    if (typeof video.stats.dislikes !== 'number') video.stats.dislikes = 0;
    if (typeof video.stats.saves !== 'number') video.stats.saves = 0;
    if (typeof video.stats.comments !== 'number') video.stats.comments = 0;
    if (typeof video.stats.views !== 'number') video.stats.views = 0;
    return video;
}

function isInlineWatchOpen() {
    return USE_INLINE_VIDEO_WATCH && document.body.classList.contains('video-watch-open');
}

function getVideoById(videoId) {
    const fromVideos = videosCache.find(function (entry) { return entry.id === videoId; });
    if (fromVideos) return fromVideos;
    return (homeVideosCache || []).find(function (entry) { return entry.id === videoId; }) || null;
}

function getVideoEngagementStatus(videoId) {
    return {
        liked: videoEngagementState.liked.has(videoId),
        disliked: videoEngagementState.disliked.has(videoId),
        saved: videoEngagementState.saved.has(videoId)
    };
}

function setVideoEngagementStatus(videoId, { liked, disliked, saved }) {
    if (liked === true) videoEngagementState.liked.add(videoId);
    if (liked === false) videoEngagementState.liked.delete(videoId);
    if (disliked === true) videoEngagementState.disliked.add(videoId);
    if (disliked === false) videoEngagementState.disliked.delete(videoId);
    if (saved === true) videoEngagementState.saved.add(videoId);
    if (saved === false) videoEngagementState.saved.delete(videoId);
}

function updateVideoStats(videoId, deltas = {}) {
    const video = getVideoById(videoId);
    if (!video) return;
    ensureVideoStats(video);
    if (typeof deltas.likes === 'number') video.stats.likes = Math.max(0, video.stats.likes + deltas.likes);
    if (typeof deltas.dislikes === 'number') video.stats.dislikes = Math.max(0, video.stats.dislikes + deltas.dislikes);
    if (typeof deltas.saves === 'number') video.stats.saves = Math.max(0, video.stats.saves + deltas.saves);
    if (typeof deltas.comments === 'number') video.stats.comments = Math.max(0, video.stats.comments + deltas.comments);
    if (typeof deltas.views === 'number') video.stats.views = Math.max(0, video.stats.views + deltas.views);
}

function renderVideoActionButton(button, { icon, label, count, active, activeColor }) {
    if (!button) return;
    const finalIcon = `${active ? 'ph-fill' : 'ph'} ${icon}`;
    const color = active ? activeColor : 'inherit';
    const countLabel = typeof count === 'number' ? formatCompactNumber(count) : '';
    const text = label ? ` ${label}` : '';
    button.innerHTML = `<i class="${finalIcon}"></i>${countLabel ? ` ${countLabel}` : text}`;
    button.style.color = color;
}

function updateVideoModalButtons(videoId) {
    const modal = document.getElementById('video-detail-modal');
    if (!modal) return;
    if (modal.dataset.videoId && modal.dataset.videoId !== videoId) return;
    const video = getVideoById(videoId);
    if (!video) return;
    ensureVideoStats(video);
    const state = getVideoEngagementStatus(videoId);
    renderVideoActionButton(document.getElementById('video-modal-like'), {
        icon: 'ph-thumbs-up',
        label: 'Like',
        count: video.stats.likes,
        active: state.liked,
        activeColor: '#00f2ea'
    });
    renderVideoActionButton(document.getElementById('video-modal-dislike'), {
        icon: 'ph-thumbs-down',
        label: 'Dislike',
        count: video.stats.dislikes,
        active: state.disliked,
        activeColor: '#ff3d3d'
    });
    renderVideoActionButton(document.getElementById('video-modal-save'), {
        icon: 'ph-bookmark',
        label: state.saved ? 'Saved' : 'Save',
        count: null,
        active: state.saved,
        activeColor: '#00f2ea'
    });
    renderVideoActionButton(document.getElementById('video-modal-share'), {
        icon: 'ph-share-network',
        label: 'Share',
        count: null,
        active: false,
        activeColor: 'inherit'
    });
    const viewsEl = document.getElementById('video-modal-views');
    if (viewsEl) viewsEl.textContent = `${formatCompactNumber(video.stats.views || 0)} views`;
}

async function refreshVideoStatsFromServer(videoId) {
    if (!videoId) return;
    try {
        const snap = await getDoc(doc(db, 'videos', videoId));
        if (!snap.exists()) return;
        const video = getVideoById(videoId);
        if (!video) return;
        video.stats = { ...(snap.data().stats || {}) };
    } catch (err) {
        console.warn('Unable to refresh video stats', err);
    }
}

async function hydrateVideoEngagement(videoId) {
    if (!currentUser || !videoId) return getVideoEngagementStatus(videoId);
    if (videoEngagementHydrated.has(videoId)) return getVideoEngagementStatus(videoId);
    try {
        const likeRef = doc(db, 'videos', videoId, 'likes', currentUser.uid);
        const dislikeRef = doc(db, 'videos', videoId, 'dislikes', currentUser.uid);
        const saveRef = doc(db, 'videos', videoId, 'saves', currentUser.uid);
        const [likeSnap, dislikeSnap, saveSnap] = await Promise.all([
            getDoc(likeRef),
            getDoc(dislikeRef),
            getDoc(saveRef)
        ]);
        setVideoEngagementStatus(videoId, {
            liked: likeSnap.exists(),
            disliked: dislikeSnap.exists(),
            saved: saveSnap.exists() || videoEngagementState.saved.has(videoId)
        });
        videoEngagementHydrated.add(videoId);
    } catch (err) {
        console.warn('Unable to hydrate video engagement', err);
    }
    return getVideoEngagementStatus(videoId);
}

function openVideoFromFeed(videoId, videoData) {
    if (!videoId) return;
    if (videoData && window.Nexera?.ensureVideoInCache) {
        window.Nexera.ensureVideoInCache(videoData);
    }
    pendingVideoOpenId = videoId;
    window.navigateTo('videos');
}

function buildVideoCard(video) {
    const author = getCachedUser(video.ownerId) || { name: 'Nexera Creator', username: 'creator' };
    const canEdit = !!(currentUser?.uid && video.ownerId === currentUser.uid);
    backfillVideoDuration(video);
    const result = buildVideoCardElement({
        video,
        author,
        canEdit,
        utils: {
            formatCompactNumber,
            formatVideoTimestamp,
            resolveVideoThumbnail,
            getVideoViewCount,
            formatVideoDuration,
            resolveCategoryLabelBySlug,
            applyAvatarToElement,
            ensureVideoStats
        },
        onOpen: function () {
            if (currentViewId === 'feed') {
                openVideoFromFeed(video.id, video);
                return;
            }
            window.openVideoDetail(video.id);
        },
        onOpenProfile: function (uid, event) {
            window.openUserProfile(uid, event);
        },
        onEdit: function (entry, event) {
            if (!canEdit) return;
            event?.stopPropagation?.();
            window.openVideoEditModal(entry.id);
        },
        onOverflow: function (entry, event) {
            openVideoManagerMenu(event, entry.id);
        }
    });

    if (video.ownerId && !getCachedUser(video.ownerId, { allowStale: false })) {
        resolveUserProfile(video.ownerId).then(function (profile) {
            if (!profile) return;
            applyAvatarToElement(result.avatar, profile, { size: 42 });
            result.channel.textContent = profile.displayName || profile.name || profile.username || 'Nexera Creator';
        });
    }

    return result.card;
}

function renderVideoFeed(videos = []) {
    const feed = document.getElementById('video-feed');
    if (!feed) return;
    feed.innerHTML = '';
    if (videosFeedLoading) {
        feed.appendChild(renderVideoSkeletons());
        return;
    }
    debugVideo('feed-render', { count: videos.length });
    if (videos.length === 0) {
        feed.innerHTML = `
            <div class="empty-state">
                <div style="font-weight:700; margin-bottom:6px;">No videos match this filter.</div>
                <div style="color:var(--text-muted);">Try clearing filters or refreshing the list.</div>
                <div style="margin-top:12px; display:flex; gap:8px; justify-content:center;">
                    <button class="icon-pill" onclick="window.handleUiStubAction?.('videos-clear-filters')"><i class="ph ph-eraser"></i> Clear filters</button>
                    <button class="icon-pill" onclick="window.handleUiStubAction?.('videos-refresh')"><i class="ph ph-arrow-clockwise"></i> Refresh</button>
                </div>
            </div>`;
        return;
    }

    videos.forEach(function (video) {
        const card = buildVideoCard(video);
        const animateIn = shouldAnimateItem(`video:${video.id}`);
        if (animateIn) card.classList.add('animate-in');
        feed.appendChild(card);
    });

    insertScrollSentinel(feed, 'video-feed-sentinel', 0, { placeAfter: true });
    ensureVideoScrollObserver();
}

function ensureVideoScrollObserver() {
    if (isInlineWatchOpen()) return;
    const sentinel = document.getElementById('video-feed-sentinel');
    if (!sentinel || videosPagination.done) return;
    if (videosScrollObserver) {
        videosScrollObserver.disconnect();
    }
    videosScrollObserver = new IntersectionObserver(function (entries) {
        entries.forEach(function (entry) {
            if (entry.isIntersecting) {
                loadMoreVideos();
            }
        });
    }, { rootMargin: FEED_PREFETCH_ROOT_MARGIN });
    videosScrollObserver.observe(sentinel);
}

async function loadMoreVideos() {
    if (isInlineWatchOpen()) return;
    if (videosPagination.loading || videosPagination.done) return;
    try {
        const batch = await fetchVideosBatch();
        if (!batch.length) return;
        const existing = new Set(videosCache.map(function (video) { return video.id; }));
        batch.forEach(function (video) {
            if (!existing.has(video.id)) {
                videosCache.push(video);
                existing.add(video.id);
            }
        });
        videosCache.forEach(ensureVideoStats);
        refreshVideoFeedWithFilters({ skipTopBar: true });
    } catch (error) {
        console.warn('Videos pagination failed', error);
    }
}

function renderVideoSkeletons(count = VIDEOS_BATCH_SIZE) {
    const wrapper = document.createElement('div');
    wrapper.className = 'video-skeleton-grid';
    for (let i = 0; i < count; i += 1) {
        const card = document.createElement('div');
        card.className = 'video-card skeleton';
        card.innerHTML = `
            <div class="video-thumb skeleton-block"></div>
            <div class="video-meta">
                <div class="video-avatar skeleton-circle"></div>
                <div class="video-info">
                    <div class="skeleton-line"></div>
                    <div class="skeleton-line short"></div>
                </div>
            </div>
        `;
        wrapper.appendChild(card);
    }
    return wrapper;
}

function captureVideoProfileReturn(videoId) {
    const player = document.getElementById('video-modal-player');
    profileReturnContext = {
        videoId,
        currentTime: player?.currentTime || 0
    };
    minimizeVideoDetail({ updateRoute: false });
}

function getVideoModalPlayer() {
    return document.getElementById('video-modal-player');
}

function getVideoModalPlayerContainer() {
    return document.querySelector('.video-player-frame') || document.querySelector('.video-modal-player');
}

let videoFeedRestoreState = null;
let videoWatchRestoreState = null;
const videoFeedModalHomes = new Map();

function mountInlineVideoViewer() {
    const viewVideos = document.getElementById('view-videos');
    if (!viewVideos) return false;
    // Mental anchor: the viewer must live inside #view-videos (no floating overlays).
    if (!videoViewerInlineState) {
        videoViewerInlineState = {
            nodes: Array.from(viewVideos.childNodes),
            scrollTop: viewVideos.scrollTop || 0
        };
    }
    viewVideos.innerHTML = '';
    const container = document.createElement('div');
    container.id = 'video-detail-modal';
    container.className = 'video-modal-inline';
    const viewerShell = document.createElement('div');
    viewerShell.className = 'video-viewer-inline';
    const closeBtn = document.createElement('button');
    closeBtn.className = 'video-modal-close';
    closeBtn.type = 'button';
    closeBtn.setAttribute('aria-label', 'Close video details');
    closeBtn.innerHTML = '&times;';
    closeBtn.onclick = function () { window.closeVideoDetail(); };
    viewerShell.appendChild(closeBtn);
    viewerShell.appendChild(buildVideoViewerLayout());
    container.appendChild(viewerShell);
    viewVideos.appendChild(container);
    document.body.classList.add('video-viewer-inline-open');
    return true;
}

function restoreInlineVideoViewer() {
    const viewVideos = document.getElementById('view-videos');
    if (!viewVideos || !videoViewerInlineState) return false;
    // Mental anchor end: restore the grid content back into #view-videos.
    viewVideos.innerHTML = '';
    videoViewerInlineState.nodes.forEach(function (node) {
        viewVideos.appendChild(node);
    });
    viewVideos.scrollTop = videoViewerInlineState.scrollTop || 0;
    document.body.classList.remove('video-viewer-inline-open');
    videoViewerInlineState = null;
    initVideoFeed({ force: true });
    return true;
}

function mountVideoModalInFeed(modalId) {
    const feed = document.getElementById('video-feed');
    const modal = document.getElementById(modalId);
    if (!feed || !modal) return false;
    if (!videoFeedModalHomes.has(modalId)) {
        videoFeedModalHomes.set(modalId, {
            parent: modal.parentElement,
            nextSibling: modal.nextElementSibling
        });
    }
    if (!videoFeedRestoreState) {
        videoFeedRestoreState = {
            nodes: Array.from(feed.childNodes),
            scrollTop: feed.scrollTop,
            activeModalId: modalId
        };
    } else {
        videoFeedRestoreState.activeModalId = modalId;
    }
    feed.innerHTML = '';
    feed.appendChild(modal);
    feed.classList.add('video-feed-modal-open');
    if (modal.classList.contains('modal-overlay')) {
        modal.classList.add('video-feed-modal');
    }
    modal.style.display = 'flex';
    return true;
}

function restoreVideoFeedFromModal(modalId) {
    const feed = document.getElementById('video-feed');
    if (!feed) return false;
    const modal = modalId ? document.getElementById(modalId) : null;
    if (modal) {
        modal.style.display = 'none';
        modal.classList.remove('video-feed-modal');
    }
    if (!videoFeedRestoreState) return false;
    feed.innerHTML = '';
    videoFeedRestoreState.nodes.forEach(function (node) {
        feed.appendChild(node);
    });
    if (typeof videoFeedRestoreState.scrollTop === 'number') {
        feed.scrollTop = videoFeedRestoreState.scrollTop;
    }
    feed.classList.remove('video-feed-modal-open');
    if (modalId && modal) {
        const home = videoFeedModalHomes.get(modalId);
        if (home?.parent) {
            if (home.nextSibling && home.nextSibling.parentElement === home.parent) {
                home.parent.insertBefore(modal, home.nextSibling);
            } else {
                home.parent.appendChild(modal);
            }
        }
    }
    videoFeedRestoreState = null;
    return true;
}

// UI scaffolding: rebuild video viewer layout to enable three-column layout and controls.
function initVideoViewerLayout() {
    const modal = document.getElementById('video-detail-modal');
    if (!modal || modal.dataset.viewerScaffold || !USE_CUSTOM_VIDEO_VIEWER) return;
    modal.dataset.viewerScaffold = 'true';
    modal.innerHTML = '';

    const backdrop = document.createElement('div');
    backdrop.className = 'video-modal-backdrop';
    backdrop.onclick = function () { window.closeVideoDetail(); };

    const shell = document.createElement('div');
    shell.className = 'video-modal-shell';

    const closeBtn = document.createElement('button');
    closeBtn.className = 'video-modal-close';
    closeBtn.type = 'button';
    closeBtn.setAttribute('aria-label', 'Close video details');
    closeBtn.innerHTML = '&times;';
    closeBtn.onclick = function () { window.closeVideoDetail(); };

    shell.appendChild(closeBtn);
    shell.appendChild(buildVideoViewerLayout());

    const dialog = document.createElement('div');
    dialog.className = 'video-modal-dialog';
    dialog.appendChild(shell);

    modal.appendChild(backdrop);
    modal.appendChild(dialog);
}

// Inline watch helpers are grouped to avoid duplicate function declarations in module scope.
const InlineWatchHelpers = {
    getInlineWatchSuggestions(videoId, limit = 8) {
        return videosCache.filter(function (video) { return video.id !== videoId; }).slice(0, limit);
    }
};

function buildInlineWatchPage(video, author, suggestions) {
    const videoTitle = escapeHtml(video.title || video.caption || 'Untitled video');
    const videoDescription = escapeHtml(video.description || '');
    const channelName = escapeHtml(author?.displayName || author?.name || author?.username || 'Nexera Creator');
    const channelHandle = escapeHtml(author?.username ? `@${author.username}` : 'Nexera');
    const views = formatCompactNumber(getVideoViewCount(video));
    const updated = formatVideoTimestamp(video.createdAt) || 'Today';
    const likeCount = formatCompactNumber(video.stats?.likes || 0);
    const suggestedHtml = suggestions.map(function (entry) {
        const thumb = escapeHtml(resolveVideoThumbnail(entry));
        const title = escapeHtml(entry.title || entry.caption || 'Untitled video');
        const channel = escapeHtml((getCachedUser(entry.ownerId)?.displayName || getCachedUser(entry.ownerId)?.name || getCachedUser(entry.ownerId)?.username || 'Nexera Creator'));
        const stats = `${formatCompactNumber(getVideoViewCount(entry))} views • ${formatVideoTimestamp(entry.createdAt)}`;
        return `
            <div class="watch-suggestion">
                <div class="watch-suggestion-thumb" style="background-image:url('${thumb}')">
                    <span class="watch-suggestion-duration">${formatVideoDuration(entry)}</span>
                </div>
                <div class="watch-suggestion-meta">
                    <div class="watch-suggestion-title">${title}</div>
                    <div class="watch-suggestion-channel">${channel}</div>
                    <div class="watch-suggestion-stats">${stats}</div>
                </div>
                <button class="watch-suggestion-menu" aria-label="More options"><i class="ph ph-dots-three-vertical"></i></button>
            </div>
        `;
    }).join('');

    return `
        <div class="watch-page">
            <div class="watch-grid">
                <section class="watch-primary">
                    <div class="watch-player">
                        <video id="watch-player" playsinline preload="metadata" src="${escapeHtml(video.videoURL || '')}"></video>
                        <div class="watch-player-overlay">
                            <div class="watch-timeline"></div>
                            <div class="watch-controls">
                                <button class="watch-control-btn" data-action="toggle-play"><i class="ph ph-play"></i></button>
                                <button class="watch-control-btn"><i class="ph ph-speaker-high"></i></button>
                                <div class="watch-control-spacer"></div>
                                <button class="watch-control-btn"><i class="ph ph-gear"></i></button>
                                <button class="watch-control-btn"><i class="ph ph-arrows-out"></i></button>
                            </div>
                        </div>
                    </div>
                    <div class="watch-title">${videoTitle}</div>
                    <div class="watch-channel-row">
                        <div class="watch-channel-info">
                            <div class="watch-channel-avatar"></div>
                            <div>
                                <div class="watch-channel-name">${channelName}</div>
                                <div class="watch-channel-handle">${channelHandle}</div>
                            </div>
                            <button class="watch-join-btn">Follow</button>
                        </div>
                        <div class="watch-actions">
                            <button class="watch-pill watch-like-btn" data-count="${video.stats?.likes || 0}" data-liked="false"><i class="ph ph-thumbs-up"></i><span>Like</span><span class="watch-like-count">${likeCount}</span></button>
                            <button class="watch-pill"><i class="ph ph-thumbs-down"></i></button>
                            <button class="watch-pill" data-watch-action="share"><i class="ph ph-share-network"></i> Share</button>
                            <button class="watch-pill" data-watch-action="save"><i class="ph ph-bookmark-simple"></i> Save</button>
                            <button class="watch-pill" data-watch-action="thanks"><i class="ph ph-currency-dollar"></i> Thanks</button>
                        </div>
                    </div>
                    <div class="watch-description">
                        <div class="watch-description-meta">${views} views • ${updated}</div>
                        <div class="watch-description-text">${videoDescription || 'No description yet.'}</div>
                        <span class="watch-description-more">...more</span>
                    </div>
                    <div class="watch-comments">
                        <div class="watch-comments-header">
                            <span>5,090 Comments</span>
                            <button class="watch-pill small"><i class="ph ph-sort-ascending"></i> Sort by</button>
                        </div>
                        <div class="watch-comment-compose">
                            <div class="watch-comment-avatar"></div>
                            <input type="text" placeholder="Add a comment..." />
                        </div>
                        <div class="watch-comment">
                            <div class="watch-comment-avatar"></div>
                            <div>
                                <div class="watch-comment-meta">Nexera Viewer • 1 day ago</div>
                                <div class="watch-comment-text">Great breakdown—thanks for sharing!</div>
                                <div class="watch-comment-actions"><span><i class="ph ph-thumbs-up"></i> 24</span><span>Reply</span></div>
                            </div>
                        </div>
                    </div>
                </section>
                <aside class="watch-rail">
                    <div class="watch-rail-header">Up next</div>
                    ${suggestedHtml}
                </aside>
            </div>
        </div>
    `;
}

async function openInlineVideoWatch(video) {
    const feed = document.getElementById('video-feed');
    if (!feed) return;
    if (!videoWatchRestoreState) {
        videoWatchRestoreState = {
            nodes: Array.from(feed.childNodes),
            scrollTop: feed.scrollTop,
            sidebarCollapsed
        };
    }
    applyDesktopSidebarState(true, false);
    if (videosScrollObserver) {
        videosScrollObserver.disconnect();
    }
    const sentinel = document.getElementById('video-feed-sentinel');
    if (sentinel) sentinel.remove();
    feed.innerHTML = '';
    feed.classList.add('video-watch-open');
    document.body.classList.add('video-watch-open');

    ensureVideoStats(video);
    const author = await resolveUserProfile(video.ownerId || '');
    const suggestions = InlineWatchHelpers.getInlineWatchSuggestions(video.id);
    feed.innerHTML = buildInlineWatchPage(video, author, suggestions);
    bindInlineWatchInteractions(video, author);
}

function restoreInlineVideoWatch() {
    const feed = document.getElementById('video-feed');
    if (!feed || !videoWatchRestoreState) return;
    feed.innerHTML = '';
    videoWatchRestoreState.nodes.forEach(function (node) {
        feed.appendChild(node);
    });
    feed.scrollTop = videoWatchRestoreState.scrollTop || 0;
    applyDesktopSidebarState(videoWatchRestoreState.sidebarCollapsed, false);
    feed.classList.remove('video-watch-open');
    document.body.classList.remove('video-watch-open');
    videoWatchRestoreState = null;
    if (!document.getElementById('video-feed-sentinel')) {
        insertScrollSentinel(feed, 'video-feed-sentinel', 0, { placeAfter: true });
    }
    ensureVideoScrollObserver();
}

function openInlineWatchActionModal(action) {
    const titles = {
        share: 'Share',
        save: 'Save',
        thanks: 'Thanks'
    };
    const messages = {
        share: 'Sharing options are coming soon.',
        save: 'Save this video to your library.',
        thanks: 'Thanks for supporting the creator.'
    };
    if (typeof openConfirmModal === 'function') {
        openConfirmModal({
            title: titles[action] || 'Action',
            message: messages[action] || 'This action is coming soon.',
            confirmText: 'Close',
            cancelText: 'Dismiss'
        });
        return;
    }
    toast(messages[action] || 'Action coming soon.', 'info');
}

function updateInlineWatchPlayState(player, button) {
    if (!player || !button) return;
    const icon = player.paused ? 'ph ph-play' : 'ph ph-pause';
    button.innerHTML = `<i class="${icon}"></i>`;
}

function updateInlineWatchLikeButton(button) {
    if (!button) return;
    const liked = button.dataset.liked === 'true';
    const count = Number(button.dataset.count) || 0;
    const iconClass = liked ? 'ph-fill ph-thumbs-up' : 'ph ph-thumbs-up';
    const label = liked ? 'Liked' : 'Like';
    const countEl = button.querySelector('.watch-like-count');
    const textNode = button.querySelector('span');
    const iconEl = button.querySelector('i');
    if (iconEl) iconEl.className = iconClass;
    if (textNode) textNode.textContent = label;
    if (countEl) countEl.textContent = formatCompactNumber(count);
}

function bindInlineWatchInteractions(video, author) {
    const root = document.querySelector('#video-feed .watch-page');
    if (!root) return;
    const player = root.querySelector('#watch-player');
    const playBtn = root.querySelector('.watch-control-btn[data-action="toggle-play"]');
    const likeBtn = root.querySelector('.watch-like-btn');
    const channelAvatar = root.querySelector('.watch-channel-avatar');
    const composeAvatar = root.querySelector('.watch-comment-compose .watch-comment-avatar');

    if (channelAvatar) applyAvatarToElement(channelAvatar, author || {}, { size: 40 });
    if (composeAvatar) applyAvatarToElement(composeAvatar, currentUser || author || {}, { size: 40 });
    root.querySelectorAll('.watch-comment-avatar').forEach(function (avatar) {
        if (avatar === composeAvatar) return;
        applyAvatarToElement(avatar, author || {}, { size: 40 });
    });

    if (player && playBtn) {
        updateInlineWatchPlayState(player, playBtn);
        playBtn.addEventListener('click', function () {
            if (player.paused) {
                player.play().catch(function () {});
            } else {
                player.pause();
            }
            updateInlineWatchPlayState(player, playBtn);
        });
        player.addEventListener('play', function () { updateInlineWatchPlayState(player, playBtn); });
        player.addEventListener('pause', function () { updateInlineWatchPlayState(player, playBtn); });
    }

    if (likeBtn) {
        updateInlineWatchLikeButton(likeBtn);
        likeBtn.addEventListener('click', function () {
            const liked = likeBtn.dataset.liked === 'true';
            const current = Number(likeBtn.dataset.count) || 0;
            const nextLiked = !liked;
            const nextCount = Math.max(0, current + (nextLiked ? 1 : -1));
            likeBtn.dataset.liked = nextLiked ? 'true' : 'false';
            likeBtn.dataset.count = String(nextCount);
            updateInlineWatchLikeButton(likeBtn);
        });
    }

    root.querySelectorAll('[data-watch-action]').forEach(function (button) {
        button.addEventListener('click', function () {
            openInlineWatchActionModal(button.dataset.watchAction);
        });
    });
}

function updateVideoControlPlayState(player, button) {
    if (!player || !button) return;
    const icon = player.paused ? 'ph ph-play' : 'ph ph-pause';
    button.innerHTML = `<i class="${icon}"></i>`;
}

function updateVideoScrubVisuals(player, scrub) {
    if (!player || !scrub) return;
    const container = scrub.closest('.video-control-scrub');
    if (!container) return;
    const duration = Number(player.duration) || 0;
    const current = Math.max(0, Number(player.currentTime) || 0);
    let progress = 0;
    let buffered = 0;
    if (duration > 0) {
        progress = Math.min(100, (current / duration) * 100);
        if (player.buffered && player.buffered.length) {
            try {
                const end = player.buffered.end(player.buffered.length - 1);
                buffered = Math.min(100, (end / duration) * 100);
            } catch (error) {
                buffered = 0;
            }
        }
    }
    container.style.setProperty('--video-progress', `${progress}%`);
    container.style.setProperty('--video-buffer', `${buffered}%`);
}

function applyVideoCaptions(player, video) {
    if (!player || !video) return;
    if (!Array.isArray(video.captions)) {
        video.captions = [{
            label: 'English',
            srclang: 'en',
            src: 'data:text/vtt,WEBVTT%0A%0A',
            default: true,
            placeholder: true
        }];
    }
    player.querySelectorAll('track').forEach(function (track) { track.remove(); });
    video.captions.forEach(function (entry, index) {
        if (!entry?.src) return;
        const track = document.createElement('track');
        track.kind = 'subtitles';
        track.label = entry.label || 'English';
        track.srclang = entry.srclang || 'en';
        track.src = entry.src;
        if (entry.default || index === 0) track.default = true;
        player.appendChild(track);
    });
}

function resolveDefaultQuality() {
    const connection = navigator.connection || navigator.mozConnection || navigator.webkitConnection;
    const downlink = connection?.downlink || 0;
    if (!downlink) return 'auto';
    if (downlink < 3) return '480';
    if (downlink < 6) return '720';
    return '1080';
}

function getStoredPlaybackSpeed() {
    const stored = Number(localStorage.getItem(PLAYBACK_SPEED_KEY));
    return Number.isFinite(stored) && stored > 0 ? stored : 1;
}

function getStoredQuality() {
    return localStorage.getItem(VIDEO_QUALITY_KEY) || resolveDefaultQuality();
}

function applyVideoSettings(player) {
    if (!player) return;
    player.playbackRate = getStoredPlaybackSpeed();
}

function updateSettingsPopoverSelection(root) {
    if (!root) return;
    const quality = String(getStoredQuality());
    root.querySelectorAll('[data-quality]').forEach(function (btn) {
        btn.classList.toggle('is-selected', btn.dataset.quality === quality);
    });
    const speedInput = root.querySelector('#video-settings-speed');
    const speedValue = root.querySelector('#video-settings-speed-value');
    if (speedInput && speedValue) {
        const speed = getStoredPlaybackSpeed();
        speedInput.value = String(speed);
        speedValue.textContent = `${speed.toFixed(2)}×`;
    }
}

function getStoredCaptionsMode() {
    return localStorage.getItem(AUTO_CAPTIONS_KEY) || 'off';
}

function setStoredCaptionsMode(mode) {
    localStorage.setItem(AUTO_CAPTIONS_KEY, mode);
}

function ensureAutoCaptionsTrack(player) {
    if (!player) return null;
    const tracks = Array.from(player.textTracks || []);
    const existing = tracks.find(function (track) { return track.label === 'Auto'; });
    if (existing) return existing;
    return player.addTextTrack('captions', 'Auto', 'en');
}

let autoCaptionState = null;
function stopAutoCaptions() {
    if (!autoCaptionState) return;
    try {
        autoCaptionState.recognition?.stop();
    } catch (err) {
        console.warn('Unable to stop auto captions', err);
    }
    autoCaptionState = null;
}

function startAutoCaptions(player) {
    if (!player) return false;
    if (!player.captureStream || !(window.SpeechRecognition || window.webkitSpeechRecognition)) {
        toast('Auto-generated captions not supported in this browser.', 'info');
        return false;
    }
    const stream = player.captureStream();
    const audioTracks = stream.getAudioTracks();
    if (!audioTracks.length) {
        toast('Auto-generated captions not supported for this video.', 'info');
        return false;
    }
    const speechStream = new MediaStream([audioTracks[0]]);
    const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
    const recognition = new SpeechRecognition();
    recognition.stream = speechStream;
    recognition.continuous = true;
    recognition.interimResults = true;
    recognition.lang = 'en-US';
    const autoTrack = ensureAutoCaptionsTrack(player);
    autoTrack.mode = 'showing';
    recognition.onresult = function (event) {
        const result = event.results[event.results.length - 1];
        const transcript = result && result[0] ? result[0].transcript.trim() : '';
        if (!transcript) return;
        const start = Math.max(0, player.currentTime - 1);
        const end = player.currentTime + 2;
        try {
            autoTrack.addCue(new VTTCue(start, end, transcript));
        } catch (err) {
            console.warn('Unable to add caption cue', err);
        }
    };
    recognition.onerror = function (event) {
        console.warn('Auto captions error', event);
    };
    try {
        recognition.start();
    } catch (err) {
        console.warn('Unable to start auto captions', err);
        return false;
    }
    autoCaptionState = { recognition, speechStream };
    return true;
}

function applyCaptionsMode(player, mode) {
    if (!player) return;
    const tracks = Array.from(player.textTracks || []);
    tracks.forEach(function (track) {
        track.mode = 'disabled';
    });
    if (mode === 'auto') {
        stopAutoCaptions();
        startAutoCaptions(player);
        return;
    }
    stopAutoCaptions();
    if (mode.startsWith('track:')) {
        const label = mode.replace('track:', '');
        const match = tracks.find(function (track) { return track.label === label; });
        if (match) {
            match.mode = 'showing';
        }
    }
}

let videoFullscreenTarget = null;
let videoFullscreenPending = false;
let isFullscreenChanging = false;
let pendingViewerCleanup = null;
let videoFullscreenHandlerBound = false;
let videoViewerDocumentHandlersBound = false;
let videoViewerShortcutsBound = false;
let autoplayNextEnabled = true;
const PLAYBACK_SPEED_KEY = 'nexara_playback_speed';
const VIDEO_QUALITY_KEY = 'nexara_video_quality';
const AUTO_CAPTIONS_KEY = 'nexara_captions_mode';
function handleVideoFullscreenChange() {
    videoFullscreenPending = false;
    isFullscreenChanging = false;
    const viewer = document.querySelector('.video-viewer-player');
    if (viewer) {
        viewer.classList.toggle('is-fullscreen', Boolean(document.fullscreenElement));
    }
    if (!document.fullscreenElement && pendingViewerCleanup) {
        const cleanup = pendingViewerCleanup;
        pendingViewerCleanup = null;
        cleanup();
    }
}

function ensureVideoFullscreenHandler(target) {
    if (target) videoFullscreenTarget = target;
    if (videoFullscreenHandlerBound) return;
    videoFullscreenHandlerBound = true;
    document.addEventListener('fullscreenchange', function () {
        if (videoFullscreenPending || isFullscreenChanging) {
            requestAnimationFrame(handleVideoFullscreenChange);
            return;
        }
        handleVideoFullscreenChange();
    });
}

function togglePlayerFullscreen(container) {
    if (!container || videoFullscreenPending || isFullscreenChanging) return;
    ensureVideoFullscreenHandler(container);
    videoFullscreenPending = true;
    isFullscreenChanging = true;
    if (document.fullscreenElement) {
        document.exitFullscreen?.().catch(function () {
            videoFullscreenPending = false;
            isFullscreenChanging = false;
        });
        return;
    }
    container.requestFullscreen?.().catch(function () {
        videoFullscreenPending = false;
        isFullscreenChanging = false;
    });
}

function requestExitFullscreen() {
    if (document.fullscreenElement && !isFullscreenChanging) {
        isFullscreenChanging = true;
        document.exitFullscreen?.().catch(function () {
            isFullscreenChanging = false;
        });
        return true;
    }
    return false;
}

function isVideoViewerActive() {
    return document.body.classList.contains('video-viewer-open') || document.body.classList.contains('video-viewer-inline-open');
}

function bindVideoViewerShortcuts() {
    if (videoViewerShortcutsBound) return;
    videoViewerShortcutsBound = true;
    document.addEventListener('keydown', function (event) {
        if (!isVideoViewerActive()) return;
        const tag = event.target?.tagName?.toLowerCase();
        if (tag === 'input' || tag === 'textarea' || event.target?.isContentEditable) return;
        const player = getVideoModalPlayer();
        if (!player) return;
        if (event.code === 'Space') {
            event.preventDefault();
            if (player.paused) player.play().catch(function () {});
            else player.pause();
        }
        if (event.key === 'k' || event.key === 'K') {
            if (player.paused) player.play().catch(function () {});
            else player.pause();
        }
        if (event.key === 'm' || event.key === 'M') {
            player.muted = !player.muted;
        }
        if (event.key === 'f' || event.key === 'F') {
            togglePlayerFullscreen(document.getElementById('video-player-frame') || player);
        }
        if (event.key === 'ArrowRight') {
            player.currentTime = Math.min(player.duration || player.currentTime + 5, (player.currentTime || 0) + 5);
        }
        if (event.key === 'ArrowLeft') {
            player.currentTime = Math.max(0, (player.currentTime || 0) - 5);
        }
        if (event.shiftKey && event.key === ',') {
            const next = Math.max(0.25, (player.playbackRate || 1) - 0.05);
            player.playbackRate = Number(next.toFixed(2));
            localStorage.setItem(PLAYBACK_SPEED_KEY, String(player.playbackRate));
            toast(`Speed: ${player.playbackRate.toFixed(2)}×`, 'info');
            updateSettingsPopoverSelection(document.querySelector('[data-popover="settings"]'));
        }
        if (event.shiftKey && event.key === '.') {
            const next = Math.min(3, (player.playbackRate || 1) + 0.05);
            player.playbackRate = Number(next.toFixed(2));
            localStorage.setItem(PLAYBACK_SPEED_KEY, String(player.playbackRate));
            toast(`Speed: ${player.playbackRate.toFixed(2)}×`, 'info');
            updateSettingsPopoverSelection(document.querySelector('[data-popover="settings"]'));
        }
    });
}

function bindVideoViewerControls() {
    const player = getVideoModalPlayer();
    const viewer = document.querySelector('.video-viewer-player');
    const playerFrame = document.getElementById('video-player-frame');
    const playBtn = document.getElementById('video-control-play');
    const scrub = document.getElementById('video-control-scrub');
    const volumeBtn = document.getElementById('video-control-volume');
    const volumeRange = document.getElementById('video-control-volume-range');
    const volumeGroup = document.getElementById('video-control-volume-group');
    const volumePopover = volumeGroup?.querySelector('.video-volume-popover');
    const captionsBtn = document.getElementById('video-control-captions');
    const settingsBtn = document.getElementById('video-control-settings');
    const spinner = document.getElementById('video-player-spinner');
    const theaterBtn = document.getElementById('video-control-theater');
    const fullscreenBtn = document.getElementById('video-control-fullscreen');
    if (!player || !playBtn || !scrub || !volumeBtn || !volumeRange || playBtn.dataset.bound) return;

    playBtn.dataset.bound = 'true';
    player.controls = false;
    player.removeAttribute('controls');
    applyVideoSettings(player);
    debugVideo('bind-controls');
    let lastVolumeLevel = player.volume || 1;
    if (volumeRange) {
        volumeRange.value = Math.round((player.volume || 1) * 100).toString();
    }
    if (volumeBtn) {
        volumeBtn.innerHTML = (player.muted || player.volume === 0)
            ? '<i class="ph ph-speaker-slash"></i>'
            : '<i class="ph ph-speaker-high"></i>';
    }

    let controlsTimeout;
    const showControls = function (force = false) {
        if (viewer) viewer.classList.add('controls-active');
        if (controlsTimeout) {
            window.clearTimeout(controlsTimeout);
        }
        if (!force && player && !player.paused) {
            controlsTimeout = window.setTimeout(function () {
                viewer?.classList.remove('controls-active');
            }, 2200);
        }
    };
    const hideControls = function () {
        if (player && !player.paused) {
            viewer?.classList.remove('controls-active');
        }
    };

    if (viewer) {
        viewer.addEventListener('mousemove', function () { showControls(); });
        viewer.addEventListener('mouseenter', function () { showControls(); });
        viewer.addEventListener('mouseleave', function () { hideControls(); });
    }

    const showSpinner = function () { spinner?.classList.add('is-active'); };
    const hideSpinner = function () { spinner?.classList.remove('is-active'); };
    player.addEventListener('loadstart', showSpinner);
    player.addEventListener('waiting', showSpinner);
    player.addEventListener('seeking', showSpinner);
    player.addEventListener('playing', function () {
        hideSpinner();
        showControls();
    });
    player.addEventListener('canplay', hideSpinner);
    player.addEventListener('canplaythrough', hideSpinner);
    player.addEventListener('loadeddata', hideSpinner);
    playBtn.addEventListener('click', function () {
        if (player.paused) player.play();
        else player.pause();
    });
    const updateVolumeIcon = function () {
        volumeBtn.innerHTML = (player.muted || player.volume === 0)
            ? '<i class="ph ph-speaker-slash"></i>'
            : '<i class="ph ph-speaker-high"></i>';
    };
    player.addEventListener('loadedmetadata', function () {
        scrub.max = Math.floor(player.duration || 0).toString() || '0';
        scrub.value = '0';
        updateVideoScrubVisuals(player, scrub);
        showControls(true);
    });
    player.addEventListener('play', function () {
        updateVideoControlPlayState(player, playBtn);
        showControls();
    });
    player.addEventListener('pause', function () {
        updateVideoControlPlayState(player, playBtn);
        showControls(true);
    });
    player.addEventListener('timeupdate', function () {
        if (!scrub.max || Number(scrub.max) === 100) {
            scrub.max = Math.floor(player.duration || 0).toString() || '0';
        }
        scrub.value = Math.floor(player.currentTime || 0).toString();
        updateVideoScrubVisuals(player, scrub);
    });
    player.addEventListener('progress', function () {
        updateVideoScrubVisuals(player, scrub);
    });
    player.addEventListener('durationchange', function () {
        if (player.duration) {
            scrub.max = Math.floor(player.duration || 0).toString() || '0';
        }
        updateVideoScrubVisuals(player, scrub);
    });
    scrub.addEventListener('input', function () {
        if (!player.duration) return;
        player.currentTime = Number(scrub.value) || 0;
        updateVideoScrubVisuals(player, scrub);
    });
    volumeBtn.addEventListener('click', function () {
        if (player.muted) {
            player.muted = false;
            const restored = lastVolumeLevel > 0 ? lastVolumeLevel : 0.6;
            player.volume = restored;
            volumeRange.value = Math.round(restored * 100).toString();
        } else {
            if (player.volume > 0) {
                lastVolumeLevel = player.volume;
            }
            player.muted = true;
            volumeRange.value = '0';
        }
        updateVolumeIcon();
    });
    volumeRange.addEventListener('input', function () {
        player.volume = Math.min(1, Math.max(0, Number(volumeRange.value) / 100));
        if (player.volume > 0) {
            lastVolumeLevel = player.volume;
            if (player.muted) player.muted = false;
        } else {
            player.muted = true;
        }
        updateVolumeIcon();
    });
    player.addEventListener('volumechange', function () {
        if (!player.muted && player.volume > 0) {
            lastVolumeLevel = player.volume;
        }
        updateVolumeIcon();
    });
    if (volumeGroup) {
        let hideTimeout;
        const showVolume = function () {
            if (hideTimeout) clearTimeout(hideTimeout);
            volumeGroup.classList.add('show-volume-slider');
        };
        const scheduleHide = function () {
            if (hideTimeout) clearTimeout(hideTimeout);
            hideTimeout = setTimeout(function () {
                volumeGroup.classList.remove('show-volume-slider');
            }, 300);
        };
        volumeGroup.addEventListener('mouseenter', showVolume);
        volumeGroup.addEventListener('mouseleave', scheduleHide);
        volumeGroup.addEventListener('focusin', showVolume);
        volumeGroup.addEventListener('focusout', scheduleHide);
        if (volumePopover) {
            volumePopover.addEventListener('mouseenter', showVolume);
            volumePopover.addEventListener('mouseleave', scheduleHide);
        }
    }
    if (theaterBtn) {
        theaterBtn.addEventListener('click', function () { window.toggleVideoTheaterMode?.(); });
    }
    if (fullscreenBtn) {
        fullscreenBtn.addEventListener('click', function () {
            togglePlayerFullscreen(playerFrame || player);
        });
    }
    if (captionsBtn || settingsBtn) {
        const setPopoverOpen = function (group, open) {
            if (!group) return;
            group.classList.toggle('is-open', open);
        };
        const popoverGroups = Array.from(document.querySelectorAll('.video-control-popover-group'));
        popoverGroups.forEach(function (group) {
            if (group.dataset.bound === 'true') return;
            group.dataset.bound = 'true';
            const button = group.querySelector('button');
            if (!button) return;
            button.addEventListener('click', function (event) {
                event.stopPropagation();
                const shouldOpen = !group.classList.contains('is-open');
                popoverGroups.forEach(function (entry) { setPopoverOpen(entry, false); });
                setPopoverOpen(group, shouldOpen);
                updateSettingsPopoverSelection(group);
            });
        });
        if (!videoViewerDocumentHandlersBound) {
            videoViewerDocumentHandlersBound = true;
            document.addEventListener('click', function (event) {
                const groups = Array.from(document.querySelectorAll('.video-control-popover-group'));
                if (groups.some(function (group) { return group.contains(event.target); })) return;
                groups.forEach(function (group) { group.classList.remove('is-open'); });
            });
        }
    }
    if (captionsBtn) {
        const captionsGroup = captionsBtn.closest('.video-control-popover-group');
        const buildCaptionsMenu = function () {
            if (!captionsGroup) return;
            const currentMode = getStoredCaptionsMode();
            const popover = captionsGroup.querySelector('.video-control-captions-popover');
            if (!popover) return;
            const emptyNote = popover.querySelector('#video-captions-empty');
            const select = popover.querySelector('#video-captions-select');
            if (!select) return;
            const baseOptions = Array.from(select.querySelectorAll('option[data-caption="track"]'));
            baseOptions.forEach(function (option) { option.remove(); });
            const tracks = Array.from(player.textTracks || []);
            tracks.forEach(function (track) {
                if (!track.label || track.label === 'Auto') return;
                const option = document.createElement('option');
                option.value = `track:${track.label}`;
                option.textContent = track.label;
                option.dataset.caption = 'track';
                select.appendChild(option);
            });
            if (emptyNote) {
                const hasTracks = tracks.some(function (track) { return track.label && track.label !== 'Auto'; });
                emptyNote.textContent = (!hasTracks && currentMode === 'off') ? 'No subtitles' : '';
            }
            select.value = currentMode;
            const autoOption = select.querySelector('option[value="auto"]');
            const autoSupported = !!(player.captureStream && (window.SpeechRecognition || window.webkitSpeechRecognition));
            if (autoOption) {
                autoOption.disabled = !autoSupported;
                if (!autoSupported) autoOption.textContent = 'Auto-generated (unsupported)';
                else autoOption.textContent = 'Auto-generated';
            }
            if (select.value === 'auto' && !autoSupported) {
                select.value = 'off';
                setStoredCaptionsMode('off');
            }
        };
        const select = captionsGroup?.querySelector('#video-captions-select');
        if (select && !select.dataset.bound) {
            select.dataset.bound = 'true';
            select.addEventListener('change', function () {
                const mode = select.value;
                setStoredCaptionsMode(mode);
                applyCaptionsMode(player, mode);
                const message = mode === 'off' ? 'Captions off' : mode === 'auto' ? 'Captions: Auto' : `Captions: ${mode.replace('track:', '')}`;
                toast(message, 'info');
                buildCaptionsMenu();
            });
        }
        captionsBtn.addEventListener('click', function () {
            buildCaptionsMenu();
        });
    }
    if (settingsBtn) {
        const settingsGroup = settingsBtn.closest('.video-control-popover-group');
        const speedInput = settingsGroup?.querySelector('#video-settings-speed');
        const speedValue = settingsGroup?.querySelector('#video-settings-speed-value');
        const qualityButtons = settingsGroup?.querySelectorAll('[data-quality]') || [];
        if (speedInput && speedValue) {
            const storedSpeed = getStoredPlaybackSpeed();
            speedInput.value = String(storedSpeed);
            speedValue.textContent = `${storedSpeed.toFixed(2)}×`;
            speedInput.addEventListener('input', function () {
                const speed = Number(speedInput.value) || 1;
                localStorage.setItem(PLAYBACK_SPEED_KEY, String(speed));
                player.playbackRate = speed;
                speedValue.textContent = `${speed.toFixed(2)}×`;
                toast(`Speed: ${speed.toFixed(2)}×`, 'info');
            });
        }
        qualityButtons.forEach(function (btn) {
            btn.addEventListener('click', function () {
                const quality = btn.dataset.quality || 'auto';
                localStorage.setItem(VIDEO_QUALITY_KEY, quality);
                updateSettingsPopoverSelection(settingsGroup);
            });
        });
        if (!localStorage.getItem(VIDEO_QUALITY_KEY)) {
            localStorage.setItem(VIDEO_QUALITY_KEY, resolveDefaultQuality());
        }
        updateSettingsPopoverSelection(settingsGroup);
    }
    const autoplayToggle = document.getElementById('video-up-next-autoplay');
    if (autoplayToggle) {
        autoplayToggle.checked = autoplayNextEnabled;
        autoplayToggle.addEventListener('change', function () {
            autoplayNextEnabled = autoplayToggle.checked;
        });
    }
    player.addEventListener('ended', function () {
        if (!autoplayNextEnabled) return;
        const list = document.getElementById('video-up-next-list');
        const nextId = list?.querySelector('.video-up-next-item')?.dataset?.videoId;
        if (nextId) window.openVideoDetail(nextId);
    });
    ensureVideoFullscreenHandler(playerFrame || player);
    bindVideoViewerShortcuts();
    showControls(true);
}

window.toggleVideoTheaterMode = function () {
    const modal = document.getElementById('video-detail-modal');
    if (!modal) return;
    modal.classList.toggle('video-theater');
};

function renderVideoUpNextList(currentVideoId) {
    const list = document.getElementById('video-up-next-list');
    if (!list) return;
    const options = arguments.length > 1 && typeof arguments[1] === 'object' ? arguments[1] : {};
    const limit = Number(options.limit) || 10;
    const append = options.append === true;
    if (!append) {
        list.innerHTML = '';
        list.dataset.videoId = currentVideoId || '';
        list.dataset.ids = '[]';
        list.dataset.offset = '0';
        list.setAttribute('aria-label', 'Up next recommendations');
    }
    const storedIds = new Set(JSON.parse(list.dataset.ids || '[]'));
    const suggestions = videosCache
        .filter(function (video) { return video.id !== currentVideoId && !storedIds.has(video.id); })
        .slice(0, limit);
    if (!suggestions.length && !append) {
        list.innerHTML = '<div class="empty-state">No recommendations yet.</div>';
        return;
    }
    suggestions.forEach(function (video) {
        const item = document.createElement('button');
        item.type = 'button';
        item.className = 'video-up-next-item';
        item.dataset.videoId = video.id;
        item.onclick = function () { window.openVideoDetail(video.id); };
        item.innerHTML = `
            <div class="video-up-next-thumb" style="background-image:url('${resolveVideoThumbnail(video)}')"></div>
            <div class="video-up-next-meta">
                <div class="video-up-next-title">${escapeHtml(video.title || video.caption || 'Untitled video')}</div>
                <div class="video-up-next-channel">${escapeHtml((getCachedUser(video.ownerId)?.displayName || getCachedUser(video.ownerId)?.name || getCachedUser(video.ownerId)?.username || 'Nexera Creator'))}</div>
                <div class="video-up-next-stats">${formatCompactNumber(getVideoViewCount(video))} views • ${formatVideoTimestamp(video.createdAt)}</div>
            </div>
        `;
        list.appendChild(item);
        storedIds.add(video.id);
    });
    list.dataset.ids = JSON.stringify(Array.from(storedIds));
    updateVideoUpNextHeight();
}

function bindUpNextLazyLoad(currentVideoId) {
    const list = document.getElementById('video-up-next-list');
    if (!list || list.dataset.scrollBound === 'true') return;
    list.dataset.scrollBound = 'true';
    list.addEventListener('scroll', function () {
        if (list.dataset.loading === 'true') return;
        if (list.scrollTop + list.clientHeight < list.scrollHeight - 12) return;
        list.dataset.loading = 'true';
        renderVideoUpNextList(currentVideoId, { limit: 8, append: true });
        list.dataset.loading = 'false';
    });
}

let upNextResizeBound = false;
let upNextResizeFrame = null;
function updateVideoUpNextHeight() {
    const list = document.getElementById('video-up-next-list');
    const comments = document.querySelector('.video-viewer-comments');
    if (!list || !comments) return;
    const listRect = list.getBoundingClientRect();
    const commentsRect = comments.getBoundingClientRect();
    const available = Math.max(200, Math.round(commentsRect.bottom - listRect.top));
    if (available > 0) {
        list.style.maxHeight = `${available}px`;
    }
}

function bindVideoUpNextHeight() {
    if (upNextResizeBound) return;
    upNextResizeBound = true;
    window.addEventListener('resize', function () {
        if (upNextResizeFrame) cancelAnimationFrame(upNextResizeFrame);
        upNextResizeFrame = requestAnimationFrame(function () {
            upNextResizeFrame = null;
            updateVideoUpNextHeight();
        });
    });
}

function renderVideoCommentsPlaceholder(videoId) {
    const list = document.getElementById('video-comments-list');
    if (!list) return;
    list.innerHTML = '';
    const samples = [
        { id: `${videoId}-c1`, name: 'Nexera Viewer', text: 'Great breakdown—thanks for sharing!' },
        { id: `${videoId}-c2`, name: 'Community', text: 'Can we get a follow-up on this topic?' }
    ];
    samples.forEach(function (sample) {
        const item = document.createElement('div');
        item.className = 'video-comment';
        item.innerHTML = `
            <div class="video-comment-avatar"></div>
            <div class="video-comment-body">
                <div class="video-comment-author">${escapeHtml(sample.name)}</div>
                <div class="video-comment-text">${escapeHtml(sample.text)}</div>
                <div class="video-comment-actions">
                    <button class="icon-pill" onclick="window.handleUiStubAction?.('comment-like')"><i class="ph ph-thumbs-up"></i></button>
                    <button class="icon-pill" onclick="window.handleUiStubAction?.('comment-reply')"><i class="ph ph-chat-centered"></i></button>
                    <button class="icon-pill" onclick="window.handleUiStubAction?.('comment-more')"><i class="ph ph-dots-three"></i></button>
                </div>
            </div>
        `;
        list.appendChild(item);
    });
}

function captureVideoDetailReturnPath() {
    const path = window.location.pathname || '/';
    const search = window.location.search || '';
    const hash = window.location.hash || '';
    if (path.startsWith('/video/')) {
        videoDetailReturnPath = '/videos';
        return;
    }
    if (path.startsWith('/videos')) {
        videoDetailReturnPath = '/videos';
        return;
    }
    videoDetailReturnPath = `${path}${search}${hash}` || '/videos';
}

function getVideoRouteVideoId() {
    const url = new URL(window.location.href);
    if (url.pathname.startsWith('/videos/')) {
        const raw = url.pathname.replace('/videos/', '').split('/')[0];
        return raw ? decodeURIComponent(raw) : null;
    }
    if (url.pathname.startsWith('/video/')) {
        const raw = url.pathname.replace('/video/', '').split('/')[0];
        return raw ? decodeURIComponent(raw) : null;
    }
    const queryId = url.searchParams.get('open') || url.searchParams.get('video') || url.searchParams.get('v');
    if (queryId) return queryId;
    if (url.hash && url.hash.startsWith('#video=')) {
        return url.hash.replace('#video=', '');
    }
    return null;
}

function clearVideoDetailRoute() {
    const url = new URL(window.location.href);
    const fallback = videoDetailReturnPath || '/videos';
    if (url.pathname.startsWith('/videos')) {
        url.pathname = '/videos';
        url.searchParams.delete('open');
        url.searchParams.delete('video');
        url.searchParams.delete('v');
        if (url.hash && url.hash.startsWith('#video')) url.hash = '';
        const next = `${url.pathname}${url.search}${url.hash}`;
        if (window.NexeraRouter?.replaceStateSilently) {
            window.NexeraRouter.replaceStateSilently(next);
        } else {
            history.replaceState({}, '', next);
        }
        return;
    }
    if (window.NexeraRouter?.replaceStateSilently) {
        window.NexeraRouter.replaceStateSilently(fallback);
    } else {
        history.replaceState({}, '', fallback);
    }
}

function clearVideoDetailState({ updateRoute = false } = {}) {
    const player = getVideoModalPlayer();
    if (miniPlayerMode === 'pip' && document.pictureInPictureElement === player) {
        document.exitPictureInPicture?.().catch(function () {});
    }
    miniPlayerMode = null;
    miniPlayerState = null;
    videoModalResumeTime = null;
    profileReturnContext = null;
    hideMiniPlayer({ stopPlayback: true });
    closeVideoDetailModalHandler({ keepPlayback: false });
    if (updateRoute) clearVideoDetailRoute();
}

function closeVideoDetailModalHandler(options = {}) {
    const { keepPlayback = false } = options;
    const modal = document.getElementById('video-detail-modal');
    if (!modal) return;
    const performCleanup = function () {
        // Viewer lifecycle: stop playback + restore grid state on close.
        debugVideo('close', { keepPlayback });
        const player = getVideoModalPlayer();
        if (player && !keepPlayback) {
            player.pause();
            player.removeAttribute('src');
            player.load();
        }
        delete modal.dataset.videoId;
        modal.style.display = 'none';
        modal.classList.remove('video-theater');
        document.querySelector('.video-viewer-player')?.classList.remove('is-fullscreen');
        videoFullscreenTarget = null;
        videoFullscreenPending = false;
        document.body.classList.remove('modal-open');
        document.body.classList.remove('video-viewer-open');
        const spinner = document.getElementById('video-player-spinner');
        if (spinner) spinner.classList.remove('is-active');
        restoreVideoFeedFromModal('video-detail-modal');
        if (lastVideoTrigger && typeof lastVideoTrigger.focus === 'function') {
            lastVideoTrigger.focus();
        }
    };
    if (document.fullscreenElement || isFullscreenChanging) {
        pendingViewerCleanup = performCleanup;
        if (isFullscreenChanging) {
            return;
        }
        if (requestExitFullscreen()) {
            return;
        }
    }
    performCleanup();
}

const closeVideoDetailModal = closeVideoDetailModalHandler;
window.closeVideoDetail = function () {
    if (USE_INLINE_VIDEO_WATCH) {
        restoreInlineVideoWatch();
        return;
    }
    if (USE_INLINE_VIDEO_VIEWER && restoreInlineVideoViewer()) {
        clearVideoDetailRoute();
        clearVideoDetailState({ updateRoute: false });
        return;
    }
    clearVideoDetailState({ updateRoute: true });
};

function getMiniPlayerElements() {
    return {
        container: document.getElementById('video-mini-player'),
        slot: document.getElementById('video-mini-player-slot'),
        status: document.getElementById('video-mini-player-status'),
        indicator: document.getElementById('video-mini-player-indicator')
    };
}

function shouldHideMiniPlayer(viewId) {
    return viewId === 'live' || viewId === 'live-setup';
}

function updateMiniPlayerIndicator(player) {
    const { status, indicator } = getMiniPlayerElements();
    if (!status || !indicator || !player) return;
    const playing = !player.paused && !player.ended;
    status.textContent = playing ? 'Playing' : 'Paused';
    indicator.classList.toggle('is-playing', playing);
    indicator.classList.toggle('is-paused', !playing);
}

function bindMiniPlayerEvents(player) {
    if (!player || player.dataset.miniPlayerBound) return;
    player.dataset.miniPlayerBound = 'true';
    player.addEventListener('timeupdate', function () {
        if (miniPlayerState) miniPlayerState.currentTime = player.currentTime || 0;
    });
    player.addEventListener('play', function () { updateMiniPlayerIndicator(player); });
    player.addEventListener('pause', function () { updateMiniPlayerIndicator(player); });
    player.addEventListener('ended', function () { updateMiniPlayerIndicator(player); });
}

function moveVideoPlayerTo(container) {
    const player = getVideoModalPlayer();
    if (!player || !container) return;
    if (player.parentElement !== container) {
        if (container.firstChild) {
            container.insertBefore(player, container.firstChild);
        } else {
            container.appendChild(player);
        }
    }
}

function showMiniPlayer() {
    const { container, slot } = getMiniPlayerElements();
    if (!container || !slot) return;
    if (!miniPlayerState?.videoId) return;
    if (miniPlayerMode !== 'dock') return;
    if (shouldHideMiniPlayer(currentViewId)) return;
    moveVideoPlayerTo(slot);
    const player = getVideoModalPlayer();
    bindMiniPlayerEvents(player);
    updateMiniPlayerIndicator(player);
    container.style.display = 'flex';
}

function hideMiniPlayer({ stopPlayback = true } = {}) {
    const { container } = getMiniPlayerElements();
    const player = getVideoModalPlayer();
    if (container) container.style.display = 'none';
    if (!player) return;
    if (stopPlayback) {
        player.pause();
        player.removeAttribute('src');
        player.load();
    }
    if (stopPlayback || player.closest('#video-mini-player')) {
        moveVideoPlayerTo(getVideoModalPlayerContainer());
    }
}

window.closeMiniPlayer = function () {
    const player = getVideoModalPlayer();
    if (document.pictureInPictureElement === player) {
        document.exitPictureInPicture?.().catch(function () {});
    }
    miniPlayerMode = null;
    miniPlayerState = null;
    hideMiniPlayer({ stopPlayback: true });
};

window.expandMiniPlayer = function () {
    restoreModalFromMiniPlayer();
};

async function maybeEnterPictureInPicture(player) {
    if (!player) return false;
    if (!document.pictureInPictureEnabled || player.disablePictureInPicture) return false;
    try {
        await player.requestPictureInPicture();
        return true;
    } catch (err) {
        console.warn('Picture-in-Picture failed', err);
        return false;
    }
}

async function minimizeVideoDetail({ updateRoute = true, preferPiP = true } = {}) {
    const modal = document.getElementById('video-detail-modal');
    const player = getVideoModalPlayer();
    if (!modal || !player) return;
    const videoId = modal.dataset.videoId;
    if (!videoId) {
        closeVideoDetailModalHandler();
        return;
    }
    miniPlayerState = { videoId, currentTime: player.currentTime || 0 };
    closeVideoDetailModalHandler({ keepPlayback: true });

    if (updateRoute) {
        const nextPath = videoDetailReturnPath || '/videos';
        if (window.location.pathname !== nextPath) {
            window.NexeraRouter?.replaceStateSilently?.(nextPath);
        }
    }

    if (preferPiP) {
        const pipActive = await maybeEnterPictureInPicture(player);
        if (pipActive) {
            miniPlayerMode = 'pip';
            return;
        }
    }
    miniPlayerMode = 'dock';
    showMiniPlayer();
}

function transitionModalToMiniPlayer() {
    minimizeVideoDetail({ updateRoute: false });
}

function restoreModalFromMiniPlayer() {
    if (!miniPlayerState) return;
    const { videoId } = miniPlayerState;
    const player = getVideoModalPlayer();
    if (miniPlayerMode === 'pip' && document.pictureInPictureElement === player) {
        document.exitPictureInPicture?.().catch(function () {});
    }
    miniPlayerMode = null;
    hideMiniPlayer({ stopPlayback: false });
    miniPlayerState = null;
    window.openVideoDetail(videoId);
}

document.addEventListener('leavepictureinpicture', function () {
    if (miniPlayerMode === 'pip' && miniPlayerState && !document.pictureInPictureElement) {
        miniPlayerMode = 'dock';
        showMiniPlayer();
    }
});

function normalizeProfileLinks(rawLinks) {
    if (!Array.isArray(rawLinks)) return [];
    return rawLinks.map(function (link) {
        if (!link) return null;
        if (typeof link === 'string') {
            return { label: link, url: link };
        }
        if (typeof link === 'object') {
            return { label: link.label || link.url || '', url: link.url || '' };
        }
        return null;
    }).filter(Boolean).map(function (link) {
        let url = (link.url || '').trim();
        if (!url) return null;
        if (!/^https?:\/\//i.test(url)) {
            url = `https://${url}`;
        }
        if (!/^https?:\/\//i.test(url)) return null;
        let label = (link.label || '').trim();
        if (!label) {
            try {
                label = new URL(url).hostname.replace(/^www\./, '');
            } catch (err) {
                label = url;
            }
        }
        return { label, url };
    }).filter(Boolean);
}

function renderProfileLinks(links = []) {
    if (!links.length) return '';
    return links.map(function (link) {
        const safeLabel = escapeHtml(link.label || link.url || '');
        const safeUrl = escapeHtml(link.url || '');
        return `<a class="video-modal-link-chip" href="${safeUrl}" target="_blank" rel="noopener" onclick="event.stopPropagation()">${safeLabel}</a>`;
    }).join('');
}

window.openVideoDetail = async function (videoId) {
    let modal = document.getElementById('video-detail-modal');
    const video = getVideoById(videoId);
    if (!video) return;

    if (USE_INLINE_VIDEO_WATCH) {
        await openInlineVideoWatch(video);
        return;
    }
    if (USE_INLINE_VIDEO_VIEWER) {
        mountInlineVideoViewer();
    }
    modal = document.getElementById('video-detail-modal');
    if (!modal) return;

    // Viewer lifecycle: store focus + bind controls on open for custom viewer.
    lastVideoTrigger = document.activeElement;
    debugVideo('open', { videoId });
    captureVideoDetailReturnPath();
    if (!USE_INLINE_VIDEO_VIEWER) {
        initVideoViewerLayout();
    }
    if (USE_CUSTOM_VIDEO_VIEWER) {
        bindVideoViewerControls();
    }
    document.body.classList.add('video-viewer-open');

    const spinner = document.getElementById('video-player-spinner');
    if (spinner) spinner.classList.add('is-active');

    const player = getVideoModalPlayer();
    const title = document.getElementById('video-modal-title');
    const description = document.getElementById('video-modal-description');
    const avatar = document.getElementById('video-modal-avatar');
    const channelName = document.getElementById('video-modal-channel-name');
    const channelHandle = document.getElementById('video-modal-channel-handle');
    const channelCard = document.getElementById('video-modal-channel-card');
    const channelBio = document.getElementById('video-modal-channel-bio');
    const channelLinks = document.getElementById('video-modal-channel-links');
    const followBtn = document.getElementById('video-modal-follow');
    const likeBtn = document.getElementById('video-modal-like');
    const dislikeBtn = document.getElementById('video-modal-dislike');
    const saveBtn = document.getElementById('video-modal-save');
    const shareBtn = document.getElementById('video-modal-share');

    modal.dataset.videoId = video.id;
    ensureVideoStats(video);
    renderVideoUpNextList(video.id, { limit: 10 });
    bindUpNextLazyLoad(video.id);
    renderVideoCommentsPlaceholder(video.id);
    bindVideoUpNextHeight();
    requestAnimationFrame(updateVideoUpNextHeight);

    if (miniPlayerState && miniPlayerState.videoId !== video.id) {
        window.closeMiniPlayer();
    }
    if (miniPlayerState && miniPlayerState.videoId === video.id) {
        if (miniPlayerMode === 'pip' && document.pictureInPictureElement === player) {
            await document.exitPictureInPicture?.().catch(function () {});
        }
        miniPlayerMode = null;
        hideMiniPlayer({ stopPlayback: false });
        miniPlayerState = null;
        moveVideoPlayerTo(getVideoModalPlayerContainer());
    }

    if (player) {
        player.preload = 'metadata';
        player.setAttribute('preload', 'metadata');
        const videoSrc = video.videoURL || '';
        const currentSrc = player.currentSrc || player.src || '';
        const shouldReset = videoSrc && currentSrc !== videoSrc;
        applyVideoCaptions(player, video);
        applyVideoSettings(player);
        applyCaptionsMode(player, getStoredCaptionsMode());
        if (shouldReset) {
            player.src = videoSrc;
            player.onloadedmetadata = function () {
                if (typeof videoModalResumeTime === 'number') {
                    try {
                        player.currentTime = videoModalResumeTime;
                    } catch (err) {
                        console.warn('Unable to resume video time', err);
                    } finally {
                        videoModalResumeTime = null;
                    }
                }
            };
            player.load();
            player.autoplay = true;
            player.controls = false;
            player.removeAttribute('controls');
            player.play().catch(function () {});
        } else if (typeof videoModalResumeTime === 'number') {
            try {
                player.currentTime = videoModalResumeTime;
            } catch (err) {
                console.warn('Unable to resume video time', err);
            } finally {
                videoModalResumeTime = null;
            }
        } else {
            player.autoplay = true;
            player.controls = false;
            player.removeAttribute('controls');
            player.play().catch(function () {});
        }
    }

    const author = await resolveUserProfile(video.ownerId || '');
    const authorDisplay = author?.displayName || author?.name || author?.username || 'Nexera Creator';
    const authorHandle = author?.username ? `@${author.username}` : 'Nexera';

    if (avatar) applyAvatarToElement(avatar, author || {}, { size: 44 });
    if (channelName) channelName.textContent = authorDisplay;
    if (channelHandle) channelHandle.textContent = authorHandle;
    if (channelBio) {
        const bioText = (author?.bio || '').trim();
        channelBio.textContent = bioText;
        channelBio.style.display = bioText ? 'block' : 'none';
    }
    if (channelLinks) {
        const links = normalizeProfileLinks(author?.links || []);
        channelLinks.innerHTML = renderProfileLinks(links);
        channelLinks.style.display = links.length ? 'flex' : 'none';
    }
    const videoTitle = video.title || video.caption || 'Untitled video';
    const videoDescription = video.description || '';
    const tagLine = Array.isArray(video.tags) && video.tags.length
        ? video.tags.map(function (tag) { return `<span class="tag-chip">#${escapeHtml(tag)}</span>`; }).join('')
        : '';
    const mentionLine = Array.isArray(video.mentions) && video.mentions.length
        ? video.mentions.map(function (m) {
            const username = typeof m === 'string' ? m : (m.username || m.handle || '');
            return username ? `<span class="tag-chip">@${escapeHtml(username)}</span>` : '';
        }).join('')
        : '';

    if (title) title.textContent = videoTitle;
    if (description) {
        const descText = videoDescription ? escapeHtml(videoDescription) : 'No description yet.';
        const metaLine = `${tagLine}${mentionLine ? ` ${mentionLine}` : ''}`;
        description.innerHTML = `${descText}${metaLine ? `<div style="margin-top:10px; display:flex; gap:6px; flex-wrap:wrap;">${metaLine}</div>` : ''}`;
    }

    if (followBtn) {
        followBtn.onclick = function (event) {
            event.stopPropagation();
            if (video.ownerId) window.toggleFollowUser(video.ownerId, event);
        };
        if (video.ownerId) {
            followBtn.classList.add(`js-follow-user-${video.ownerId}`);
            updateFollowButtonsForUser(video.ownerId, followedUsers.has(video.ownerId));
        } else {
            followBtn.textContent = 'Follow';
        }
        if (!currentUser?.uid) {
            followBtn.disabled = true;
            followBtn.title = 'Log in to follow';
        } else {
            followBtn.disabled = false;
            followBtn.title = '';
        }
    }

    if (video.ownerId) {
        if (avatar) {
            avatar.onclick = function (event) {
                event.stopPropagation();
                captureVideoProfileReturn(video.id);
                window.openUserProfile(video.ownerId, event);
            };
        }
        if (channelName) {
            channelName.onclick = function (event) {
                event.stopPropagation();
                captureVideoProfileReturn(video.id);
                window.openUserProfile(video.ownerId, event);
            };
        }
        if (channelHandle) {
            channelHandle.onclick = function (event) {
                event.stopPropagation();
                captureVideoProfileReturn(video.id);
                window.openUserProfile(video.ownerId, event);
            };
        }
        if (channelCard) {
            channelCard.onclick = function (event) {
                if (event.target.closest('button') || event.target.closest('a')) return;
                captureVideoProfileReturn(video.id);
                window.openUserProfile(video.ownerId, event);
            };
        }
    }

    if (likeBtn) {
        likeBtn.onclick = function (event) { event.stopPropagation(); window.likeVideo(video.id); };
    }
    if (dislikeBtn) {
        dislikeBtn.onclick = function (event) { event.stopPropagation(); window.dislikeVideo(video.id); };
    }
    if (saveBtn) {
        saveBtn.onclick = function (event) { event.stopPropagation(); window.saveVideo(video.id); };
    }
    if (shareBtn) {
        shareBtn.onclick = function (event) {
            event.stopPropagation();
            const shareUrl = video.videoURL || window.location.href;
            if (navigator.share) {
                navigator.share({ title: videoTitle, url: shareUrl }).catch(function () {});
                return;
            }
            navigator.clipboard?.writeText(shareUrl).then(function () {
                toast('Video link copied.', 'info');
            }).catch(function () {
                toast('Unable to copy link.', 'error');
            });
        };
    }

    await hydrateVideoEngagement(video.id);
    updateVideoModalButtons(video.id);

    if (modal && !USE_INLINE_VIDEO_VIEWER) {
        modal.style.display = 'flex';
        document.body.classList.add('modal-open');
    }
};

document.addEventListener('keydown', function (event) {
    if (event.key === 'Escape') window.closeVideoDetail();
});

window.uploadVideo = async function () {
    if (!requireAuth()) return;

    const fileInput = document.getElementById('video-file');
    if (!fileInput || !fileInput.files || !fileInput.files[0]) return;

    const submitBtn = document.getElementById('video-upload-submit');
    const thumbInput = document.getElementById('video-thumbnail');
    const title = document.getElementById('video-title')?.value || '';
    const description = document.getElementById('video-description')?.value || '';
    const tags = Array.from(new Set((videoTags || []).map(normalizeTagValue).filter(Boolean)));
    const mentions = normalizeMentionsField(videoMentions || []);
    const visibility = document.getElementById('video-visibility').value || 'public';
    const category = document.getElementById('video-category')?.value || VIDEO_UPLOAD_DEFAULTS.category;
    const categorySlug = videoPostingDestinationId && videoPostingDestinationId !== 'no-topic' ? videoPostingDestinationId : 'no-topic';
    const topic = resolveCategoryLabelBySlug(categorySlug) || 'No topic';
    const language = document.getElementById('video-language')?.value || VIDEO_UPLOAD_DEFAULTS.language;
    const license = document.getElementById('video-license')?.value || VIDEO_UPLOAD_DEFAULTS.license;
    const allowDownload = getVideoToggleValue('video-allow-download', VIDEO_UPLOAD_DEFAULTS.allowDownload);
    const allowEmbed = getVideoToggleValue('video-allow-embed', VIDEO_UPLOAD_DEFAULTS.allowEmbed);
    const allowComments = getVideoToggleValue('video-allow-comments', VIDEO_UPLOAD_DEFAULTS.allowComments);
    const notifyFollowers = getVideoToggleValue('video-notify-followers', VIDEO_UPLOAD_DEFAULTS.notifyFollowers);
    const ageRestricted = getVideoToggleValue('video-age-restricted', VIDEO_UPLOAD_DEFAULTS.ageRestricted);
    const containsSensitiveContent = getVideoToggleValue('video-sensitive-content', VIDEO_UPLOAD_DEFAULTS.containsSensitiveContent);
    const scheduledAt = parseVideoScheduleTimestamp();
    const file = fileInput.files[0];
    let uploadSession = null;
    let videoId = `${Date.now()}`;
    let storagePath = `videos/${currentUser.uid}/${videoId}`;
    const draftDuration = pendingVideoDurationSeconds;
    if (USE_UPLOAD_SESSION) {
        try {
            console.info('[VideoUpload] Requesting upload session');
            const createSession = httpsCallable(functions, 'createUploadSession');
            const result = await createSession({ size: file.size, type: file.type });
            uploadSession = result?.data || null;
            if (uploadSession?.uploadId) {
                videoId = uploadSession.uploadId;
                storagePath = uploadSession.storagePath || `videos/${currentUser.uid}/${videoId}`;
            }
            console.info('[VideoUpload] Upload session ready', uploadSession);
        } catch (err) {
            console.warn('[VideoUpload] Upload session failed, falling back to direct upload', err);
        }
    }

    const storageRef = ref(storage, `${storagePath}/source.mp4`);
    activeUploadId = videoId;
    window.activeUploadId = activeUploadId;
    const task = {
        id: videoId,
        title: title || 'Untitled video',
        status: 'Uploading',
        progress: 0,
        thumbnail: ''
    };
    uploadTasks = uploadTasks.filter(function (t) { return t.id !== videoId; }).concat(task);

    let previewBlob = null;
    if (thumbInput && thumbInput.files && thumbInput.files[0]) {
        previewBlob = thumbInput.files[0];
    } else if (pendingVideoThumbnailBlob) {
        previewBlob = pendingVideoThumbnailBlob;
    }
    if (previewBlob) {
        task.thumbnail = await blobToDataUrl(previewBlob);
    }
    renderUploadTasks();

    try {
        if (submitBtn) {
            submitBtn.disabled = true;
            submitBtn.innerHTML = `<span class="button-spinner" aria-hidden="true"></span> Publishing...`;
        }
        resetVideoUploadForm();
        toggleTaskViewer(true);
        console.info('[VideoUpload] Starting upload', { videoId, size: file.size, type: file.type });
        const uploadTask = uploadBytesResumable(storageRef, file);
        await new Promise(function (resolve, reject) {
            uploadTask.on('state_changed', function (snapshot) {
                const progress = snapshot.totalBytes ? Math.round((snapshot.bytesTransferred / snapshot.totalBytes) * 100) : 0;
                console.info('[VideoUpload] Progress', { videoId, progress });
                task.progress = progress;
                renderUploadTasks();
            }, function (error) {
                console.error('[VideoUpload] Upload failed', error);
                task.status = 'Failed';
                renderUploadTasks();
                reject(error);
            }, function () {
                console.info('[VideoUpload] Upload complete', { videoId });
                task.progress = 100;
                task.status = 'Processing';
                renderUploadTasks();
                resolve();
            });
        });

        console.info('[VideoUpload] Fetching video URL', { videoId });
        const videoURL = await getDownloadURL(uploadTask.snapshot.ref);
        console.info('[VideoUpload] Video URL ready', { videoId });
        let thumbURL = '';
        let thumbBlob = null;
        let hasCustomThumbnail = false;

        if (thumbInput && thumbInput.files && thumbInput.files[0]) {
            thumbBlob = thumbInput.files[0];
            hasCustomThumbnail = true;
        } else if (pendingVideoThumbnailBlob) {
            thumbBlob = pendingVideoThumbnailBlob;
            hasCustomThumbnail = pendingVideoHasCustomThumbnail;
        } else {
            thumbBlob = await generateThumbnailFromVideo(file);
            hasCustomThumbnail = false;
        }

        if (thumbBlob) {
            console.info('[VideoUpload] Uploading thumbnail', { videoId });
            const thumbRef = ref(storage, `${storagePath}/thumb.jpg`);
            await uploadBytes(thumbRef, thumbBlob);
            thumbURL = await getDownloadURL(thumbRef);
            console.info('[VideoUpload] Thumbnail ready', { videoId });
        }

        const scheduledVisibility = scheduledAt ? visibility : null;
        const effectiveVisibility = scheduledAt ? 'private' : visibility;
        const docData = {
            ownerId: currentUser.uid,
            title,
            caption: title,
            description,
            tags,
            mentions,
            duration: draftDuration || null,
            createdAt: serverTimestamp(),
            storagePath,
            videoURL,
            thumbURL,
            visibility: effectiveVisibility,
            scheduledVisibility,
            scheduledAt,
            hasCustomThumbnail,
            monetizable: false,
            allowDownload,
            allowEmbed,
            allowComments,
            notifyFollowers,
            ageRestricted,
            containsSensitiveContent,
            category,
            categorySlug,
            topic,
            language,
            license,
            stats: { likes: 0, comments: 0, saves: 0, views: 0 }
        };

        console.info('[VideoUpload] Writing Firestore doc', { videoId });
        await setDoc(doc(db, 'videos', videoId), docData);
        console.info('[VideoUpload] Done', { videoId });
        task.status = 'Ready';
        renderUploadTasks();
        videosCache = [{ id: videoId, ...docData }, ...videosCache];
    } catch (err) {
        console.error('Video upload failed', err);
        if (uploadManager && uploadManager.markFailed && currentUser?.uid && activeUploadId) {
            uploadManager.markFailed(currentUser.uid, activeUploadId);
        }
        if (activeUploadId) {
            try {
                await updateDoc(doc(db, 'videoUploads', activeUploadId), {
                    status: 'FAILED',
                    error: err?.message || 'Upload failed',
                    updatedAt: serverTimestamp()
                });
            } catch (error) {
                console.warn('[VideoUpload] Failed to update upload status', error);
            }
        }
        toast('Video upload failed. Please try again.', 'error');
    } finally {
        if (submitBtn) {
            submitBtn.disabled = false;
            submitBtn.textContent = 'Publish';
        }
    }
};

document.addEventListener('keydown', function (event) {
    if (event.key === 'Escape' && destinationPickerOpen) {
        closeDestinationPicker();
    }
});

window.likeVideo = async function (videoId) {
    if (!requireAuth()) return;
    if (!videoId) return;
    if (!videoEngagementHydrated.has(videoId)) await hydrateVideoEngagement(videoId);

    const state = getVideoEngagementStatus(videoId);
    const wasLiked = state.liked;
    const wasDisliked = state.disliked;
    const video = videosCache.find(function (entry) { return entry.id === videoId; });

    if (wasLiked) {
        setVideoEngagementStatus(videoId, { liked: false });
        updateVideoStats(videoId, { likes: -1 });
    } else {
        setVideoEngagementStatus(videoId, { liked: true, disliked: false });
        updateVideoStats(videoId, { likes: 1 });
        if (wasDisliked) updateVideoStats(videoId, { dislikes: -1 });
    }
    updateVideoModalButtons(videoId);

    const videoRef = doc(db, 'videos', videoId);
    const likeRef = doc(db, 'videos', videoId, 'likes', currentUser.uid);
    const dislikeRef = doc(db, 'videos', videoId, 'dislikes', currentUser.uid);

    try {
        if (wasLiked) {
            await Promise.all([
                deleteDoc(likeRef),
                updateDoc(videoRef, { 'stats.likes': increment(-1) })
            ]);
        } else {
            const writes = [
                setDoc(likeRef, { createdAt: serverTimestamp() }),
                updateDoc(videoRef, { 'stats.likes': increment(1) })
            ];
            if (wasDisliked) {
                writes.push(deleteDoc(dislikeRef));
                writes.push(updateDoc(videoRef, { 'stats.dislikes': increment(-1) }));
            }
            await Promise.all(writes);
        }
    } catch (err) {
        console.error('Video like failed', err);
        await hydrateVideoEngagement(videoId);
        await refreshVideoStatsFromServer(videoId);
        updateVideoModalButtons(videoId);
    }
};

window.dislikeVideo = async function (videoId) {
    if (!requireAuth()) return;
    if (!videoId) return;
    if (!videoEngagementHydrated.has(videoId)) await hydrateVideoEngagement(videoId);

    const state = getVideoEngagementStatus(videoId);
    const wasDisliked = state.disliked;
    const wasLiked = state.liked;

    if (wasDisliked) {
        setVideoEngagementStatus(videoId, { disliked: false });
        updateVideoStats(videoId, { dislikes: -1 });
    } else {
        setVideoEngagementStatus(videoId, { disliked: true, liked: false });
        updateVideoStats(videoId, { dislikes: 1 });
        if (wasLiked) updateVideoStats(videoId, { likes: -1 });
    }
    updateVideoModalButtons(videoId);

    const videoRef = doc(db, 'videos', videoId);
    const dislikeRef = doc(db, 'videos', videoId, 'dislikes', currentUser.uid);
    const likeRef = doc(db, 'videos', videoId, 'likes', currentUser.uid);

    try {
        if (wasDisliked) {
            await Promise.all([
                deleteDoc(dislikeRef),
                updateDoc(videoRef, { 'stats.dislikes': increment(-1) })
            ]);
        } else {
            const writes = [
                setDoc(dislikeRef, { createdAt: serverTimestamp() }),
                updateDoc(videoRef, { 'stats.dislikes': increment(1) })
            ];
            if (wasLiked) {
                writes.push(deleteDoc(likeRef));
                writes.push(updateDoc(videoRef, { 'stats.likes': increment(-1) }));
            }
            await Promise.all(writes);
        }
    } catch (err) {
        console.error('Video dislike failed', err);
        await hydrateVideoEngagement(videoId);
        await refreshVideoStatsFromServer(videoId);
        updateVideoModalButtons(videoId);
    }
};

window.saveVideo = async function (videoId) {
    if (!requireAuth()) return;
    if (!videoId) return;
    if (!videoEngagementHydrated.has(videoId)) await hydrateVideoEngagement(videoId);

    const wasSaved = videoEngagementState.saved.has(videoId);
    const nextSaved = wasSaved
        ? (userProfile.savedVideos || []).filter(function (id) { return id !== videoId; })
        : [...(userProfile.savedVideos || []), videoId];

    userProfile.savedVideos = nextSaved;
    syncSavedVideosFromProfile(userProfile);
    updateVideoStats(videoId, { saves: wasSaved ? -1 : 1 });
    updateVideoModalButtons(videoId);
    if (currentCategory === 'Saved') renderSaved();
    if (currentViewId === 'videos' && videoFilter === 'Saved') refreshVideoFeedWithFilters();

    const videoRef = doc(db, 'videos', videoId);
    const saveRef = doc(db, 'videos', videoId, 'saves', currentUser.uid);
    const userRef = doc(db, 'users', currentUser.uid);

    try {
        const writes = [
            updateDoc(userRef, { savedVideos: wasSaved ? arrayRemove(videoId) : arrayUnion(videoId) }),
            updateDoc(videoRef, { 'stats.saves': increment(wasSaved ? -1 : 1) })
        ];
        if (wasSaved) {
            writes.push(deleteDoc(saveRef));
        } else {
            writes.push(setDoc(saveRef, { createdAt: serverTimestamp() }));
        }
        await Promise.all(writes);
        toast(wasSaved ? 'Removed from saved videos.' : 'Saved to your videos.', 'info');
    } catch (err) {
        console.error('Video save failed', err);
        await hydrateVideoEngagement(videoId);
        updateVideoStats(videoId, { saves: wasSaved ? 1 : -1 });
        updateVideoModalButtons(videoId);
    }
};

// --- Live Sessions ---
window.toggleGoLiveModal = function (show = true) { const modal = document.getElementById('go-live-modal'); if (modal) modal.style.display = show ? 'flex' : 'none'; };

window.openGoLiveSetupPage = function () {
    window.location.hash = '#live-setup';
    window.navigateTo('live-setup');
};

function parseViewerCount(value) {
    if (typeof value === 'number') return value;
    if (typeof value === 'string') {
        let normalized = parseFloat(value.replace(/[^0-9.]/g, '')) || 0;
        if (value.toLowerCase().includes('m')) normalized *= 1_000_000;
        else if (value.toLowerCase().includes('k')) normalized *= 1_000;
        return normalized;
    }
    return 0;
}

function resolveLiveThumbnail(session = {}) {
    const candidates = [session.thumbnail, session.thumbnailUrl, session.coverImage, session.imageUrl];
    const resolved = candidates.find(Boolean);
    if (resolved) return resolved;
    return 'data:image/svg+xml;utf8,<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"800\" height=\"450\" viewBox=\"0 0 800 450\"><defs><linearGradient id=\"g\" x1=\"0\" x2=\"1\" y1=\"0\" y2=\"1\"><stop offset=\"0%\" stop-color=\"%2300121a\"/><stop offset=\"100%\" stop-color=\"%23002633\"/></linearGradient></defs><rect width=\"800\" height=\"450\" fill=\"url(%23g)\"/><circle cx=\"120\" cy=\"120\" r=\"60\" fill=\"%2300f2ea\" opacity=\"0.6\"/><rect x=\"200\" y=\"190\" width=\"420\" height=\"120\" rx=\"18\" fill=\"%23ffffff\" opacity=\"0.08\"/><path d=\"M360 210l90 45-90 45z\" fill=\"%2300f2ea\"/></svg>';
}

function handleLiveSearchInput(event) {
    const input = event.target;
    const rawValue = input?.value || '';
    const selection = captureInputSelection(input);
    liveSearchTerm = rawValue.toLowerCase();
    clearTimeout(liveSearchDebounce);
    liveSearchDebounce = setTimeout(function () {
        updateSearchQueryParam(rawValue);
        renderLiveDirectoryFromCache({ skipTopBar: true, skipFilterRow: true });
        restoreInputSelection(input, selection);
    }, SEARCH_DEBOUNCE_MS);
}

function handleLiveTagFilterInput(event) {
    const input = event.target;
    const rawValue = input?.value || '';
    const selection = captureInputSelection(input);
    liveTagFilter = rawValue.toLowerCase();
    clearTimeout(liveTagSearchDebounce);
    liveTagSearchDebounce = setTimeout(function () {
        renderLiveDirectoryFromCache({ skipTopBar: true, skipFilterRow: true });
        restoreInputSelection(input, selection);
    }, SEARCH_DEBOUNCE_MS);
}

function setLiveSortMode(mode) {
    liveSortMode = mode;
    renderLiveDirectoryFromCache();
}

function setLiveCategoryFilter(category) {
    liveCategoryFilter = category;
    renderLiveDirectoryFromCache();
}

function renderLiveTopBar() {
    const container = document.getElementById('live-topbar');
    if (!container) return;

    const goLiveBtn = document.createElement('button');
    goLiveBtn.type = 'button';
    goLiveBtn.className = 'create-btn-sidebar topbar-go-live';
    goLiveBtn.innerHTML = '<i class="ph ph-broadcast"></i> Go Live';
    goLiveBtn.onclick = function () { window.openGoLiveSetupPage(); };

    const topBar = buildTopBar({
        title: 'Live Directory',
        searchPlaceholder: 'Search live streams...',
        searchValue: liveSearchTerm,
        onSearch: handleLiveSearchInput,
        filters: [
            { label: 'All Streams', className: 'discover-pill live-filter-pill', active: liveCategoryFilter === 'All', onClick: function () { setLiveCategoryFilter('All'); } },
            { label: 'STEM', className: 'discover-pill live-filter-pill', active: liveCategoryFilter === 'STEM', onClick: function () { setLiveCategoryFilter('STEM'); } },
            { label: 'Gaming', className: 'discover-pill live-filter-pill', active: liveCategoryFilter === 'Gaming', onClick: function () { setLiveCategoryFilter('Gaming'); } },
            { label: 'Music', className: 'discover-pill live-filter-pill', active: liveCategoryFilter === 'Music', onClick: function () { setLiveCategoryFilter('Music'); } },
            { label: 'Sports', className: 'discover-pill live-filter-pill', active: liveCategoryFilter === 'Sports', onClick: function () { setLiveCategoryFilter('Sports'); } }
        ],
        dropdowns: [
            {
                id: 'live-sort-select',
                className: 'discover-dropdown',
                forId: 'live-sort-select',
                label: 'Sort:',
                options: [
                    { value: 'featured', label: 'Featured' },
                    { value: 'popular', label: 'Most Popular' },
                    { value: 'most_viewed', label: 'Most Viewed Right Now' },
                    { value: 'new', label: 'New' }
                ],
                selected: liveSortMode,
                onChange: function (event) { setLiveSortMode(event.target.value); },
                show: true
            }
        ],
        actions: [{ element: goLiveBtn }]
    });

    container.innerHTML = '';
    container.appendChild(topBar);
}

function renderLiveFilterRow() {
    const container = document.getElementById('live-filter-row');
    if (!container) return;

    const filterShell = document.createElement('div');
    filterShell.className = 'topbar-shell live-filter-shell';

    const controls = buildTopBarControls({
        filters: [
            { label: 'All Streams', className: 'discover-pill live-filter-pill', active: liveCategoryFilter === 'All', onClick: function () { setLiveCategoryFilter('All'); } },
            { label: 'STEM', className: 'discover-pill live-filter-pill', active: liveCategoryFilter === 'STEM', onClick: function () { setLiveCategoryFilter('STEM'); } },
            { label: 'Gaming', className: 'discover-pill live-filter-pill', active: liveCategoryFilter === 'Gaming', onClick: function () { setLiveCategoryFilter('Gaming'); } },
            { label: 'Music', className: 'discover-pill live-filter-pill', active: liveCategoryFilter === 'Music', onClick: function () { setLiveCategoryFilter('Music'); } },
            { label: 'Sports', className: 'discover-pill live-filter-pill', active: liveCategoryFilter === 'Sports', onClick: function () { setLiveCategoryFilter('Sports'); } }
        ],
        dropdowns: [
            {
                id: 'live-sort-secondary',
                className: 'discover-dropdown',
                forId: 'live-sort-secondary-select',
                label: 'Sort:',
                options: [
                    { value: 'featured', label: 'Featured' },
                    { value: 'popular', label: 'Most Popular' },
                    { value: 'most_viewed', label: 'Most Viewed Right Now' },
                    { value: 'new', label: 'New' }
                ],
                selected: liveSortMode,
                onChange: function (event) { setLiveSortMode(event.target.value); }
            },
            {
                id: 'live-category-dropdown',
                className: 'discover-dropdown',
                forId: 'live-category-dropdown-select',
                label: 'Topic:',
                options: [
                    { value: 'All', label: 'All Topics' },
                    { value: 'STEM', label: 'STEM' },
                    { value: 'Gaming', label: 'Gaming' },
                    { value: 'Music', label: 'Music' },
                    { value: 'Sports', label: 'Sports' }
                ],
                selected: liveCategoryFilter,
                onChange: function (event) { setLiveCategoryFilter(event.target.value); }
            },
            {
                id: 'live-tag-filter',
                render: function () {
                    const wrap = document.createElement('div');
                    wrap.className = 'discover-dropdown live-tag-filter';

                    const label = document.createElement('label');
                    label.setAttribute('for', 'live-tag-filter-input');
                    label.textContent = 'Tags:';
                    wrap.appendChild(label);

                    const input = document.createElement('input');
                    input.type = 'text';
                    input.id = 'live-tag-filter-input';
                    input.className = 'form-input';
                    input.placeholder = 'Filter by tag';
                    input.value = liveTagFilter;
                    input.addEventListener('input', handleLiveTagFilterInput);
                    wrap.appendChild(input);
                    return wrap;
                }
            }
        ]
    });

    filterShell.appendChild(controls);
    container.innerHTML = '';
    container.appendChild(filterShell);
}

function renderLiveFeatured(sessions = []) {
    const container = document.getElementById('live-featured-row');
    if (!container) return;
    container.innerHTML = '';
    if (sessions.length === 0) {
        container.innerHTML = '<div class="empty-state">No featured livestreams yet.</div>';
        return;
    }

    const featured = sessions.slice(0, 3);
    featured.forEach(function (session) {
        const card = document.createElement('div');
        card.className = 'social-card live-featured-card';
        card.onclick = function () { window.openLiveSession(session.id); };
        const thumbnail = escapeHtml(resolveLiveThumbnail(session));
        const viewerCount = escapeHtml(session.viewerCount || session.stats?.viewerCount || '0');
        const tags = escapeHtml((session.tags || []).join(', '));
        card.innerHTML = `
            <div class="live-featured-thumb">
                <img src="${thumbnail}" alt="Live thumbnail" class="live-thumb-img" loading="lazy" />
                <div class="live-featured-badge"><i class="ph-fill ph-broadcast"></i> LIVE</div>
            </div>
            <div class="live-featured-body">
                <div class="live-featured-title">${escapeHtml(session.title || 'Live Session')}</div>
                <div class="live-featured-meta">
                    <span class="live-streamer">@${escapeHtml(session.hostId || session.author || 'streamer')}</span>
                    <span class="live-viewers"><i class="ph-fill ph-eye"></i> ${viewerCount}</span>
                </div>
                <div class="live-featured-footer">
                    <span class="live-featured-category">${escapeHtml(session.category || 'Live')}</span>
                    <span class="live-featured-tags">${tags}</span>
                </div>
            </div>`;
        container.appendChild(card);
    });
}

function renderLiveGrid(sessions = []) {
    const grid = document.getElementById('live-directory-grid');
    if (!grid) return;
    grid.innerHTML = '';
    if (sessions.length === 0) {
        grid.innerHTML = '<div class="empty-state">No active livestreams.</div>';
        return;
    }

    sessions.forEach(function (session) {
        const card = document.createElement('div');
        card.className = 'social-card live-directory-card';
        card.onclick = function () { window.openLiveSession(session.id); };
        const tags = (session.tags || []).join(', ');
        const thumbnail = escapeHtml(resolveLiveThumbnail(session));
        const viewerCount = escapeHtml(session.viewerCount || session.stats?.viewerCount || '0');
        card.innerHTML = `
            <div class="live-directory-thumb">
                <img src="${thumbnail}" alt="Live thumbnail" class="live-thumb-img" loading="lazy" />
                <div class="live-directory-badge">LIVE</div>
                <div class="live-viewers live-directory-viewers"><i class="ph-fill ph-eye"></i> ${viewerCount}</div>
            </div>
            <div class="live-directory-body">
                <div class="live-directory-title">${escapeHtml(session.title || 'Live Session')}</div>
                <div class="live-directory-meta">
                    <span class="live-streamer">@${escapeHtml(session.hostId || session.author || 'streamer')}</span>
                    <span class="live-viewers"><i class="ph-fill ph-eye"></i> ${viewerCount}</span>
                </div>
                <div class="live-directory-footer">
                    <span class="live-directory-category">${escapeHtml(session.category || 'Live')}</span>
                    <span class="live-directory-tags">${escapeHtml(tags)}</span>
                </div>
            </div>`;
        grid.appendChild(card);
    });
}

function renderLiveDirectoryFromCache(options = {}) {
    if (!options.skipTopBar) {
        renderLiveTopBar();
    }
    if (!options.skipFilterRow) {
        renderLiveFilterRow();
    }
    const divider = document.getElementById('live-divider');
    const sessions = liveSessionsCache.slice();

    let filtered = sessions;
    if (liveSearchTerm) {
        filtered = filtered.filter(function (session) {
            const title = (session.title || '').toLowerCase();
            const category = (session.category || '').toLowerCase();
            const host = (session.hostId || session.author || '').toLowerCase();
            const tags = (session.tags || []).map(function (t) { return (t || '').toLowerCase(); });
            return title.includes(liveSearchTerm) || category.includes(liveSearchTerm) || host.includes(liveSearchTerm) || tags.some(function (tag) { return tag.includes(liveSearchTerm); });
        });
    }

    if (liveCategoryFilter !== 'All') {
        filtered = filtered.filter(function (session) { return (session.category || '').toLowerCase() === liveCategoryFilter.toLowerCase(); });
    }

    if (liveTagFilter) {
        filtered = filtered.filter(function (session) {
            return (session.tags || []).some(function (tag) { return (tag || '').toLowerCase().includes(liveTagFilter); });
        });
    }

    if (liveSortMode === 'popular' || liveSortMode === 'most_viewed') {
        filtered = filtered.slice().sort(function (a, b) { return parseViewerCount(b.viewerCount || b.stats?.viewerCount) - parseViewerCount(a.viewerCount || a.stats?.viewerCount); });
    } else if (liveSortMode === 'new') {
        filtered = filtered.slice().sort(function (a, b) {
            const aStarted = a.startedAt?.toMillis?.() || a.createdAt?.toMillis?.() || 0;
            const bStarted = b.startedAt?.toMillis?.() || b.createdAt?.toMillis?.() || 0;
            return bStarted - aStarted;
        });
    }

    renderLiveFeatured(filtered);
    if (divider) divider.style.display = filtered.length ? 'block' : 'none';
    renderLiveGrid(filtered);
}

const IVS_BROADCAST_SOURCES = [
    'https://web-broadcast.live-video.net/1.31.1/amazon-ivs-web-broadcast.js',
    'https://web-broadcast.live-video.net/1.31.0/amazon-ivs-web-broadcast.js',
    'https://web-broadcast.live-video.net/1.13.0/amazon-ivs-web-broadcast.js',
];

function loadBroadcastSdk() {
    if (window.IVSBroadcastClient) return Promise.resolve();
    const loadFromSource = (src) =>
        new Promise((resolve, reject) => {
            const script = document.createElement('script');
            script.src = src;
            script.async = true;
            script.onload = () => {
                if (window.IVSBroadcastClient) {
                    resolve();
                } else {
                    const err = new Error(`IVS Broadcast SDK loaded from ${src} but IVSBroadcastClient is missing`);
                    console.error('[GoLive]', err);
                    reject(err);
                }
            };
            script.onerror = (err) => {
                console.error('[GoLive]', `Failed to load IVS Broadcast SDK from ${src}`, err);
                reject(err || new Error(`Failed to load IVS Broadcast SDK from ${src}`));
            };
            document.head.appendChild(script);
        });

    return IVS_BROADCAST_SOURCES.reduce(
        (chain, src) => {
            return chain.catch(() => loadFromSource(src));
        },
        Promise.reject()
    ).then(() => {
        if (!window.IVSBroadcastClient) {
            throw new Error('IVS Broadcast SDK did not initialize after script load attempts');
        }
    });
}

function mapSessionFromBackend(data = {}, config = {}) {
    const ingestEndpoint = data.ingestEndpoint || data.inputEndpoint;
    const rtmpsIngestUrl = data.rtmpsIngestUrl || (ingestEndpoint ? `rtmps://${ingestEndpoint}:443/app/` : '');
    return {
        uid: data.uid || currentUser?.uid || auth?.currentUser?.uid,
        sessionId: data.sessionId,
        channelArn: data.channelArn,
        playbackUrl: data.playbackUrl,
        visibility: data.visibility ?? (config.privacy || 'public').toLowerCase(),
        title: data.title ?? config.title ?? '',
        category: data.category ?? config.category ?? '',
        tags: Array.isArray(data.tags) ? data.tags : config.tags ?? [],
        latencyMode: (data.latencyMode || config.latencyMode || 'NORMAL').toUpperCase(),
        autoRecord: data.autoRecord ?? config.autoRecord ?? false,
        inputMode: config.videoMode || data.inputMode || 'camera',
        audioMode: config.audioMode || data.audioMode || 'mic',
        isLive: Boolean(data.isLive),
        ingestEndpoint,
        rtmpsIngestUrl,
        streamKey: data.streamKey,
    };
}

function deriveIngestHostname(session = {}) {
    if (session.ingestEndpoint) return session.ingestEndpoint;
    if (session.rtmpsIngestUrl) {
        try {
            const url = new URL(session.rtmpsIngestUrl);
            return url.hostname;
        } catch (_) {
            return null;
        }
    }
    return null;
}

class GoLiveSetupController {
    constructor() {
        this.functions = getFunctions(app);
        this.state = 'idle';
        this.session = null;
        this.stream = null;
        this.broadcastClient = null;
        this.previewEl = null;
        this.statusEl = null;
        this.startButton = null;
        this.endButton = null;
        this.videoModeSelect = null;
        this.audioModeSelect = null;
        this.videoDeviceSelect = null;
        this.audioDeviceSelect = null;
        this.latencySelect = null;
        this.bound = false;
        this.inputMode = 'camera';
        this.audioMode = 'mic';
        this.latencyMode = 'normal';
        this.uiMode = localStorage.getItem(GO_LIVE_MODE_STORAGE_KEY) === 'advanced' ? 'advanced' : 'basic';

        this.onStart = this.onStart.bind(this);
        this.onEnd = this.onEnd.bind(this);
        this.handleDeviceChange = this.handleDeviceChange.bind(this);

        console.info('[GoLive]', 'Controller initialized');
        if (navigator.mediaDevices?.addEventListener) {
            navigator.mediaDevices.addEventListener('devicechange', this.handleDeviceChange);
        }
    }

    bindUI() {
        const startBtn = document.getElementById('start-stream');
        const endBtn = document.getElementById('end-stream');
        const preview = document.getElementById('live-preview');
        const status = document.getElementById('live-status-text');
        const videoMode = document.getElementById('live-video-mode');
        const audioMode = document.getElementById('live-audio-mode');
        const videoDevice = document.getElementById('live-video-device');
        const audioDevice = document.getElementById('live-audio-device');
        const latencySelect = document.getElementById('latency-mode');

        if (!startBtn || !preview) {
            console.warn('[GoLive]', 'Go Live UI not ready; waiting for DOM');
            return;
        }

        if (this.startButton) {
            this.startButton.removeEventListener('click', this.onStart);
        }
        if (this.endButton) {
            this.endButton.removeEventListener('click', this.onEnd);
        }

        this.startButton = startBtn;
        this.endButton = endBtn;
        this.previewEl = preview;
        this.statusEl = status;
        this.videoModeSelect = videoMode;
        this.audioModeSelect = audioMode;
        this.videoDeviceSelect = videoDevice;
        this.audioDeviceSelect = audioDevice;
        this.latencySelect = latencySelect;

        this.startButton.addEventListener('click', this.onStart);
        if (this.endButton) this.endButton.addEventListener('click', this.onEnd);

        if (this.videoModeSelect) {
            this.videoModeSelect.addEventListener('change', (e) => {
                this.inputMode = e.target.value || 'camera';
                console.info('[GoLive]', 'Video mode changed', { mode: this.inputMode });
                this.updateStatus('Video input updated');
            });
        }
        if (this.audioModeSelect) {
            this.audioModeSelect.addEventListener('change', (e) => {
                this.audioMode = e.target.value || 'mic';
                console.info('[GoLive]', 'Audio mode changed', { mode: this.audioMode });
                this.updateStatus('Audio input updated');
            });
        }
        if (this.latencySelect) {
            this.latencyMode = this.latencySelect.value || 'normal';
            this.latencySelect.addEventListener('change', (e) => {
                this.latencyMode = e.target.value || 'normal';
                console.info('[GoLive]', 'Latency mode changed', { mode: this.latencyMode });
                this.updateStatus('Latency preference updated');
            });
        }

        this.bound = true;
        console.info('[GoLive]', 'Go Live UI bound and ready');
        this.refreshDeviceOptions();
        this.updateUiState();
    }

    handleDeviceChange() {
        console.info('[GoLive]', 'Media devices changed');
        this.refreshDeviceOptions();
    }

    setState(nextState, detail = '') {
        console.info('[GoLive]', 'State change', { from: this.state, to: nextState, detail });
        this.state = nextState;
        this.updateStatus(detail);
        this.updateUiState();
    }

    updateStatus(detail = '') {
        if (this.statusEl) {
            const suffix = detail ? ` – ${detail}` : '';
            this.statusEl.textContent = `State: ${this.state}${suffix}`;
        }
    }

    updateUiState() {
        if (this.startButton) {
            const disableStart = ['initializing', 'starting', 'live', 'ready'].includes(this.state);
            this.startButton.disabled = disableStart;
            this.startButton.textContent = this.state === 'initializing' ? 'Starting…' : this.state === 'starting' ? 'Going Live…' : 'Start Streaming';
        }
        if (this.endButton) {
            this.endButton.style.display = this.state === 'live' || this.state === 'previewing' || this.state === 'ready' ? 'inline-flex' : 'none';
        }
    }

    async refreshDeviceOptions(requestLabels = false) {
        if (!navigator.mediaDevices?.enumerateDevices) {
            console.warn('[GoLive]', 'Media device enumeration not supported');
            return;
        }
        try {
            console.info('[GoLive]', 'Enumerating media devices');
            const devices = await navigator.mediaDevices.enumerateDevices();
            this.populateDeviceSelect(this.videoDeviceSelect, devices.filter((d) => d.kind === 'videoinput'), 'camera');
            this.populateDeviceSelect(this.audioDeviceSelect, devices.filter((d) => d.kind === 'audioinput'), 'microphone');
            if (requestLabels && this.videoDeviceSelect && this.videoDeviceSelect.options.length && !this.videoDeviceSelect.options[0].textContent) {
                this.updateStatus('Device labels refreshed');
            }
        } catch (error) {
            console.error('[GoLive]', 'Device enumeration failed', error);
            this.setState('error', error?.message || 'Failed to enumerate devices');
        }
    }

    populateDeviceSelect(selectEl, devices = [], fallbackLabel = 'device') {
        if (!selectEl) return;
        selectEl.innerHTML = '';
        const defaultOption = document.createElement('option');
        defaultOption.value = '';
        defaultOption.textContent = `Use default ${fallbackLabel}`;
        selectEl.appendChild(defaultOption);
        devices.forEach((device) => {
            const option = document.createElement('option');
            option.value = device.deviceId;
            option.textContent = device.label || `${fallbackLabel} ${selectEl.options.length}`;
            selectEl.appendChild(option);
        });
    }

    collectFormValues() {
        const tagsRaw = (document.getElementById('live-setup-tags')?.value || '').split(',').map((t) => t.trim()).filter(Boolean);
        const autoRecordInput = document.getElementById('auto-record') || document.getElementById('live-setup-autorecord');
        const latencySelect = document.getElementById('latency-mode');
        const values = {
            title: document.getElementById('live-setup-title')?.value || '',
            category: document.getElementById('live-setup-category')?.value || '',
            tags: tagsRaw,
            privacy: document.getElementById('live-setup-privacy')?.value || 'Public',
            autoRecord: Boolean(autoRecordInput?.checked),
            latencyMode: latencySelect?.value || this.latencyMode || 'normal',
            videoMode: this.videoModeSelect?.value || 'camera',
            audioMode: this.audioModeSelect?.value || 'mic',
            videoDeviceId: this.videoDeviceSelect?.value || '',
            audioDeviceId: this.audioDeviceSelect?.value || '',
        };
        console.info('[GoLive]', 'Form values collected', values);
        return values;
    }

    async onStart(event) {
        if (event) event.preventDefault();
        console.info('[GoLive]', 'Start Streaming clicked');
        if (!this.bound) {
            console.warn('[GoLive]', 'Start ignored; UI not bound');
            return;
        }
        if (this.state === 'initializing') {
            console.warn('[GoLive]', 'Already initializing; ignoring duplicate start');
            return;
        }
        if (this.state === 'live') {
            console.warn('[GoLive]', 'Stream already live; ignoring start');
            return;
        }

        const config = this.collectFormValues();
        this.setState('configuring', 'Preparing to request media');

        try {
            if (config.videoMode !== 'external') {
                await this.acquireMedia(config);
            } else {
                this.setState('previewing', 'External streaming software selected; skipping local capture');
            }
            const session = await this.initializeBackend(config);
            if (!session) {
                throw new Error('No session returned from backend');
            }
            if (config.videoMode === 'external') {
                this.showExternalInstructions(session);
                return;
            }
            await this.startBroadcast(session);
        } catch (error) {
            console.error('[GoLive]', 'Start flow failed', error);
            this.setState('error', error?.message || 'Failed to start stream');
        }
    }

    async onEnd(event) {
        if (event) event.preventDefault();
        console.info('[GoLive]', 'End Stream requested');
        await this.stopBroadcast();
        this.stopCurrentStream();
        if (this.session?.sessionId) {
            await this.markSessionEnded(this.session.sessionId);
        }
        this.session = null;
        this.setState('idle', 'Stream ended or cancelled');
    }

    stopCurrentStream() {
        if (this.stream) {
            console.info('[GoLive]', 'Stopping current media tracks');
            this.stream.getTracks().forEach((t) => t.stop());
            this.stream = null;
        }
        if (this.previewEl) {
            this.previewEl.srcObject = null;
        }
    }

    async stopBroadcast() {
        if (this.broadcastClient) {
            try {
                await this.broadcastClient.stopBroadcast();
            } catch (error) {
                console.error('[GoLive]', 'Error stopping broadcast client', error);
            }
            this.broadcastClient = null;
        }
    }

    async markSessionLive(sessionId) {
        if (!sessionId) return;
        try {
            await updateDoc(doc(db, 'liveStreams', sessionId), {
                isLive: true,
                startedAt: serverTimestamp(),
                endedAt: null,
            });
        } catch (error) {
            console.error('[GoLive]', 'Failed to mark session live', error);
        }
    }

    async markSessionEnded(sessionId) {
        if (!sessionId) return;
        try {
            await updateDoc(doc(db, 'liveStreams', sessionId), {
                isLive: false,
                endedAt: serverTimestamp(),
            });
        } catch (error) {
            console.error('[GoLive]', 'Failed to mark session ended', error);
        }
    }

    async persistPrivateStreamKey(session) {
        if (!session?.sessionId || !session.streamKey) return;
        const uid = session.uid || currentUser?.uid || auth?.currentUser?.uid || null;
        if (!uid) return;
        try {
            await setDoc(
                doc(db, 'liveStreams', session.sessionId, 'private', 'keys'),
                { uid, streamKey: session.streamKey, updatedAt: serverTimestamp() },
                { merge: true }
            );
            await updateDoc(doc(db, 'liveStreams', session.sessionId), { streamKey: deleteField() });
        } catch (error) {
            console.error('[GoLive]', 'Failed to persist private stream key', error);
        }
    }

    async persistSession(session) {
        if (!session?.sessionId) return;
        try {
            const uid = session.uid || currentUser?.uid || auth?.currentUser?.uid || null;
            const settings = {
                inputMode: session.inputMode || this.inputMode || 'camera',
                audioMode: session.audioMode || this.audioMode || 'mic',
                latencyMode: (session.latencyMode || this.latencyMode || 'NORMAL').toUpperCase(),
                autoRecord: !!(session.autoRecord ?? false),
                visibility: session.visibility || 'public',
                title: session.title || '',
                category: session.category || '',
                tags: Array.isArray(session.tags) ? session.tags : [],
            };
            const payload = {
                sessionId: session.sessionId,
                uid,
                channelArn: session.channelArn,
                playbackUrl: session.playbackUrl,
                visibility: settings.visibility,
                title: settings.title,
                category: settings.category,
                tags: settings.tags,
                ingestEndpoint: session.ingestEndpoint,
                rtmpsIngestUrl: session.rtmpsIngestUrl,
                isLive: Boolean(session.isLive),
                settings,
                ui: { mode: this.uiMode, updatedAt: serverTimestamp() },
                createdAt: serverTimestamp(),
            };
            await setDoc(doc(db, 'liveStreams', session.sessionId), payload, { merge: true });
            await updateDoc(doc(db, 'liveStreams', session.sessionId), { streamKey: deleteField() });
            await this.persistPrivateStreamKey(session);
        } catch (error) {
            console.error('[GoLive]', 'Failed to persist session details', error);
        }
    }

    async startBroadcast(session) {
        if (!session) throw new Error('Missing session details for broadcast');
        if (!this.stream) throw new Error('No media stream available to broadcast');

        const ingestHostname = deriveIngestHostname(session);
        if (!ingestHostname) {
            throw new Error('Missing ingest endpoint from backend');
        }

        this.setState('starting', 'Loading broadcast SDK');
        await loadBroadcastSdk();

        const streamConfig = window.IVSBroadcastClient?.BASIC_LANDSCAPE;
        this.broadcastClient = window.IVSBroadcastClient.create({
            ingestEndpoint: ingestHostname,
            streamConfig,
        });

        const videoTrack = this.stream.getVideoTracks?.()[0] || null;
        if (!videoTrack) {
            throw new Error('No video track available to broadcast');
        }
        const videoStream = new MediaStream([videoTrack]);
        await this.broadcastClient.addVideoInputDevice(videoStream, 'video1', { index: 0 });

        const audioTracks = this.stream.getAudioTracks ? this.stream.getAudioTracks() : [];
        if (audioTracks && audioTracks.length) {
            await Promise.all(audioTracks.map((track, idx) => {
                const audioStream = new MediaStream([track]);
                return this.broadcastClient.addAudioInputDevice(audioStream, `audio${idx + 1}`);
            }));
        }

        this.setState('starting', 'Connecting to ingest server');
        await this.broadcastClient.startBroadcast(session.streamKey);
        await this.markSessionLive(session.sessionId);
        this.setState('live', 'Broadcast started');
    }

    showExternalInstructions(session) {
        const ingestUrl = session?.rtmpsIngestUrl || (session?.ingestEndpoint ? `rtmps://${session.ingestEndpoint}:443/app/` : '');
        const detail = ingestUrl && session?.streamKey
            ? `Use ${ingestUrl} with stream key ${session.streamKey}`
            : 'Channel ready. Configure your encoder with the provided ingest endpoint and stream key.';
        this.setState('ready', detail);
    }

    async acquireMedia(config) {
        this.stopCurrentStream();
        this.setState('initializing', 'Requesting media devices');

        try {
            const stream = config.videoMode === 'screen'
                ? await this.buildScreenStream(config)
                : await this.buildCameraStream(config);

            if (stream) {
                this.stream = stream;
                this.attachPreview(stream);
                this.setState('previewing', 'Media ready');
                await this.refreshDeviceOptions(true);
            } else {
                this.setState('previewing', 'No local media stream provided');
            }
            return stream;
        } catch (error) {
            console.error('[GoLive]', 'Media request failed', error);
            this.setState('error', error?.message || 'Unable to access media devices');
            throw error;
        }
    }

    async buildCameraStream(config) {
        const videoConstraints = config.videoDeviceId ? { deviceId: { exact: config.videoDeviceId } } : true;
        const wantsAudio = config.audioMode !== 'external';
        const audioConstraints = wantsAudio ? (config.audioDeviceId ? { deviceId: { exact: config.audioDeviceId }, echoCancellation: true } : { echoCancellation: true }) : false;

        console.info('[GoLive]', 'Requesting camera stream', { videoConstraints, audioConstraints });
        return navigator.mediaDevices.getUserMedia({ video: videoConstraints, audio: audioConstraints });
    }

    async buildScreenStream(config) {
        console.info('[GoLive]', 'Requesting screen stream', { audioMode: config.audioMode });
        const wantsSystemAudio = config.audioMode === 'system' || config.audioMode === 'mixed';
        const displayStream = await navigator.mediaDevices.getDisplayMedia({ video: true, audio: wantsSystemAudio });
        const tracks = [];
        const displayVideo = displayStream.getVideoTracks()[0];
        if (displayVideo) tracks.push(displayVideo);

        if (wantsSystemAudio) {
            const systemAudio = displayStream.getAudioTracks()[0];
            if (systemAudio) tracks.push(systemAudio);
        }

        if (config.audioMode === 'mic' || config.audioMode === 'mixed') {
            const micConstraints = config.audioDeviceId ? { deviceId: { exact: config.audioDeviceId }, echoCancellation: true } : { echoCancellation: true };
            const micStream = await navigator.mediaDevices.getUserMedia({ audio: micConstraints });
            const micTrack = micStream.getAudioTracks()[0];
            if (micTrack) tracks.push(micTrack);
        }

        return tracks.length ? new MediaStream(tracks) : null;
    }

    attachPreview(stream) {
        if (!this.previewEl) return;
        console.info('[GoLive]', 'Attaching preview stream');
        this.previewEl.srcObject = stream;
        const playResult = this.previewEl.play?.();
        if (playResult?.catch) {
            playResult.catch((error) => console.warn('[GoLive]', 'Preview play blocked', error));
        }
    }

    async initializeBackend(config) {
        if (!this.functions) {
            this.functions = getFunctions(app);
        }
        if (!auth?.currentUser) {
            console.error('[GoLive]', 'Cannot start stream without authenticated user');
            this.setState('error', 'You must be signed in to go live');
            throw new Error('User not authenticated');
        }
        const payload = {
            title: config.title,
            category: config.category,
            tags: config.tags,
            latencyMode: (config.latencyMode || 'normal').toUpperCase(),
            visibility: (config.privacy || 'Public').toLowerCase(),
            autoRecord: config.autoRecord,
            inputMode: config.videoMode,
            audioMode: config.audioMode,
            uid: auth?.currentUser?.uid || currentUser?.uid,
        };
        this.setState('initializing', 'Calling backend to create channel');
        console.info('[GoLive]', 'Calling createEphemeralChannel', payload);
        try {
            const idToken = await auth.currentUser.getIdToken();
            const response = await fetch(
                'https://us-central1-spike-streaming-service.cloudfunctions.net/createEphemeralChannel',
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        Authorization: `Bearer ${idToken}`,
                    },
                    body: JSON.stringify(payload),
                }
            );

            const raw = await response.text();
            let json = null;
            try {
                json = raw ? JSON.parse(raw) : null;
            } catch (_) {
                json = null;
            }

            if (!response.ok) {
                const messageCandidate = json?.error?.message || json?.error || json?.message || raw || `HTTP ${response.status}`;
                const message = typeof messageCandidate === 'string' ? messageCandidate : JSON.stringify(messageCandidate);
                console.error('[GoLive] createEphemeralChannel failed', { status: response.status, raw, json });
                throw new Error(message);
            }

            const data = json ?? {};
            const session = mapSessionFromBackend(data, config);
            this.session = { ...session, isLive: session.isLive ?? false };
            console.info('[GoLive]', 'createEphemeralChannel response', session);
            await this.persistSession(this.session);
            this.setState('starting', 'Ephemeral channel ready');
            return session;
        } catch (error) {
            console.error('[GoLive]', 'Backend call failed', error);
            this.setState('error', error?.message || 'Backend initialization failed');
            throw error;
        }
    }
}

function ensureGoLiveController() {
    if (!goLiveController) {
        goLiveController = new NexeraGoLiveController();
        goLiveController.initializeUI();
        window.__goLiveController = goLiveController;
        window.__goLiveSetupController = goLiveController;
    } else if (typeof goLiveController.initializeUI === 'function') {
        goLiveController.initializeUI();
    }
    return goLiveController;
}

function renderLiveSetup() {
    const titleInput = document.getElementById('live-setup-title');
    if (titleInput && !titleInput.value) titleInput.placeholder = 'Give your stream a standout title';
    ensureGoLiveController();
}

function renderLiveSessions() {
    if (liveSessionsUnsubscribe) { renderLiveDirectoryFromCache(); return; }
    const liveRef = query(
        collection(db, 'liveStreams'),
        where('isLive', '==', true),
        orderBy('startedAt', 'desc'),
        orderBy('createdAt', 'desc')
    );
    liveSessionsUnsubscribe = ListenerRegistry.register('live:sessions', onSnapshot(liveRef, function (snap) {
        try {
            liveSessionsCache = snap.docs.map(function (d) {
                const data = d.data();
                const playbackUrl = data.playbackUrl || data.streamUrl || data.streamEmbedURL || '';
                return ({
                    id: d.id,
                    ...data,
                    streamUrl: data.streamUrl || playbackUrl,
                    streamEmbedURL: data.streamEmbedURL || playbackUrl,
                    title: data.title || '',
                    category: data.category || '',
                    tags: Array.isArray(data.tags) ? data.tags : [],
                });
            });
            renderLiveDirectoryFromCache();
        } catch (e) {
            console.error('Live sessions snapshot failed', e);
        }
    }, function (error) {
        console.error('Live sessions listener error', error);
        renderLiveDirectoryFromCache();
    }));
}

window.createLiveSession = async function () {
    if (!requireAuth()) return;
    const title = document.getElementById('live-title').value;
    const category = document.getElementById('live-category').value;
    const tags = (document.getElementById('live-tags').value || '').split(',').map(function (t) { return t.trim(); }).filter(Boolean);
    const streamEmbedURL = document.getElementById('live-url').value;
    try {
        await addDoc(collection(db, 'liveStreams'), {
            hostId: currentUser.uid,
            title,
            category,
            tags,
            streamEmbedURL,
            isLive: true,
            createdAt: serverTimestamp(),
            startedAt: serverTimestamp(),
        });
        toggleGoLiveModal(false);
    } catch (e) {
        console.error('Failed to create live session', e);
    }
};

window.openLiveSession = function (sessionId) {
    if (activeLiveSessionId && activeLiveSessionId !== sessionId) {
        ListenerRegistry.unregister(`live:chat:${activeLiveSessionId}`);
    }
    activeLiveSessionId = sessionId;
    const container = document.getElementById('live-directory-grid') || document.getElementById('live-grid-container');
    if (!container) return;
    const sessionData = liveSessionsCache.find(function (session) { return session.id === sessionId; });
    const streamUrl = sessionData?.streamEmbedURL || sessionData?.streamUrl || sessionData?.playbackUrl;
    const sessionCard = document.createElement('div');
    sessionCard.className = 'social-card';
    if (!sessionData || !streamUrl) {
        sessionCard.innerHTML = '<div style="padding:1rem;"><div class="empty-state">Stream is offline or unavailable.</div></div>';
        container.prepend(sessionCard);
        return;
    }
    const tags = (sessionData.tags || []).join(', ');
    sessionCard.innerHTML = `<div style="padding:1rem;">
        <div id="live-player" style="margin-bottom:10px;">
            <video src="${escapeHtml(streamUrl)}" controls autoplay playsinline style="width:100%;max-height:320px;" type="application/x-mpegURL"></video>
        </div>
        <div style="margin-bottom:8px;">
            <div style="font-weight:700;">${escapeHtml(sessionData.title || 'Live Session')}</div>
            <div style="color:var(--text-muted);">${escapeHtml(sessionData.category || 'Live')}${tags ? ` • ${escapeHtml(tags)}` : ''}</div>
        </div>
        <div id="live-chat" style="max-height:200px; overflow:auto;"></div>
        <div style="display:flex; gap:8px; margin-top:8px;">
            <input id="live-chat-input" class="form-input" placeholder="Chat"/>
            <button class="create-btn-sidebar" style="width:auto;" onclick="window.sendLiveChat('${sessionId}')">Send</button>
        </div>
    </div>`;
    container.prepend(sessionCard);
    listenLiveChat(sessionId);
};

function listenLiveChat(sessionId) {
    const chatRef = query(collection(db, 'liveStreams', sessionId, 'chat'), orderBy('createdAt'));
    ListenerRegistry.register(`live:chat:${sessionId}`, onSnapshot(chatRef, function (snap) {
        try {
            const chatEl = document.getElementById('live-chat');
            if (!chatEl) return;
            chatEl.innerHTML = '';
            snap.docs.forEach(function (docSnap) {
                const data = docSnap.data();
                const row = document.createElement('div');
                row.textContent = `${userCache[data.senderId]?.username || 'user'}: ${data.text}`;
                chatEl.appendChild(row);
            });
        } catch (e) {
            console.error('Live chat snapshot failed', e);
        }
    }, function (error) {
        console.error('Live chat listener error', error);
        const chatEl = document.getElementById('live-chat');
        if (chatEl) chatEl.innerHTML = '<div class="empty-state">Chat unavailable.</div>';
    }));
}

window.sendLiveChat = async function (sessionId) {
    if (!requireAuth()) return;
    const input = document.getElementById('live-chat-input');
    if (!input || !input.value.trim()) return;
    try {
        await addDoc(collection(db, 'liveStreams', sessionId, 'chat'), { senderId: currentUser.uid, text: input.value, createdAt: serverTimestamp() });
        input.value = '';
    } catch (e) {
        console.error('Failed to send live chat', e);
    }
};

// --- Staff Console ---
function renderStaffConsole() {
    const warning = document.getElementById('staff-access-warning');
    const panels = document.getElementById('staff-panels');
    const isStaff = hasGlobalRole('staff') || hasGlobalRole('admin') || hasFounderClaimClient();
    if (!isStaff) {
        if (warning) warning.style.display = 'block';
        if (panels) panels.style.display = 'none';
        return;
    }
    if (warning) warning.style.display = 'none';
    if (panels) panels.style.display = 'block';
    listenVerificationRequests();
    listenReports();
    listenAdminLogs();
    bindTrendingCategoriesSync();
}

async function syncTrendingCategories() {
    const categoriesSnap = await getDocs(collection(db, 'categories'));
    const scores = [];

    for (const docSnap of categoriesSnap.docs) {
        const data = docSnap.data() || {};
        const slug = data.slug || docSnap.id;
        if (!slug) continue;
        const memberCount = Number(data.memberCount || 0) || 0;
        let postCount = 0;
        let commentCount = 0;

        const postsSnap = await getDocs(query(collection(db, 'posts'), where('categoryId', '==', slug)));
        postsSnap.forEach(function (postDoc) {
            postCount += 1;
            const postData = postDoc.data() || {};
            const postComments = Number(postData.comments || postData.commentCount || postData.stats?.comments || 0) || 0;
            commentCount += postComments;
        });

        const popularity = Math.round((postCount * 5) + (memberCount * 2) + commentCount);
        scores.push({ slug, popularity });
    }

    scores.sort(function (a, b) { return b.popularity - a.popularity; });
    const topScores = scores.slice(0, 10);
    const batch = writeBatch(db);
    const now = serverTimestamp();

    topScores.forEach(function (entry) {
        const ref = doc(db, 'trendingCategories', entry.slug);
        batch.set(ref, {
            slug: entry.slug,
            popularity: entry.popularity,
            updatedAt: now
        }, { merge: true });
    });

    await batch.commit();
}

// Staff-only sync helper to seed trendingCategories from categories.
function bindTrendingCategoriesSync() {
    const btn = document.getElementById('sync-trending-categories');
    if (!btn || staffTrendingSyncBound) return;
    staffTrendingSyncBound = true;
    btn.addEventListener('click', async function () {
        btn.disabled = true;
        try {
            await syncTrendingCategories();
            alert('Trending categories synced successfully.');
        } catch (err) {
            console.error('Error syncing trending categories:', err);
            alert('Failed to sync trending categories. Check console for details.');
        } finally {
            btn.disabled = false;
        }
    });
}

function listenVerificationRequests() {
    if (staffRequestsUnsub) return;
    staffRequestsUnsub = ListenerRegistry.register('staff:verificationRequests', onSnapshot(collection(db, 'verificationRequests'), function (snap) {
        const container = document.getElementById('verification-requests');
        if (!container) return;
        container.innerHTML = '';
        snap.docs.forEach(function (docSnap) {
            const data = docSnap.data();
            const card = document.createElement('div');
            card.className = 'social-card';
            card.innerHTML = `<div style="padding:1rem;"><div style="font-weight:800;">${data.category}</div><div style="font-size:0.9rem; color:var(--text-muted);">${(data.evidenceLinks || []).join('<br>')}</div><div style="margin-top:6px; display:flex; gap:8px;"><button class="icon-pill" onclick="window.approveVerification('${docSnap.id}', '${data.userId}')">Approve</button><button class="icon-pill" onclick="window.denyVerification('${docSnap.id}')">Deny</button></div></div>`;
            container.appendChild(card);
        });
    }));
}

window.approveVerification = async function (requestId, userId) {
    await updateDoc(doc(db, 'verificationRequests', requestId), { status: 'approved', reviewedAt: serverTimestamp() });
    if (userId) await setDoc(doc(db, 'users', userId), { accountRoles: arrayUnion('verified'), verified: true, updatedAt: serverTimestamp() }, { merge: true });
    await addDoc(collection(db, 'adminLogs'), { actorId: currentUser.uid, action: 'approveVerification', targetRef: requestId, createdAt: serverTimestamp() });
};

window.denyVerification = async function (requestId) {
    await updateDoc(doc(db, 'verificationRequests', requestId), { status: 'denied', reviewedAt: serverTimestamp() });
    await addDoc(collection(db, 'adminLogs'), { actorId: currentUser.uid, action: 'denyVerification', targetRef: requestId, createdAt: serverTimestamp() });
};

function listenReports() {
    if (staffReportsUnsub) return;
    staffReportsUnsub = ListenerRegistry.register('staff:reports', onSnapshot(collection(db, 'reports'), function (snap) {
        const container = document.getElementById('reports-queue');
        if (!container) return;
        container.innerHTML = '';
        snap.docs.forEach(function (docSnap) {
            const data = docSnap.data();
            const card = document.createElement('div');
            card.className = 'social-card';
            card.innerHTML = `<div style="padding:1rem;"><div style="font-weight:800;">${data.type || 'report'}</div><div style="color:var(--text-muted); font-size:0.9rem;">${data.reason || ''}</div></div>`;
            container.appendChild(card);
        });
    }));
}

function listenAdminLogs() {
    if (staffLogsUnsub) return;
    staffLogsUnsub = ListenerRegistry.register('staff:adminLogs', onSnapshot(collection(db, 'adminLogs'), function (snap) {
        const container = document.getElementById('admin-logs');
        if (!container) return;
        container.innerHTML = '';
        snap.docs.forEach(function (docSnap) {
            const data = docSnap.data();
            const row = document.createElement('div');
            row.textContent = `${data.actorId}: ${data.action}`;
            container.appendChild(row);
        });
    }));
}

// --- Verification Request ---
window.openVerificationRequest = function () { toggleVerificationModal(true); };
window.toggleVerificationModal = function (show = true) { const modal = document.getElementById('verification-modal'); if (modal) modal.style.display = show ? 'flex' : 'none'; };
window.submitVerificationRequest = async function () {
    if (!requireAuth()) return;
    const category = document.getElementById('verify-category').value;
    const links = (document.getElementById('verify-links').value || '').split('\n').map(function (l) { return l.trim(); }).filter(Boolean);
    const notes = document.getElementById('verify-notes').value;
    await addDoc(collection(db, 'verificationRequests'), { userId: currentUser.uid, category, evidenceLinks: links, notes, status: 'pending', createdAt: serverTimestamp() });
    toggleVerificationModal(false);
};

// --- Mobile shell helpers ---
window.toggleDiscoverSearch = function () {
    window.navigateTo('discover');
    const discoverInput = document.querySelector('#view-discover input[type="text"]');
    if (discoverInput) discoverInput.focus();
    window.closeMobileActionSheet();
};

window.openMobileActionSheet = function () {
    const sheet = document.getElementById('mobile-action-sheet');
    if (sheet) sheet.classList.add('show');
};

window.closeMobileActionSheet = function () {
    const sheet = document.getElementById('mobile-action-sheet');
    if (sheet) sheet.classList.remove('show');
};

window.openMobileComposer = function () {
    closeMobileActionSheet();
    const sheet = document.getElementById('mobile-composer-sheet');
    const input = document.getElementById('mobile-compose-input');
    const existingContent = document.getElementById('postContent');
    if (input && existingContent) input.value = existingContent.value || '';
    if (sheet) sheet.classList.add('show');
    syncMobileComposerState();
    if (input) input.focus();
};

window.closeMobileComposer = function () {
    const sheet = document.getElementById('mobile-composer-sheet');
    if (sheet) sheet.classList.remove('show');
};

window.syncMobileComposerState = function () {
    const input = document.getElementById('mobile-compose-input');
    const submit = document.getElementById('mobile-compose-submit');
    const hasContent = input && input.value.trim().length > 0;
    if (submit) submit.disabled = !hasContent;
};

window.triggerComposerPost = function () {
    const input = document.getElementById('mobile-compose-input');
    const content = document.getElementById('postContent');
    if (!input || !content) return;
    if (!input.value.trim()) return;
    content.value = input.value;
    closeMobileComposer();
    window.toggleCreateModal(true);
    syncPostButtonState();
};

function bindMobileScrollHelper() {
    const btn = document.getElementById('mobile-scroll-top');
    if (!btn) return;
    const handler = function () {
        const shouldShow = isMobileViewport() && window.scrollY > 200;
        btn.classList.toggle('show', shouldShow);
    };
    handler();
    window.addEventListener('scroll', handler, { passive: true });
    if (MOBILE_VIEWPORT && MOBILE_VIEWPORT.addEventListener) {
        MOBILE_VIEWPORT.addEventListener('change', handler);
    }
}

function updateInboxTabsHeight() {
    const tabs = document.querySelector('.inbox-tabs');
    if (!tabs) return;
    const styles = window.getComputedStyle ? window.getComputedStyle(tabs) : null;
    const marginTop = styles ? parseFloat(styles.marginTop) || 0 : 0;
    const marginBottom = styles ? parseFloat(styles.marginBottom) || 0 : 0;
    const height = Math.ceil(tabs.getBoundingClientRect().height + marginTop + marginBottom);
    document.documentElement.style.setProperty('--inbox-tabs-h', `${height}px`);
}

function refreshInboxLayout() {
    requestAnimationFrame(function () {
        updateInboxTabsHeight();
        requestAnimationFrame(updateInboxTabsHeight);
    });
}

function syncSidebarHomeState() {
    const path = window.location.pathname || '/';
    const isHome = path === '/home' || path === '/' || path === '';
    document.body.classList.toggle('sidebar-home', isHome);
    document.body.classList.toggle('sidebar-wide', shouldShowRightSidebar(currentViewId || 'feed'));
    if (isHome) {
        mountFeedTypeToggleBar();
        renderStoriesAndLiveBar(document.getElementById('stories-live-bar-slot'));
    }
    uiDebugLog('sidebar home sync', { path, isHome });
}

window.syncSidebarHomeState = syncSidebarHomeState;

document.addEventListener('DOMContentLoaded', function () {
    bindMobileNav();
    bindMobileScrollHelper();
    updateInboxTabsHeight();
    initSidebarState();
    bindSidebarEvents();
    loadFeedTypeState();
    mountFeedTypeToggleBar();
    syncMobileComposerState();
    bindAuthFormShortcuts();
    initMiniPlayerDrag();
    initTrendingTopicsUI();
    initVideoViewerLayout();
    bindVideoDestinationField();
    enhanceInboxLayout();
    syncInboxContentFilters();
    renderStoriesAndLiveBar(document.getElementById('stories-live-bar-slot'));
    const title = document.getElementById('postTitle');
    const content = document.getElementById('postContent');
    if (title) title.addEventListener('input', syncPostButtonState);
    if (content) content.addEventListener('input', syncPostButtonState);
    syncSidebarHomeState();
    updateTimeCapsule();
    initializeNexeraApp();
    const initialHash = (window.location.hash || '').replace('#', '');
    if (initialHash === 'live-setup') { window.navigateTo('live-setup', false); }
});

window.addEventListener('resize', function () {
    updateInboxTabsHeight();
});

window.addEventListener('hashchange', function () {
    const hash = (window.location.hash || '').replace('#', '');
    if (hash === 'live-setup') { window.navigateTo('live-setup', false); }
});

// --- Security Rules Snippet (reference) ---
// See firestore.rules for suggested rules ensuring users write their own content and staff-only access.
