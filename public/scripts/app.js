import { initializeApp } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-app.js";
import { getAuth, signInWithEmailAndPassword, createUserWithEmailAndPassword, signInAnonymously, onAuthStateChanged, signOut, updateProfile } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-auth.js";
import { getFirestore, collection, addDoc, onSnapshot, query, orderBy, serverTimestamp, doc, setDoc, getDoc, updateDoc, deleteDoc, deleteField, arrayUnion, arrayRemove, increment, where, getDocs, collectionGroup, limit, startAt, endAt, Timestamp, runTransaction } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";
import { getStorage, ref, uploadBytes, uploadBytesResumable, getDownloadURL, deleteObject } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-storage.js";
import { getFunctions, httpsCallable } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-functions.js";
import { normalizeReplyTarget, buildReplyRecord, groupCommentsByParent } from "/scripts/commentUtils.js";
import { buildTopBar, buildTopBarControls } from "/scripts/ui/topBar.js";
import { initializeLiveDiscover } from "/scripts/LiveDiscover.js";
import { NexeraGoLiveController } from "/scripts/GoLive.js";

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
const db = getFirestore(app);
const storage = getStorage(app);

// --- Global State & Cache ---
let currentUser = null;
let allPosts = [];
let userCache = {};
let userFetchPromises = {};
const USER_CACHE_TTL_MS = 10 * 60 * 1000;
window.myReviewCache = {}; // Global cache for reviews
let currentCategory = 'For You';
let currentProfileFilter = 'All Results';
let discoverFilter = 'All Results';
let discoverSearchTerm = '';
let discoverPostsSort = 'recent';
let discoverCategoriesMode = 'verified_first';
let savedSearchTerm = '';
let savedFilter = 'All Saved';

const GO_LIVE_MODE_STORAGE_KEY = 'nexera-go-live-mode';
let videoSearchTerm = '';
let videoFilter = 'All';
let videoSortMode = 'recent';
let liveSearchTerm = '';
let liveSortMode = 'featured';
let liveCategoryFilter = 'All';
let liveTagFilter = '';
let liveDiscoverInitialized = false;
let isInitialLoad = true;
let feedLoading = false;
let feedHydrationPromise = null;
let composerTags = [];
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
let mentionSearchTimer = null;
let currentThreadComments = [];
let liveSessionsCache = [];
let profileMediaPrefetching = {};

// Optimistic UI Sets
let followedCategories = new Set();
let followedCategoryList = [];
let followedUsers = new Set();
let followedTopicsUnsubscribe = null;
let followingUnsubscribe = null;

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
    messages: 'Messages',
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

function showSplash() {
    const splash = document.getElementById('nexera-splash');
    if (!splash) return;
    splash.style.display = 'flex';
    splash.classList.remove('nexera-splash-hidden');
    splash.style.pointerEvents = 'auto';
    splash.style.visibility = 'visible';
}

function hideSplash() {
    const splash = document.getElementById('nexera-splash');
    if (!splash) return;
    splash.classList.add('nexera-splash-hidden');
    splash.style.pointerEvents = 'none';
    const TRANSITION_BUFFER = 520;
    setTimeout(function () {
        splash.style.display = 'none';
    }, TRANSITION_BUFFER);
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
    return userLike.name || userLike.displayName || userLike.fullName || userLike.nickname || '';
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
        photoPath: data.photoPath || ''
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
let videosUnsubscribe = null;
let videosCache = [];
let videoObserver = null;
const viewedVideos = new Set();
let liveSessionsUnsubscribe = null;
let activeLiveSessionId = null;

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
            return function deregister() {
                if (!listeners.has(key)) return;
                const current = listeners.get(key);
                listeners.delete(key);
                try { current(); } catch (e) { console.warn('Listener cleanup failed for', key, e); }
            };
        },
        unregister(key) {
            if (!listeners.has(key)) return;
            const unsub = listeners.get(key);
            listeners.delete(key);
            try { unsub(); } catch (e) { console.warn('Listener cleanup failed for', key, e); }
        },
        clearAll() {
            listeners.forEach(function (unsub, key) {
                try { unsub(); } catch (e) { console.warn('Listener cleanup failed for', key, e); }
            });
            listeners.clear();
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
let staffRequestsUnsub = null;
let staffReportsUnsub = null;
let staffLogsUnsub = null;
let activeOptionsPost = null;
let threadComments = [];
let optimisticThreadComments = [];
let commentFilterMode = 'popularity';
let commentFilterQuery = '';

// --- Navigation Stack ---
let navStack = [];
let currentViewId = 'feed';
const MOBILE_VIEWPORT = typeof window !== 'undefined' && window.matchMedia ? window.matchMedia('(max-width: 820px)') : null;

function isMobileViewport() {
    return !!(MOBILE_VIEWPORT && MOBILE_VIEWPORT.matches);
}

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

const THEMES = {
    'For You': '#00f2ea', 'Following': '#ffffff', 'STEM': '#00f2ea',
    'History': '#ffd700', 'Coding': '#00ff41', 'Art': '#ff0050',
    'Random': '#bd00ff', 'Brainrot': '#ff00ff', 'Sports': '#ff4500',
    'Gaming': '#7000ff', 'News': '#ff3d3d', 'Music': '#00bfff'
};

const VERIFIED_TOPICS = [
    'STEM',
    'Coding',
    'Gaming',
    'Music',
    'Sports',
    'News',
    'History'
];

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
        if (readyResolver) readyResolver();
    };

    onAuthStateChanged(auth, async function (user) {
        const loadingOverlay = document.getElementById('loading-overlay');
        const authScreen = document.getElementById('auth-screen');
        const appLayout = document.getElementById('app-layout');

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

                        // Apply stored theme preference
                        const savedTheme = userProfile.theme || nexeraGetStoredThemePreference() || 'system';
                        userProfile.theme = savedTheme;
                        applyTheme(savedTheme);

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
                updateTimeCapsule();
                window.navigateTo('feed', false);
                renderProfile(); // Pre-render profile
            } else {
                currentUser = null;
                updateAuthClaims({});
                if (followedTopicsUnsubscribe) {
                    try { followedTopicsUnsubscribe(); } catch (err) { }
                    followedTopicsUnsubscribe = null;
                }
                if (followingUnsubscribe) {
                    try { followingUnsubscribe(); } catch (err) { }
                    followingUnsubscribe = null;
                }
                followedCategories = new Set();
                followedCategoryList = [];
                userProfile.followedCategories = [];
                followedUsers = new Set();
                userProfile.following = [];
                if (loadingOverlay) loadingOverlay.style.display = 'none';
                if (appLayout) appLayout.style.display = 'none';
                if (authScreen) authScreen.style.display = 'flex';
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
}

function showAnotherTimeCapsuleEvent() {
    updateTimeCapsule(true);
}

window.showAnotherTimeCapsuleEvent = showAnotherTimeCapsuleEvent;

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
        await setDoc(ref, {
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
        return await getDoc(ref);
    }

    await setDoc(ref, { updatedAt: now }, { merge: true });
    return await getDoc(ref);
}

async function backfillAvatarColorIfMissing(uid, profile = {}) {
    if (!uid || avatarColorBackfilled) return;
    if (!profile.avatarColor) {
        const color = computeAvatarColor(uid || profile.username || profile.name || 'user');
        profile.avatarColor = color;
        try {
            await setDoc(doc(db, 'users', uid), { avatarColor: color }, { merge: true });
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
        await setDoc(doc(db, "users", cred.user.uid), {
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
    const fetchPromises = Array.from(missingIds).map(function (uid) { return getDoc(doc(db, "users", uid)); });

    try {
        const userDocs = await Promise.all(fetchPromises);
        userDocs.forEach(function (docSnap) {
            if (docSnap.exists()) {
                storeUserInCache(docSnap.id, docSnap.data());
            } else {
                storeUserInCache(docSnap.id, { name: "Unknown User", username: "unknown" });
            }
        });

        // Re-render dependent views once data arrives
        renderFeed();
        if (activePostId) renderThreadMainPost(activePostId);
    } catch (e) {
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

async function loadFeedData({ showSplashDuringLoad = false } = {}) {
    if (feedLoading && feedHydrationPromise) return feedHydrationPromise;

    feedLoading = true;
    feedHydrationPromise = (async function () {
        if (showSplashDuringLoad) showSplash();
        const postsRef = collection(db, 'posts');
        const q = query(postsRef);
        const snapshot = await getDocs(q);
        const nextCache = {};
        allPosts = [];
        snapshot.forEach(function (docSnap) {
            const data = docSnap.data();
            const normalized = normalizePostData(docSnap.id, data);
            allPosts.push(normalized);
            nextCache[docSnap.id] = data;
        });

        allPosts.sort(function (a, b) { return (b.timestamp?.seconds || 0) - (a.timestamp?.seconds || 0); });

        if (currentUser) {
            const ownLocs = allPosts.filter(function (p) { return p.userId === currentUser.uid && p.location; }).map(function (p) { return p.location; });
            const merged = new Set([...(recentLocations || []), ...ownLocs]);
            recentLocations = Array.from(merged).slice(-10);
        }

        fetchMissingProfiles(allPosts);
        feedLoading = false;
        renderFeed();
        await waitForFeedMedia();
        postSnapshotCache = nextCache;
        isInitialLoad = false;
    })().catch(function (error) {
        console.error('Feed load failed', error);
    }).finally(function () {
        feedLoading = false;
        if (showSplashDuringLoad) hideSplash();
    });

    return feedHydrationPromise;
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
    if (verifiedEl) verifiedEl.style.display = currentCategoryDoc && currentCategoryDoc.verified ? 'inline' : 'none';
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
    selectedCategoryId = destination ? destination.id : null;
    renderDestinationField();
    renderDestinationPicker();
    syncPostButtonState();
    closeDestinationPicker();
}

function renderDestinationCreateArea() {
    const area = document.getElementById('destination-create-area');
    if (!area) return;

    if (destinationPickerTab !== 'community' || activeDestinationConfig.enableCreateCommunity === false) {
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

    if (destinationPickerError) {
        resultsEl.innerHTML = `<div class="destination-error">${destinationPickerError}<div style="margin-top:8px;"><button class="icon-pill" id="destination-retry-btn">Retry</button></div></div>`;
        const retryBtn = document.getElementById('destination-retry-btn');
        if (retryBtn) retryBtn.onclick = function () { retryDestinationLoad(); };
        return;
    }

    if (destinationPickerLoading) {
        resultsEl.innerHTML = '<div class="destination-loading"><div class="inline-spinner" style="display:block; margin: 0 auto 8px;"></div>Loading destinations...</div>';
        return;
    }

    const filtered = categories
        .filter(function (c) { return destinationPickerTab === 'official' ? c.type === 'official' : c.type === 'community'; })
        .filter(function (c) {
            return !destinationPickerSearch || (c.name || '').toLowerCase().includes(destinationPickerSearch.toLowerCase());
        })
        .sort(function (a, b) { return (a.name || '').localeCompare(b.name || ''); });

    if (!filtered.length) {
        const message = destinationPickerTab === 'official'
            ? 'No official destinations found.'
            : 'No communities found. Create one?';
        resultsEl.innerHTML = `<div class="destination-empty">${message}</div>`;
        return;
    }

    resultsEl.innerHTML = '';
    filtered.forEach(function (cat) {
        const destination = getDestinationFromCategory(cat);
        const isSelected = destination.id === selectedCategoryId;
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
        desc.textContent = destination.meta?.description || memberCount || '';

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
        searchInput.placeholder = destinationPickerTab === 'official' ? 'Search official destinations' : 'Search communities';
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
    destinationPickerOpen = true;

    const currentCategoryDoc = selectedCategoryId ? getCategorySnapshot(selectedCategoryId) : null;
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
    renderDestinationPicker();
}

window.openDestinationPicker = openDestinationPicker;
window.closeDestinationPicker = closeDestinationPicker;


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
    toast('Category created', 'info');
    return slug;
}

async function joinCategory(categoryId, role = 'member') {
    if (!requireAuth()) return;
    const catRef = doc(db, 'categories', categoryId);
    const membershipRef = doc(db, `categories/${categoryId}/members/${currentUser.uid}`);
    const userMembershipRef = doc(db, `users/${currentUser.uid}/categoryMemberships/${categoryId}`);
    const catSnap = await getDoc(catRef);
    if (!catSnap.exists()) return toast('Category missing', 'error');
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
        }, { merge: true }),
        updateDoc(catRef, { memberCount: increment(1) })
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
    const catRef = doc(db, 'categories', categoryId);

    await Promise.all([
        setDoc(membershipRef, { status: 'left', updatedAt: serverTimestamp(), updatedBy: currentUser.uid }, { merge: true }),
        setDoc(userMembershipRef, { status: 'left', updatedAt: serverTimestamp() }, { merge: true }),
        updateDoc(catRef, { memberCount: increment(-1) })
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
    const catRef = doc(db, 'categories', categoryId);

    await Promise.all([
        setDoc(membershipRef, { status: 'kicked', updatedAt: serverTimestamp(), updatedBy: currentUser.uid, reason }, { merge: true }),
        setDoc(userMembershipRef, { status: 'kicked', updatedAt: serverTimestamp(), reason }, { merge: true }),
        updateDoc(catRef, { memberCount: increment(-1) })
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
        pauseAllVideos();
        ListenerRegistry.unregister('videos:feed');
        videosUnsubscribe = null;
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
            activePost: activePostId
        });
    }

    // Toggle Views
    document.querySelectorAll('.view-section').forEach(function (el) { el.style.display = 'none'; });
    const targetView = document.getElementById('view-' + viewId);
    if (targetView) targetView.style.display = 'block';

    document.body.classList.toggle('sidebar-home', viewId === 'feed');

    // Toggle Navbar Active State
    if (viewId !== 'thread' && viewId !== 'public-profile') {
        document.querySelectorAll('.nav-item').forEach(function (el) { el.classList.remove('active'); });
        const navTarget = viewId === 'live-setup' ? 'live' : viewId;
        const navEl = document.getElementById('nav-' + navTarget);
        if (navEl) navEl.classList.add('active');
    }

    // View Specific Logic
    if (viewId === 'feed') {
        currentCategory = 'For You';
        renderFeed();
        loadFeedData();
    }
    if (viewId === 'saved') { renderSaved(); }
    if (viewId === 'profile') renderProfile();
    if (viewId === 'discover') { renderDiscover(); }
    if (viewId === 'messages') { releaseScrollLockIfSafe(); initConversations(); syncMobileMessagesShell(); } else { document.body.classList.remove('mobile-thread-open'); }
    if (viewId === 'videos') { initVideoFeed(); }
    if (viewId === 'live') {
        ensureLiveDiscoverRoot();
        if (!liveDiscoverInitialized) {
            initializeLiveDiscover();
            liveDiscoverInitialized = true;
        }
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
    if (!lockScroll && !goLiveLock) window.scrollTo(0, 0);
};

function ensureLiveDiscoverRoot() {
    let root = document.getElementById('live-discover-root');
    if (!root) {
        const liveView = document.getElementById('view-live');
        root = document.createElement('div');
        root.id = 'live-discover-root';
        root.className = 'live-container';
        if (liveView) {
            liveView.prepend(root);
        } else {
            document.body.appendChild(root);
        }
    }
    root.style.display = 'block';
    return root;
}

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

    window.navigateTo(prevState.view, false);

    // Re-render Views based on restored context
    if (prevState.view === 'feed') renderFeed();
    if (prevState.view === 'public-profile' && viewingUserId) {
        window.openUserProfile(viewingUserId, null, false);
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
            : [...followedCategoryList, topic];

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
function getPostHTML(post) {
    try {
        const date = formatDateTime(post.timestamp) || 'Just now';

        let authorData = userCache[post.userId] || { name: post.author, username: "loading...", photoURL: null };
        if (!authorData.name) authorData.name = "Unknown User";

        const verifiedBadge = renderVerifiedBadge(authorData);

        const avatarHtml = renderAvatar({ ...authorData, uid: post.userId }, { size: 42 });

        const isLiked = post.likedBy && post.likedBy.includes(currentUser.uid);
        const isDisliked = post.dislikedBy && post.dislikedBy.includes(currentUser.uid);
        const isSaved = userProfile.savedPosts && userProfile.savedPosts.includes(post.id);
        const isSelfPost = currentUser && post.userId === currentUser.uid;
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
        const scheduledChip = isPostScheduledInFuture(post) && currentUser && post.userId === currentUser.uid ? `<div class="scheduled-chip">Scheduled for ${formatTimestampDisplay(post.scheduledFor)}</div>` : '';
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
                <div style="margin-top:10px; padding:8px; background:rgba(255,255,255,0.05); border-radius:8px; font-size:0.85rem; color:var(--text-muted); display:flex; gap:6px;">
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

        return `
            <div id="post-card-${post.id}" class="social-card fade-in" style="border-left: 2px solid var(--card-accent); --card-accent: ${accentColor};">
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

function renderFeed(targetId = 'feed-content') {
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

    if (currentCategory === 'For You') {
        displayPosts = displayPosts.slice().sort(function (a, b) {
            const scoreDiff = getPostAffinityScore(b) - getPostAffinityScore(a);
            if (scoreDiff !== 0) return scoreDiff;
            return (b.timestamp?.seconds || 0) - (a.timestamp?.seconds || 0);
        });
    }

    if (displayPosts.length === 0) {
        const emptyLabel = feedLoading ? 'Loading Posts...' : 'No posts found.';
        container.innerHTML = `<div class="empty-state"><i class="ph ph-magnifying-glass" style="font-size:3rem; margin-bottom:1rem;"></i><p>${emptyLabel}</p></div>`;
        return;
    }

    displayPosts.forEach(post => {
        container.innerHTML += getPostHTML(post);
    });

    displayPosts.forEach(post => {
        const reviewBtn = document.querySelector(`#post-card-${post.id} .review-action`);
        const reviewValue = window.myReviewCache ? window.myReviewCache[post.id] : null;
        applyReviewButtonState(reviewBtn, reviewValue);
    });

    applyMyReviewStylesToDOM();
}


function refreshSinglePostUI(postId) {
    const post = allPosts.find(function (p) { return p.id === postId; });
    if (!post) return;

    const likeBtn = document.getElementById(`post-like-btn-${postId}`);
    const dislikeBtn = document.getElementById(`post-dislike-btn-${postId}`);
    const saveBtn = document.getElementById(`post-save-btn-${postId}`);
    const reviewBtn = document.querySelector(`#post-card-${postId} .review-action`);

    const isLiked = post.likedBy && post.likedBy.includes(currentUser.uid);
    const isDisliked = post.dislikedBy && post.dislikedBy.includes(currentUser.uid);
    const isSaved = userProfile.savedPosts && userProfile.savedPosts.includes(postId);
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
    return tag.trim().replace(/^#/, '').toLowerCase();
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
    composerMentions = [];
    composerPoll = { title: '', options: ['', ''] };
    composerScheduledFor = '';
    composerLocation = '';
    currentEditPost = null;
    renderComposerTags();
    renderComposerMentions();
    renderPollOptions();
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

function addComposerTag(tag = '') {
    const normalized = normalizeTagValue(tag);
    if (!normalized) return;
    if (composerTags.includes(normalized)) return;
    composerTags.push(normalized);
    renderComposerTags();
}

function removeComposerTag(tag = '') {
    composerTags = composerTags.filter(function (t) { return t !== tag; });
    renderComposerTags();
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

function filterTagSuggestions(queryText = '') {
    const listEl = document.getElementById('tag-suggestions');
    if (!listEl) return;
    const cleaned = normalizeTagValue(queryText);
    const known = getKnownTags();
    const matches = cleaned ? known.filter(function (t) { return t.includes(cleaned) && !composerTags.includes(t); }).slice(0, 5) : known.slice(0, 5);
    if (!matches.length) {
        listEl.innerHTML = '';
        listEl.style.display = 'none';
        return;
    }
    listEl.style.display = 'block';
    listEl.innerHTML = matches.map(function (tag) {
        return `<button type="button" class="suggestion-chip" onclick="window.addComposerTag('${tag}')">#${escapeHtml(tag)}</button>`;
    }).join('');
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
    }
}

function handleTagInputKey(event) {
    if (event.key === 'Enter') {
        event.preventDefault();
        const input = event.target;
        addComposerTag(input.value || '');
        input.value = '';
        filterTagSuggestions('');
    } else {
        filterTagSuggestions(event.target.value || '');
    }
}

function normalizeMentionEntry(raw = {}) {
    if (typeof raw === 'string') {
        return { username: raw.replace(/^@/, '').toLowerCase(), uid: raw.uid || null };
    }
    const username = (raw.username || raw.handle || '').replace(/^@/, '').toLowerCase();
    const displayName = raw.displayName || raw.nickname || raw.name || '';
    const uid = raw.uid || raw.userId || null;
    const accountRoles = Array.isArray(raw.accountRoles) ? raw.accountRoles : (raw.role ? [raw.role] : []);
    const verified = accountRoles.includes('verified');
    return { username, uid, displayName, photoURL: raw.photoURL || '', avatarColor: raw.avatarColor || '', accountRoles, verified };
}

function addComposerMention(rawUser) {
    const normalized = normalizeMentionEntry(rawUser);
    if (!normalized.username) return;
    if (composerMentions.some(function (m) { return m.username === normalized.username; })) return;
    composerMentions.push(normalized);
    renderComposerMentions();
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

async function searchMentionSuggestions(term = '') {
    const listEl = document.getElementById('mention-suggestions');
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
            return `<button type="button" class="mention-suggestion" onclick='window.addComposerMention(${JSON.stringify({
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
        console.warn('Mention search failed', err);
        listEl.innerHTML = '';
        listEl.style.display = 'none';
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

function escapeRegex(str = '') {
    return (str || '').replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&');
}

function renderTagList(tags = []) {
    if (!tags.length) return '';
    return `<div style="margin-top:8px;">${tags.map(function (tag) { return `<span class="tag-chip">#${escapeHtml(tag)}</span>`; }).join('')}</div>`;
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
        const regex = new RegExp('#' + escapeRegex(tag), 'gi');
        safe = safe.replace(regex, `<span class="tag-chip">#${escapeHtml(tag)}</span>`);
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

async function notifyMentionedUsers(resolved = [], postId) {
    const tasks = resolved.map(function (entry) {
        const notifRef = collection(db, 'users', entry.uid, 'notifications');
        return addDoc(notifRef, {
            type: 'mention',
            postId,
            fromUserId: currentUser.uid,
            createdAt: serverTimestamp(),
            read: false
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
    const tags = Array.from(new Set((composerTags || []).map(normalizeTagValue).filter(Boolean)));
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
            const joined = await ensureJoinedCategory(targetCategoryId, currentUser.uid);
            if (!joined) {
                setComposerError('Unable to join this category. Please try again.');
                return;
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
            if (notificationTargets.length) await notifyMentionedUsers(notificationTargets, postRef.id);
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

    const updates = { name, realName, nickname, username, bio, links, phone, gender, email, region, theme, photoURL, photoPath };
    userProfile = { ...userProfile, ...updates };
    storeUserInCache(currentUser.uid, userProfile);

    try {
        await setDoc(doc(db, "users", currentUser.uid), updates, { merge: true });
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
        await setDoc(doc(db, 'users', currentUser.uid), { photoURL: '', photoPath: '' }, { merge: true });
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
window.openThread = function (postId) {
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

    const avatarHtml = renderAvatar({ ...cAuthor, uid: c.userId }, { size: 36 });
    const username = cAuthor.username ? `@${escapeHtml(cAuthor.username)}` : '';
    const timestampText = formatDateTime(c.timestamp) || 'Now';

    const parentCommentId = c.parentCommentId || c.parentId;

    const mediaHtml = c.mediaUrl
        ? `<div onclick="window.openFullscreenMedia('${c.mediaUrl}', 'image')">
         <img src="${c.mediaUrl}" style="max-width:200px; border-radius:8px; margin-top:5px; cursor:pointer;">
       </div>`
        : "";

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

    const isLiked = post.likedBy && post.likedBy.includes(currentUser.uid);
    const isDisliked = post.dislikedBy && post.dislikedBy.includes(currentUser.uid);
    const isSaved = userProfile.savedPosts && userProfile.savedPosts.includes(postId);
    const isFollowingUser = followedUsers.has(post.userId);
    const isFollowingTopic = followedCategories.has(post.category);
    const isSelfPost = currentUser && post.userId === currentUser.uid;
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
    const scheduledChip = isPostScheduledInFuture(post) && currentUser && post.userId === currentUser.uid ? `<div class="scheduled-chip">Scheduled for ${formatTimestampDisplay(post.scheduledFor)}</div>` : '';
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
            { label: 'Categories', dataset: { filter: 'Categories' }, active: discoverFilter === 'Categories', onClick: function () { window.setDiscoverFilter('Categories'); } },
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
                label: 'Categories:',
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
        ]
    });
    container.innerHTML = '';
    container.appendChild(topBar);
}

window.renderDiscover = async function () {
    renderDiscoverTopBar();
    const container = document.getElementById('discover-results');
    container.innerHTML = "";

    const postsSelect = document.getElementById('posts-sort-select');
    if (postsSelect) postsSelect.value = discoverPostsSort;
    const categoriesSelect = document.getElementById('categories-sort-select');
    if (categoriesSelect) categoriesSelect.value = discoverCategoriesMode;

    const categoriesDropdown = function (id = 'section') {
        return `<div class="discover-dropdown"><label for="categories-${id}-select">Categories:</label><select id="categories-${id}-select" class="discover-select" onchange="window.handleCategoriesModeChange(event)">
            <option value="verified_first" ${discoverCategoriesMode === 'verified_first' ? 'selected' : ''}>Verified first</option>
            <option value="verified_only" ${discoverCategoriesMode === 'verified_only' ? 'selected' : ''}>Verified only</option>
            <option value="community_first" ${discoverCategoriesMode === 'community_first' ? 'selected' : ''}>Community first</option>
            <option value="community_only" ${discoverCategoriesMode === 'community_only' ? 'selected' : ''}>Community only</option>
        </select></div>`;
    };

    const renderVideosSection = async function (onlyVideos = false) {
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

        container.innerHTML += `<div class="discover-section-header">Videos</div>`;
        filteredVideos.forEach(function (video) {
            const tags = (video.hashtags || []).map(function (t) { return '#' + t; }).join(' ');
            container.innerHTML += `
                <div class="social-card" style="padding:1rem; cursor:pointer; display:flex; gap:12px; align-items:flex-start;" onclick="window.navigateTo('videos');">
                    <div style="width:120px; height:70px; background:linear-gradient(135deg, #0f1f3a, #0adfe4); border-radius:10px; display:flex; align-items:center; justify-content:center; color:#aaf; font-weight:700;">
                        <i class="ph-fill ph-play-circle" style="font-size:2rem;"></i>
                    </div>
                    <div style="flex:1;">
                        <div style="font-weight:800; margin-bottom:4px;">${escapeHtml(video.caption || 'Untitled video')}</div>
                        <div style="color:var(--text-muted); font-size:0.9rem;">${tags}</div>
                        <div style="color:var(--text-muted); font-size:0.8rem; margin-top:4px;">Views: ${video.stats?.views || 0}</div>
                    </div>
                </div>`;
        });
    };

    const renderUsers = function () {
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
            container.innerHTML += `<div class="discover-section-header">Users</div>`;
            matches.forEach(function (user) {
                const uid = Object.keys(userCache).find(function (key) { return userCache[key] === user; });
                if (!uid) return;
                const avatarHtml = renderAvatar({ ...user, uid }, { size: 40 });

                container.innerHTML += `
                    <div class="social-card" style="padding:1rem; cursor:pointer; display:flex; align-items:center; gap:10px; border-left: 4px solid var(--border);" onclick="window.openUserProfile('${uid}')">
                        ${avatarHtml}
                        <div>
                            <div style="font-weight:700;">${escapeHtml(user.name)}</div>
                            <div style="color:var(--text-muted); font-size:0.9rem;">@${escapeHtml(user.username)}</div>
                        </div>
                        <button class="follow-btn" style="margin-left:auto; padding:10px;">View</button>
                    </div>`;
            });
        } else if (discoverFilter === 'Users' && discoverSearchTerm) {
            container.innerHTML = `<div class="empty-state"><p>No users matching "${discoverSearchTerm}"</p></div>`;
        }
    };

    const renderLiveSection = function () {
        if (MOCK_LIVESTREAMS.length > 0) {
            container.innerHTML += `<div class="discover-section-header">Livestreams</div>`;
            MOCK_LIVESTREAMS.forEach(function (stream) {
                container.innerHTML += `
                    <div class="social-card" style="padding:1rem; display:flex; gap:10px; border-left: 4px solid ${stream.color};">
                        <div style="width:80px; height:50px; background:${stream.color}; border-radius:6px; display:flex; align-items:center; justify-content:center; color:black; font-weight:900; font-size:1.5rem;"><i class="ph-fill ph-broadcast" style="margin-right:8px;"></i> LIVE</div>
                        <div style="padding:1rem;">
                            <h3 style="font-weight:700; font-size:1.1rem; margin-bottom:5px; color:var(--text-main);">${stream.title}</h3>
                            <div style="display:flex; justify-content:space-between; align-items:center;">
                                <div style="font-size:0.9rem; color:var(--text-muted);">@${stream.author}</div>
                                <div style="color:#ff3d3d; font-weight:bold; font-size:0.8rem; display:flex; align-items:center; gap:4px;"><i class="ph-fill ph-circle"></i> ${stream.viewerCount}</div>
                            </div>
                            <div class="category-badge" style="margin-top:10px;">${stream.category}</div>
                        </div>
                    </div>`;
            });
        }
    };

    const renderPostsSection = function () {
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
            container.innerHTML += `<div class="discover-section-header">Posts</div>`;
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
                container.innerHTML += `
                    <div class="social-card" style="border-left: 2px solid var(--card-accent); --card-accent: ${accentColor}; cursor:pointer;" onclick="window.openThread('${post.id}')">
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
                    </div>`;
            });
        } else if (discoverFilter === 'Posts' && discoverSearchTerm) {
            container.innerHTML = `<div class="empty-state"><p>No posts found.</p></div>`;
        }
    };

    const renderCategoriesSection = function (onlyCategories = false) {
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
            container.innerHTML += `<div class="discover-section-header discover-section-row"><span>Categories</span>${categoriesDropdown('section')}</div>`;
            visible.forEach(function (cat) {
                const verifiedMark = renderVerifiedBadge({ verified: cat.verified });
                const typeLabel = (cat.type || 'community') === 'community' ? 'Community' : 'Official';
                const memberLabel = typeof cat.memberCount === 'number' ? `${cat.memberCount} members` : '';
                const topicLabel = cat.name || cat.slug || cat.id || 'Category';
                const topicClass = topicLabel.replace(/[^a-zA-Z0-9]/g, '');
                const isFollowingTopic = followedCategories.has(topicLabel);
                const topicArg = topicLabel.replace(/'/g, "\\'");
                const followLabel = isFollowingTopic ? 'Following' : '<i class="ph-bold ph-plus"></i> Topic';
                const followClass = isFollowingTopic ? 'following' : '';
                const followButton = `<button class="follow-btn js-follow-topic-${topicClass} ${followClass}" data-topic="${escapeHtml(topicLabel)}" onclick="event.stopPropagation(); window.toggleFollow('${topicArg}', event)" style="padding:8px 12px;">${followLabel}</button>`;
                const accentColor = cat.verified ? '#00f2ea' : 'var(--border)';
                container.innerHTML += `
                    <div class="social-card" style="padding:1rem; display:flex; gap:12px; align-items:center; border-left: 2px solid var(--card-accent); --card-accent: ${accentColor};">
                        <div class="user-avatar" style="width:46px; height:46px; background:${getColorForUser(cat.name || 'C')};">${(cat.name || 'C')[0]}</div>
                        <div style="flex:1;">
                            <div style="font-weight:800; display:flex; align-items:center; gap:6px;">${escapeHtml(cat.name || 'Category')}${verifiedMark}</div>
                            <div style="color:var(--text-muted); font-size:0.9rem; display:flex; gap:10px; align-items:center; flex-wrap:wrap;">${escapeHtml(typeLabel)}${memberLabel ? ' · ' + memberLabel : ''}</div>
                        </div>
                        <div style="display:flex; flex-direction:column; align-items:flex-end; gap:10px;">
                            <div class="category-badge">${escapeHtml(cat.slug || cat.id || '')}</div>
                            ${followButton}
                        </div>
                    </div>`;
            });
        } else if (discoverFilter === 'Categories' && discoverSearchTerm) {
            container.innerHTML = `<div class="empty-state"><p>No categories found.</p></div>`;
        }
    };

    if (discoverFilter === 'All Results') {
        renderLiveSection();
        renderUsers();
        renderPostsSection();
        renderCategoriesSection();
        await renderVideosSection();
        if (container.innerHTML === "") container.innerHTML = `<div class="empty-state"><p>Start typing to search everything.</p></div>`;
    } else if (discoverFilter === 'Users') {
        renderUsers();
    } else if (discoverFilter === 'Livestreams') {
        renderLiveSection();
    } else if (discoverFilter === 'Videos') {
        await renderVideosSection(true);
    } else if (discoverFilter === 'Categories') {
        renderCategoriesSection(true);
    } else {
        renderPostsSection();
    }

    applyMyReviewStylesToDOM();
}

// --- Profile Rendering ---
window.openUserProfile = async function (uid, event, pushToStack = true) {
    if (event) event.stopPropagation();
    if (uid === currentUser.uid) {
        window.navigateTo('profile', pushToStack);
        return;
    }

    viewingUserId = uid;
    currentProfileFilter = 'All Results';
    window.navigateTo('public-profile', pushToStack);

    let profile = userCache[uid];
    if (!profile) {
        const docSnap = await getDoc(doc(db, "users", uid));
        if (docSnap.exists()) {
            profile = storeUserInCache(uid, docSnap.data());
        } else {
            profile = { name: "Unknown User", username: "unknown" };
        }
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
    const q = query(collection(db, 'users'), where('username', '==', normalized));
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
        return `<button class="discover-pill ${active ? 'active' : ''}" role="tab" aria-selected="${active}" onclick="window.setProfileFilter('${safeLabel}', '${uid}')">${label}</button>`;
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
                    name: snapshot.name || snapshot.title || snapshot.slug || 'Category',
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

function renderProfileVideoCard(video) {
    const poster = video.thumbURL || video.videoURL || '';
    const caption = escapeHtml(video.caption || 'Untitled video');
    const views = video.stats?.views || 0;
    return `<div class="social-card profile-collage-card" style="min-width:240px;">
        <div class="profile-video-thumb" style="background-image:url('${poster}')">
            <div class="profile-video-meta">${views} views</div>
        </div>
        <div class="card-content" style="gap:6px;">
            <div style="font-weight:700;">${caption}</div>
            <div style="color:var(--text-muted); font-size:0.85rem;">${(video.hashtags || []).map(function (t) { return '#' + t; }).join(' ')}</div>
        </div>
    </div>`;
}

function renderProfileLiveCard(session) {
    const title = escapeHtml(session.title || 'Live session');
    const status = (session.status || 'live').toUpperCase();
    const viewers = session.viewerCount || session.stats?.viewerCount || 0;
    return `<div class="social-card profile-collage-card" style="min-width:220px;">
        <div class="card-content" style="gap:6px;">
            <div style="display:flex; align-items:center; gap:8px;">${status === 'LIVE' ? '<span class="live-dot"></span>' : ''}<span style="font-weight:700;">${title}</span></div>
            <div style="color:var(--text-muted); font-size:0.85rem;">${viewers} watching</div>
        </div>
    </div>`;
}

function renderProfileCategoryChip(category) {
    return `<div class="category-badge" style="min-width:max-content;">${escapeHtml(category.name || 'Category')}</div>`;
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
    sections.push(renderProfileCollageRow('Videos', sources.videos.slice(0, 10), renderProfileVideoCard, `window.setProfileFilter('Videos', '${uid}')`));
    sections.push(renderProfileCollageRow('Livestreams', sources.liveSessions.slice(0, 10), renderProfileLiveCard, `window.setProfileFilter('Livestreams', '${uid}')`));
    sections.push(renderProfileCollageRow('Categories', sources.categories.slice(0, 12), renderProfileCategoryChip, `window.setProfileFilter('Categories', '${uid}')`));

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
        container.innerHTML = `<div class="profile-h-scroll">${sources.videos.map(renderProfileVideoCard).join('')}</div>`;
        return;
    }
    if (currentProfileFilter === 'Livestreams') {
        if (!sources.liveSessions.length) return container.innerHTML = `<div class="empty-state"><p>No livestreams yet.</p></div>`;
        container.innerHTML = `<div class="profile-h-scroll">${sources.liveSessions.map(renderProfileLiveCard).join('')}</div>`;
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
        <div class="profile-header" style="padding-top:1rem;">
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
        <div class="profile-filters-bar">${renderProfileFilterRow(uid, 'Public profile filters')}</div>
        <div id="public-profile-content" class="profile-content-region"></div>`;

    renderProfileContent(uid, normalizedProfile, isSelfView, 'public-profile-content');
}

function renderProfile() {
    if (!PROFILE_FILTER_OPTIONS.includes(currentProfileFilter)) currentProfileFilter = 'All Results';
    const sources = getProfileContentSources(currentUser?.uid);
    const userPosts = sources.posts;
    const displayName = userProfile.name || userProfile.nickname || "Nexera User";
    const verifiedBadge = renderVerifiedBadge(userProfile, 'with-gap');
    const avatarHtml = renderAvatar({ ...userProfile, uid: currentUser?.uid }, { size: 100, className: 'profile-pic' });

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
        <div class="profile-header">
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
        <div class="profile-filters-bar">${renderProfileFilterRow('me', 'Profile filters')}</div>
        <div id="my-profile-content" class="profile-content-region"></div>
    `;

    renderProfileContent(currentUser.uid, userProfile, true, 'my-profile-content');
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

function renderCategoryPills() {
    const header = document.getElementById('category-header');
    if (!header) return;
    header.innerHTML = '';

    const anchors = ['For You', 'Following'];
    const seen = new Set(anchors.map(function (label) { return label.toLowerCase(); }));

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
        pill.onclick = function () { window.setCategory(label); };
        header.appendChild(pill);
    });

    const divider = document.createElement('div');
    divider.className = 'category-divider';
    header.appendChild(divider);

    const dynamicTopics = [];
    const normalizedVerifiedSet = new Set(VERIFIED_TOPICS.map(function (t) { return (t || '').toLowerCase(); }));

    VERIFIED_TOPICS.forEach(function (name) { addTopic(dynamicTopics, name, true); });

    const followedNames = collectFollowedCategoryNames();
    followedNames.forEach(function (name) { addTopic(dynamicTopics, name, normalizedVerifiedSet.has((name || '').toLowerCase())); });

    if (!followedNames.length) {
        computeTrendingCategories(20).forEach(function (name) {
            addTopic(dynamicTopics, name, normalizedVerifiedSet.has((name || '').toLowerCase()));
        });
    }

    const dynamicFull = dynamicTopics;
    const dynamic = dynamicFull.slice(0, categoryVisibleCount);

    dynamic.forEach(function (topic) {
        const pill = document.createElement('div');
        pill.className = 'category-pill' + (currentCategory === topic.name ? ' active' : '') + (topic.verified ? ' verified-topic' : '');
        pill.innerHTML = `${escapeHtml(topic.name)}${topic.verified ? `<span class="topic-verified-icon">${getVerifiedIconSvg()}</span>` : ''}`;
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
}

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
function renderSaved() { currentCategory = 'Saved'; renderFeed('saved-content'); }

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
window.handlePostsSortChange = function (e) { discoverPostsSort = e.target.value; renderDiscover(); }
window.handleCategoriesModeChange = function (e) { discoverCategoriesMode = e.target.value; renderDiscover(); }
window.handleSearchInput = function (e) { discoverSearchTerm = e.target.value.toLowerCase(); renderDiscover(); }
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
window.submitReport = async function () { if (!requireAuth()) return; if (!activeOptionsPost || !activeOptionsPost.id || !activeOptionsPost.ownerId) return toast('No post selected', 'error'); const categoryEl = document.getElementById('report-category'); const detailEl = document.getElementById('report-details'); const category = categoryEl ? categoryEl.value : ''; const details = detailEl ? detailEl.value.trim().substring(0, 500) : ''; if (!category) return toast('Please choose a category.', 'error'); try { await addDoc(collection(db, 'reports'), { postId: activeOptionsPost.id, reportedUserId: activeOptionsPost.ownerId, reporterUserId: currentUser.uid, category, details, createdAt: serverTimestamp(), context: activeOptionsPost.context || currentViewId, type: 'post', reason: details }); if (detailEl) detailEl.value = ''; if (categoryEl) categoryEl.value = ''; window.closeReportModal(); toast('Report submitted', 'info'); } catch (e) { console.error(e); toast('Could not submit report.', 'error'); } }
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

function buildUnknownUserProfile(uid) {
    return storeUserInCache(uid, {
        username: 'user',
        displayName: 'Unknown user',
        name: 'Unknown user',
        photoURL: '',
        avatarColor: computeAvatarColor(uid || 'user')
    });
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
            const snap = await getDoc(doc(db, 'users', uid));
            if (snap.exists()) {
                return storeUserInCache(uid, snap.data());
            }
        } catch (e) {
            console.warn('User fetch failed', uid, e?.message || e);
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

function renderConversationList() {
    const listEl = document.getElementById('conversation-list');
    if (!listEl) return;
    listEl.innerHTML = '';

    if (conversationMappings.length === 0) {
        listEl.innerHTML = '<div class="empty-state">No conversations yet.</div>';
        return;
    }

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

    orderedMappings.forEach(function (mapping) {
        const details = conversationDetailsCache[mapping.id] || {};
        const participants = details.participants || mapping.otherParticipantIds || [];
        const meta = deriveOtherParticipantMeta(participants, currentUser.uid, details);
        const otherId = meta.otherIds?.[0] || mapping.otherParticipantIds?.[0];
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
        let avatarUser = {
            uid: mapping.id || otherId || 'conversation',
            username: name,
            displayName: name,
            photoURL: details.avatarURL || '',
            avatarColor: computeAvatarColor(name)
        };
        if (!isGroup && otherId) {
            avatarUser = {
                ...otherProfile,
                uid: otherId,
                username: otherProfile?.username || name,
                displayName: resolveDisplayName(otherProfile) || name,
                photoURL: otherProfile?.photoURL || details.avatarURL || mapping.otherParticipantAvatars?.[0] || meta.avatars?.[0] || '',
                avatarColor: otherProfile?.avatarColor || meta.colors?.[0] || computeAvatarColor(otherProfile?.username || otherId)
            };
        } else if (details.avatarURL || mapping.otherParticipantAvatars?.length || meta.avatars?.length) {
            avatarUser.photoURL = details.avatarURL || mapping.otherParticipantAvatars?.[0] || meta.avatars?.[0] || '';
        }
        const avatarHtml = renderAvatar(avatarUser, { size: 42 });

        const item = document.createElement('div');
        item.className = 'conversation-item' + (activeConversationId === mapping.id ? ' active' : '');
        const unread = mapping.unreadCount || 0;
        const badges = [];
        const muteState = resolveMuteState(mapping.id, mapping);
        const isMuted = muteState.active || (details.mutedBy || []).includes(currentUser.uid);
        updateConversationMappingState(mapping.id, { muted: isMuted, muteUntil: muteState.until || null });
        if (mapping.pinned) badges.push('<i class="ph ph-push-pin"></i>');
        if (isMuted) badges.push('<i class="ph ph-bell-slash"></i>');
        if (mapping.archived) badges.push('<i class="ph ph-archive"></i>');
        const flagHtml = badges.length || unread > 0
            ? `<div class="conversation-flags">
                    ${badges.length ? `<span style="display:inline-flex; gap:4px; color:var(--text-muted);">${badges.join('')}</span>` : ''}
                    ${unread > 0 ? `<span class="badge">${unread}</span>` : ''}
               </div>`
            : '';
        const previewText = escapeHtml(mapping.lastMessagePreview || details.lastMessagePreview || 'Start a chat');
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
        listEl.appendChild(item);
    });
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
    let avatarUser = {
        uid: cid || primaryOtherId || 'conversation',
        username: label,
        displayName: label,
        photoURL: convo.avatarURL || meta.avatars?.[0] || '',
        avatarColor: convo.avatarColor || computeAvatarColor(label)
    };

    if (primaryOtherId && !convo.avatarURL) {
        const otherMeta = resolveParticipantDisplay(convo, primaryOtherId);
        avatarUser = {
            ...otherMeta.profile,
            uid: primaryOtherId,
            username: otherMeta.username || label,
            displayName: otherMeta.displayName || label,
            photoURL: otherMeta.avatar,
            avatarColor: otherMeta.avatarColor
        };
    }

    const avatar = renderAvatar(avatarUser, { size: 36 });
    const verifiedBadge = (!convo.title && participants.length === 2) ? renderVerifiedBadge(avatarUser) : '';
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

    header.innerHTML = `<div class="message-header-shell">
        <button ${profileBtnAttrs}>
            ${avatar}
            <div>
                <div class="message-thread-title-row">${escapeHtml(label)}${verifiedBadge}</div>
                <div class="message-thread-subtitle">${subtitle}</div>
            </div>
        </button>
        <div class="message-header-actions">
            <button class="icon-pill" onclick="window.openConversationSettings('${cid || ''}')" aria-label="Conversation options"><i class="ph ph-dots-three-outline"></i></button>
        </div>
    </div>`;
}

function renderMessages(msgs = [], convo = {}) {
    const body = document.getElementById('message-thread');
    if (!body) return;
    const shouldStickToBottom = isNearBottom(body);
    const previousOffset = body.scrollHeight - body.scrollTop;
    body.innerHTML = '';

    let lastTimestamp = null;
    let lastDateDivider = null;
    let lastSenderId = null;
    let latestSelfMessage = null;
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

        const attachments = Array.isArray(msg.attachments) ? msg.attachments.slice() : [];
        if (msg.mediaURL && !attachments.length) {
            attachments.push({ url: msg.mediaURL, type: msg.mediaType || msg.type, name: msg.fileName || 'Attachment' });
        }
        const hasMediaAttachment = attachments.length > 0;

        if (!hasMediaAttachment && msg.type === 'image' && msg.mediaURL) {
            content = `<img src="${msg.mediaURL}" style="max-width:240px; border-radius:12px;">`;
        } else if (!hasMediaAttachment && msg.type === 'video' && msg.mediaURL) {
            content = `<video src="${msg.mediaURL}" controls style="max-width:260px; border-radius:12px;"></video>`;
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
                if (isImage) {
                    const img = document.createElement('img');
                    img.src = att.url;
                    img.alt = att.name || 'Attachment';
                    tile.appendChild(img);
                } else if ((att.type || '').includes('video')) {
                    tile.innerHTML = '<div class="attachment-icon"><i class="ph ph-play"></i></div>';
                } else {
                    tile.innerHTML = '<div class="attachment-icon"><i class="ph ph-paperclip"></i></div>';
                }
                tile.onclick = function (e) { e.stopPropagation(); openFullscreenMedia(att.url, (att.type || '').includes('video') ? 'video' : 'image'); };
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
    if (shouldStickToBottom) {
        body.scrollTop = body.scrollHeight;
    } else {
        body.scrollTop = Math.max(0, body.scrollHeight - previousOffset);
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

function clearReplyContext() {
    activeReplyContext = null;
    editingMessageId = null;
    const bar = document.getElementById('message-reply-preview');
    const compose = document.querySelector('.message-compose');
    if (bar) { bar.style.display = 'none'; bar.innerHTML = ''; }
    if (compose) compose.classList.remove('editing');
}

function clearAttachmentPreview() {
    const preview = document.getElementById('message-attachment-preview');
    pendingMessageAttachments = [];
    if (preview) { preview.innerHTML = ''; preview.style.display = 'none'; }
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
}

function removePendingAttachment(index) {
    if (index < 0 || index >= pendingMessageAttachments.length) return;
    pendingMessageAttachments.splice(index, 1);
    renderAttachmentPreview();
}

function handleMessageFileChange(event) {
    const files = Array.from(event?.target?.files || []);
    if (!files.length) return;
    pendingMessageAttachments = pendingMessageAttachments.concat(files);
    renderAttachmentPreview();
    if (event?.target) event.target.value = '';
}

window.handleMessageFileChange = handleMessageFileChange;
window.removePendingAttachment = removePendingAttachment;

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
    if (message.type === 'image' && message.mediaURL) return { url: message.mediaURL, type: 'image' };
    const attachments = Array.isArray(message.attachments) ? message.attachments : [];
    const imageAttachment = attachments.find(function (att) { return (att.type || '').includes('image') && att.url; });
    if (imageAttachment) return { url: imageAttachment.url, type: 'image' };
    if (message.mediaURL && ((message.mediaType || message.type || '').includes('image'))) return { url: message.mediaURL, type: 'image' };
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
        setTypingState(conversationId, hasText);
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
        console.warn('Conversation details listener error', err?.message || err);
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
            avatarURL: null,
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
        console.warn('Messages listener error', err?.message || err);
        handleConversationAccessLoss(convoId);
    }));
}

async function openConversation(conversationId) {
    if (!conversationId || !requireAuth()) return;
    if (activeConversationId && activeConversationId !== conversationId) {
        setTypingState(activeConversationId, false);
    }
    activeConversationId = conversationId;
    clearReplyContext();
    conversationSearchTerm = '';
    const searchInput = document.getElementById('conversation-search');
    if (searchInput) searchInput.value = '';
    const body = document.getElementById('message-thread');
    if (body) body.innerHTML = '';
    const header = document.getElementById('message-header');
    if (header) header.textContent = 'Loading conversation...';

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

    refreshConversationUsers(convo, { force: true, updateUI: true });
    renderMessageHeader(convo);
    renderTypingIndicator(convo);
    listenToConversationDetails(conversationId);
    attachMessageInputHandlers(conversationId);
    setTypingState(conversationId, false);
    await listenToMessages(conversationId);
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
        console.warn('Conversation list listener error', err?.message || err);
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
    const src = previewUrl || convo.avatarURL || meta.avatars?.[0] || '';
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
            photoURL: convo.avatarURL || meta.avatars?.[0] || '',
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
            const uploadRef = ref(storage, path);
            const snap = await uploadBytes(uploadRef, file);
            const url = await getDownloadURL(snap.ref);
            await updateDoc(doc(db, 'conversations', conversationSettingsId), { avatarURL: url, updatedAt: serverTimestamp() });
            conversationDetailsCache[conversationSettingsId] = { ...(conversationDetailsCache[conversationSettingsId] || {}), avatarURL: url };
            if (activeConversationId === conversationSettingsId) renderMessageHeader(conversationDetailsCache[conversationSettingsId]);
            await refreshConversationSettings(conversationSettingsId);
            toast('Conversation image updated', 'info');
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
        systemPayload: payload.systemPayload || null
    };

    await addDoc(collection(db, 'conversations', conversationId, 'messages'), message);
    await updateConversationUnread(conversationId, participants, payload);
}

async function uploadAttachments(conversationId, files = []) {
    const uploads = [];
    let idx = 0;
    for (const file of files) {
        if (!file) continue;
        const stamp = Date.now();
        const storageRef = ref(storage, `dm_media/${conversationId}/${stamp}_${idx}_${file.name}`);
        const uploadTask = uploadBytesResumable(storageRef, file);
        await uploadTask;
        const mediaURL = await getDownloadURL(uploadTask.snapshot.ref);
        uploads.push({ url: mediaURL, type: file.type || '', name: file.name || 'Attachment' });
        idx += 1;
    }
    return uploads;
}

window.sendMessage = async function (conversationId = activeConversationId) {
    if (!conversationId || !requireAuth()) return;
    const input = document.getElementById('message-input');
    const text = (input?.value || '').trim();
    const fileInput = document.getElementById('message-media');
    const directFiles = Array.from(fileInput?.files || []);
    const combinedFiles = pendingMessageAttachments.concat(directFiles);
    if (!text && !combinedFiles.length) return;

    if (editingMessageId && !combinedFiles.length) {
        try {
            await updateDoc(doc(db, 'conversations', conversationId, 'messages', editingMessageId), { text, editedAt: serverTimestamp() });
        } catch (e) { console.warn('Edit failed', e?.message || e); toast('Unable to edit message', 'error'); }
        editingMessageId = null;
    } else {
        let attachments = [];
        if (combinedFiles.length) {
            attachments = await uploadAttachments(conversationId, combinedFiles);
        }
        await sendChatPayload(conversationId, {
            text,
            attachments,
            mediaURL: attachments.length === 1 && (attachments[0].type || '').includes('image') ? attachments[0].url : null,
            mediaType: attachments.length === 1 ? attachments[0].type : null,
            type: attachments.length ? (attachments.some(function (att) { return (att.type || '').includes('video'); }) ? 'video' : 'image') : 'text'
        });
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
    const uploads = await uploadAttachments(conversationId, Array.from(fileInput.files));
    const hasVideo = uploads.some(function (att) { return (att.type || '').includes('video'); });
    await sendChatPayload(conversationId, {
        text: caption,
        attachments: uploads,
        mediaURL: uploads.length === 1 && (uploads[0].type || '').includes('image') ? uploads[0].url : null,
        mediaType: uploads.length === 1 ? uploads[0].type : null,
        type: uploads.length ? (hasVideo ? 'video' : 'image') : 'text'
    });
    fileInput.value = '';
    clearAttachmentPreview();
    const input = document.getElementById('message-input');
    if (input && caption) input.value = '';
    setTypingState(conversationId, false);
};

window.handleConversationSearch = handleConversationSearch;
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
        avatarURL: null,
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
    window.navigateTo('messages');
    await initConversations();
};

window.sharePost = async function (postId, event) {
    if (event) event.stopPropagation();
    const base = window.location.href.split('#')[0];
    const url = `${base}#thread-${postId}`;

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
window.openVideoUploadModal = function () { return window.toggleVideoUploadModal(true); };
window.toggleVideoUploadModal = function (show = true) {
    const modal = document.getElementById('video-upload-modal');
    if (modal) modal.style.display = show ? 'flex' : 'none';
};

function ensureVideoObserver() {
    if (videoObserver) return videoObserver;
    videoObserver = new IntersectionObserver(function (entries) {
        entries.forEach(function (entry) {
            const vid = entry.target;
            if (entry.isIntersecting) {
                vid.play().catch(function () { });
                const vidId = vid.dataset.videoId;
                if (vidId && !viewedVideos.has(vidId)) {
                    viewedVideos.add(vidId);
                    incrementVideoViews(vidId);
                }
            } else {
                vid.pause();
            }
        });
    }, { threshold: 0.6 });
    return videoObserver;
}

function pauseAllVideos() {
    document.querySelectorAll('#video-feed video').forEach(function (v) {
        v.pause();
        if (videoObserver) videoObserver.unobserve(v);
    });
}

function handleVideoSearchInput(event) {
    videoSearchTerm = (event.target.value || '').toLowerCase();
    renderVideosTopBar();
    refreshVideoFeedWithFilters();
}

function handleVideoSortChange(event) {
    videoSortMode = event.target.value;
    refreshVideoFeedWithFilters();
}

function setVideoFilter(filter) {
    videoFilter = filter;
    refreshVideoFeedWithFilters();
}

function renderVideosTopBar() {
    const container = document.getElementById('videos-topbar');
    if (!container) return;

    const uploadBtn = document.createElement('button');
    uploadBtn.className = 'create-btn-sidebar';
    uploadBtn.style.width = 'auto';
    uploadBtn.innerHTML = '<i class="ph ph-upload-simple"></i> Create Video';
    uploadBtn.onclick = function () { window.openVideoUploadModal(); };

    const topBar = buildTopBar({
        title: 'Videos',
        searchPlaceholder: 'Search videos...',
        searchValue: videoSearchTerm,
        onSearch: handleVideoSearchInput,
        filters: [
            { label: 'All Videos', className: 'discover-pill video-filter-pill', active: videoFilter === 'All', onClick: function () { setVideoFilter('All'); } },
            { label: 'Trending', className: 'discover-pill video-filter-pill', active: videoFilter === 'Trending', onClick: function () { setVideoFilter('Trending'); } },
            { label: 'Shorts', className: 'discover-pill video-filter-pill', active: videoFilter === 'Shorts', onClick: function () { setVideoFilter('Shorts'); } },
            { label: 'Saved', className: 'discover-pill video-filter-pill', active: videoFilter === 'Saved', onClick: function () { setVideoFilter('Saved'); } }
        ],
        dropdowns: [
            {
                id: 'video-sort-select',
                className: 'discover-dropdown',
                forId: 'video-sort-select',
                label: 'Sort:',
                options: [
                    { value: 'recent', label: 'Recent' },
                    { value: 'popular', label: 'Most Viewed' }
                ],
                selected: videoSortMode,
                onChange: handleVideoSortChange,
                show: true
            }
        ],
        actions: [{ element: uploadBtn }]
    });

    container.innerHTML = '';
    container.appendChild(topBar);
}

function refreshVideoFeedWithFilters() {
    renderVideosTopBar();
    let filtered = videosCache.slice();

    if (videoSearchTerm) {
        filtered = filtered.filter(function (video) {
            const caption = (video.caption || '').toLowerCase();
            const tags = (video.hashtags || []).map(function (t) { return (`#${t}`).toLowerCase(); });
            return caption.includes(videoSearchTerm) || tags.some(function (tag) { return tag.includes(videoSearchTerm); });
        });
    }

    if (videoFilter === 'Trending') {
        filtered = filtered.slice().sort(function (a, b) { return (b.stats?.views || 0) - (a.stats?.views || 0); });
    } else if (videoFilter === 'Shorts') {
        filtered = filtered.filter(function (video) { return (video.duration || 0) <= 120 || (video.lengthSeconds || 0) <= 120 || !(video.duration || video.lengthSeconds); });
    }

    if (videoSortMode === 'popular') {
        filtered = filtered.slice().sort(function (a, b) { return (b.stats?.views || 0) - (a.stats?.views || 0); });
    }

    renderVideoFeed(filtered);
}

function initVideoFeed() {
    if (videosUnsubscribe) return; // already live
    renderVideosTopBar();
    const refVideos = query(collection(db, 'videos'), orderBy('createdAt', 'desc'));
    videosUnsubscribe = ListenerRegistry.register('videos:feed', onSnapshot(refVideos, function (snap) {
        videosCache = snap.docs.map(function (d) { return ({ id: d.id, ...d.data() }); });
        refreshVideoFeedWithFilters();
    }));
}

function renderVideoFeed(videos = []) {
    const feed = document.getElementById('video-feed');
    if (!feed) return;
    pauseAllVideos();
    feed.innerHTML = '';
    if (videos.length === 0) { feed.innerHTML = '<div class="empty-state">No videos yet.</div>'; return; }

    const observer = ensureVideoObserver();

    videos.forEach(function (video) {
        const card = document.createElement('div');
        card.className = 'video-card';
        const tags = (video.hashtags || []).map(function (t) { return '#' + t; }).join(' ');

        const videoEl = document.createElement('video');
        videoEl.setAttribute('playsinline', '');
        videoEl.setAttribute('loop', '');
        videoEl.setAttribute('muted', '');
        videoEl.setAttribute('preload', 'metadata');
        videoEl.dataset.videoId = video.id;
        videoEl.src = video.videoURL || '';

        const meta = document.createElement('div');
        meta.className = 'video-meta';

        const topRow = document.createElement('div');
        topRow.style.display = 'flex';
        topRow.style.justifyContent = 'space-between';
        topRow.style.alignItems = 'center';

        const leftCol = document.createElement('div');
        const captionEl = document.createElement('div');
        captionEl.style.fontWeight = '800';
        captionEl.textContent = escapeHtml(video.caption || '');
        const tagsEl = document.createElement('div');
        tagsEl.style.color = 'var(--text-muted)';
        tagsEl.style.fontSize = '0.85rem';
        tagsEl.textContent = tags;
        leftCol.appendChild(captionEl);
        leftCol.appendChild(tagsEl);

        const actions = document.createElement('div');
        actions.style.display = 'flex';
        actions.style.gap = '8px';

        const likeBtn = document.createElement('button');
        likeBtn.className = 'icon-pill';
        likeBtn.innerHTML = `<i class="ph ph-heart"></i>${video.stats?.likes || 0}`;
        likeBtn.onclick = function () { return window.likeVideo(video.id); };

        const saveBtn = document.createElement('button');
        saveBtn.className = 'icon-pill';
        saveBtn.innerHTML = '<i class="ph ph-bookmark"></i>';
        saveBtn.onclick = function () { return window.saveVideo(video.id); };

        actions.appendChild(likeBtn);
        actions.appendChild(saveBtn);

        topRow.appendChild(leftCol);
        topRow.appendChild(actions);

        meta.appendChild(topRow);

        card.appendChild(videoEl);
        card.appendChild(meta);
        feed.appendChild(card);
        observer.observe(videoEl);
    });
}

window.uploadVideo = async function () {
    if (!requireAuth()) return;

    const fileInput = document.getElementById('video-file');
    if (!fileInput || !fileInput.files || !fileInput.files[0]) return;

    const caption = document.getElementById('video-caption').value || '';
    const hashtags = (document.getElementById('video-tags').value || '')
        .split(',')
        .map(function (tag) { return tag.replace('#', '').trim(); })
        .filter(Boolean);
    const visibility = document.getElementById('video-visibility').value || 'public';
    const file = fileInput.files[0];
    const videoId = `${Date.now()}`;
    const storageRef = ref(storage, `videos/${currentUser.uid}/${videoId}/source.mp4`);

    try {
        const uploadTask = uploadBytesResumable(storageRef, file);
        await new Promise(function (resolve, reject) {
            uploadTask.on('state_changed', function () { }, reject, resolve);
        });

        const videoURL = await getDownloadURL(uploadTask.snapshot.ref);
        const docData = {
            ownerId: currentUser.uid,
            caption,
            hashtags,
            createdAt: serverTimestamp(),
            videoURL,
            thumbURL: '',
            visibility,
            stats: { likes: 0, comments: 0, saves: 0, views: 0 }
        };

        await setDoc(doc(db, 'videos', videoId), docData);
        videosCache = [{ id: videoId, ...docData }, ...videosCache];
        refreshVideoFeedWithFilters();
        toggleVideoUploadModal(false);
    } catch (err) {
        console.error('Video upload failed', err);
        toast('Video upload failed. Please try again.', 'error');
    }
};

document.addEventListener('keydown', function (event) {
    if (event.key === 'Escape' && destinationPickerOpen) {
        closeDestinationPicker();
    }
});

window.likeVideo = async function (videoId) {
    if (!requireAuth()) return;
    const likeRef = doc(db, 'videos', videoId, 'likes', currentUser.uid);
    await setDoc(likeRef, { createdAt: serverTimestamp() });
    await updateDoc(doc(db, 'videos', videoId), { 'stats.likes': increment(1) });
};

window.saveVideo = async function (videoId) {
    if (!requireAuth()) return;
    await setDoc(doc(db, 'videos', videoId, 'saves', currentUser.uid), { createdAt: serverTimestamp() });
    await updateDoc(doc(db, 'videos', videoId), { 'stats.saves': increment(1) });
};

async function incrementVideoViews(videoId) {
    try {
        await updateDoc(doc(db, 'videos', videoId), { 'stats.views': increment(1) });
    } catch (e) { console.warn('view inc', e.message); }
}

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
    return 'https://images.unsplash.com/photo-1525186402429-b4ff38bedbec?auto=format&fit=crop&w=800&q=80';
}

function handleLiveSearchInput(event) {
    liveSearchTerm = (event.target.value || '').toLowerCase();
    renderLiveDirectoryFromCache();
}

function handleLiveTagFilterInput(event) {
    liveTagFilter = (event.target.value || '').toLowerCase();
    renderLiveDirectoryFromCache();
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
                label: 'Category:',
                options: [
                    { value: 'All', label: 'All Categories' },
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

function renderLiveDirectoryFromCache() {
    renderLiveTopBar();
    renderLiveFilterRow();
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

document.addEventListener('DOMContentLoaded', function () {
    bindMobileNav();
    bindMobileScrollHelper();
    syncMobileComposerState();
    bindAuthFormShortcuts();
    const title = document.getElementById('postTitle');
    const content = document.getElementById('postContent');
    if (title) title.addEventListener('input', syncPostButtonState);
    if (content) content.addEventListener('input', syncPostButtonState);
    initializeNexeraApp();
    const initialHash = (window.location.hash || '').replace('#', '');
    if (initialHash === 'live-setup') { window.navigateTo('live-setup', false); }
});

window.addEventListener('hashchange', function () {
    const hash = (window.location.hash || '').replace('#', '');
    if (hash === 'live-setup') { window.navigateTo('live-setup', false); }
});

// --- Security Rules Snippet (reference) ---
// See firestore.rules for suggested rules ensuring users write their own content and staff-only access.
