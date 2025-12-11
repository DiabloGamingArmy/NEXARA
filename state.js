import { initializeApp } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-app.js";
import { getAuth, signInWithEmailAndPassword, createUserWithEmailAndPassword, signInAnonymously, onAuthStateChanged, signOut, updateProfile } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-auth.js";
import { getFirestore, collection, addDoc, onSnapshot, query, orderBy, serverTimestamp, doc, setDoc, getDoc, updateDoc, deleteDoc, arrayUnion, arrayRemove, increment, where, getDocs, collectionGroup, limit, startAt, endAt, Timestamp } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";
import { getStorage, ref, uploadBytes, uploadBytesResumable, getDownloadURL, deleteObject } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-storage.js";
import { normalizeReplyTarget, buildReplyRecord, groupCommentsByParent } from "./commentUtils.js";


// --- Global State & Cache ---
let currentUser = null;
let allPosts = [];
let userCache = {};
window.myReviewCache = {}; // Global cache for reviews
let currentCategory = 'For You';
let currentProfileFilter = 'All';
let discoverFilter = 'All Results';
let discoverSearchTerm = '';
let discoverPostsSort = 'recent';
let discoverCategoriesMode = 'verified_first';
let savedSearchTerm = '';
let savedFilter = 'All Saved';
let isInitialLoad = true;
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
let tagSuggestionPool = [];
let mentionSearchTimer = null;
let currentThreadComments = [];
let scheduledRenderTimer = null;

// Optimistic UI Sets
let followedCategories = new Set(['STEM', 'Coding']);
let followedUsers = new Set();

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

function applyReviewButtonState(buttonEl, reviewValue) {
    if (!buttonEl) return;
    const { label, className } = getReviewDisplay(reviewValue);
    const iconSize = buttonEl.dataset.iconSize || '1.1rem';
    buttonEl.classList.remove(...REVIEW_CLASSES);
    if (className) buttonEl.classList.add(className);
    buttonEl.innerHTML = `<i class="ph ph-scales" style="font-size:${iconSize};"></i> ${label}`;
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
    followersCount: 0
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

function resolveAvatarInitial(userLike = {}) {
    const source = userLike.nickname || userLike.displayName || userLike.name || userLike.username || 'U';
    return (source || 'U').trim().charAt(0).toUpperCase() || 'U';
}

function ensureAvatarColor(profile = {}, uid = '') {
    if (profile.avatarColor) return profile.avatarColor;
    const color = computeAvatarColor(uid || profile.username || profile.displayName || profile.name || 'user');
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

function normalizeUserProfileData(data = {}) {
    const accountRoles = Array.isArray(data.accountRoles) ? data.accountRoles : [];
    const profile = { ...data, accountRoles };
    profile.photoPath = data.photoPath || '';
    profile.avatarColor = data.avatarColor || computeAvatarColor(data.username || data.displayName || data.name || 'user');
    profile.locationHistory = Array.isArray(data.locationHistory) ? data.locationHistory : [];
    return profile;
}

function userHasRole(userLike = {}, role = '') {
    const roles = new Set(Array.isArray(userLike.accountRoles) ? userLike.accountRoles : []);
    if (userLike.role) roles.add(userLike.role);
    if (userLike.verified === true) roles.add('verified');
    return roles.has(role);
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
let videosUnsubscribe = null;
let videosCache = [];
let videoObserver = null;
const viewedVideos = new Set();
let liveSessionsUnsubscribe = null;
let postsUnsubscribe = null;
let activeLiveSessionId = null;

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

// --- Navigation Stack ---
let navStack = [];
let currentViewId = 'feed';

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

// Export all variables you need globally
export { currentUser, allPosts, userCache, ListenerRegistry, /* add other vars here */ };
