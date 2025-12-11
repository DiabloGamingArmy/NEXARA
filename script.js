import { initializeApp } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-app.js";
import { getAuth, signInWithEmailAndPassword, createUserWithEmailAndPassword, signInAnonymously, onAuthStateChanged, signOut, updateProfile } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-auth.js";
import { getFirestore, collection, addDoc, onSnapshot, query, orderBy, serverTimestamp, doc, setDoc, getDoc, updateDoc, deleteDoc, arrayUnion, arrayRemove, increment, where, getDocs, collectionGroup } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";
import { getStorage, ref, uploadBytes, uploadBytesResumable, getDownloadURL } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-storage.js";
import { normalizeReplyTarget, buildReplyRecord, groupCommentsByParent } from "./commentUtils.js";

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
    profile.avatarColor = data.avatarColor || computeAvatarColor(data.username || data.displayName || data.name || 'user');
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
function initApp() {
    onAuthStateChanged(auth, async function (user) {
        const loadingOverlay = document.getElementById('loading-overlay');
        const authScreen = document.getElementById('auth-screen');
        const appLayout = document.getElementById('app-layout');

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
                    userProfile = { ...userProfile, ...normalizeUserProfileData(docSnap.data()) };
                    userCache[user.uid] = userProfile;

                    // Normalize role storage
                    userProfile.accountRoles = Array.isArray(userProfile.accountRoles) ? userProfile.accountRoles : [];

                    await backfillAvatarColorIfMissing(user.uid, userProfile);

                    // Apply stored theme preference
                    const savedTheme = userProfile.theme || nexeraGetStoredThemePreference() || 'system';
                    userProfile.theme = savedTheme;
                    applyTheme(savedTheme);

                    // Restore 'following' state locally
                    if (userProfile.following) {
                        userProfile.following.forEach(function (uid) { followedUsers.add(uid); });
                    }
                    const staffNav = document.getElementById('nav-staff');
                    if (staffNav) staffNav.style.display = (hasGlobalRole('staff') || hasGlobalRole('admin') || hasFounderClaimClient()) ? 'flex' : 'none';
                } else {
                    // Create new profile placeholder if it doesn't exist
                    userProfile.email = user.email || "";
                    userProfile.name = user.displayName || "Nexera User";
                    const storedTheme = nexeraGetStoredThemePreference() || userProfile.theme || 'system';
                    userProfile.theme = storedTheme;
                    userProfile.avatarColor = userProfile.avatarColor || computeAvatarColor(user.uid || user.email || 'user');
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
            startDataListener();
            startUserReviewListener(user.uid); // PATCH: Listen for USER reviews globally on load
            updateTimeCapsule();
            window.navigateTo('feed', false);
            renderProfile(); // Pre-render profile
        } else {
            currentUser = null;
            updateAuthClaims({});
            if (loadingOverlay) loadingOverlay.style.display = 'none';
            if (appLayout) appLayout.style.display = 'none';
            if (authScreen) authScreen.style.display = 'flex';
        }
    });
}

function updateTimeCapsule() {
    const date = new Date();
    const key = `${date.getMonth() + 1}-${date.getDate()}`;
    const eventText = HISTORICAL_EVENTS[key] || HISTORICAL_EVENTS["DEFAULT"];
    const dateString = date.toLocaleDateString('en-US', { month: 'long', day: 'numeric' });

    const dateEl = document.getElementById('otd-date-display');
    const eventEl = document.getElementById('otd-event-display');

    if (dateEl) dateEl.textContent = dateString;
    if (eventEl) eventEl.textContent = eventText;
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
            avatarColor,
            bio: "",
            website: "",
            region: "",
            email: user.email || "",
            accountRoles: [],
            tagAffinity: {},
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

// --- Auth Functions ---
window.handleLogin = async function (e) {
    if (e && typeof e.preventDefault === "function") e.preventDefault();

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

        var cred = await signInWithEmailAndPassword(auth, email, pass);

        if (typeof ensureUserDocument === 'function') {
            await ensureUserDocument(cred.user);
        }
    } catch (err) {
        var errEl2 = document.getElementById('auth-error');
        if (errEl2) errEl2.textContent = err.message;
        console.error(err);
    }
};

window.handleSignup = async function (e) {
    e.preventDefault();
    try {
        const cred = await createUserWithEmailAndPassword(
            auth,
            document.getElementById('email').value,
            document.getElementById('password').value
        );
        // Create initial user document
        await setDoc(doc(db, "users", cred.user.uid), {
            displayName: "New Explorer",
            username: cred.user.email.split('@')[0],
            email: cred.user.email,
            createdAt: serverTimestamp(),
            updatedAt: serverTimestamp(),
            savedPosts: [],
            followersCount: 0,
            following: [],
            photoURL: "",
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
    }
};

window.handleAnon = async function () {
    try {
        await signInAnonymously(auth);
    } catch (e) {
        console.error(e);
    }
};

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
                userCache[docSnap.id] = normalizeUserProfileData(docSnap.data());
            } else {
                userCache[docSnap.id] = { name: "Unknown User", username: "unknown" };
            }
        });

        // Re-render dependent views once data arrives
        renderFeed();
        if (activePostId) renderThreadMainPost(activePostId);
    } catch (e) {
        console.error("Error fetching profiles:", e);
    }
}

function startDataListener() {
    if (postsUnsubscribe) postsUnsubscribe();
    const postsRef = collection(db, 'posts');
    const q = query(postsRef);

    postsUnsubscribe = ListenerRegistry.register('feed:all', onSnapshot(q, function (snapshot) {
        const previousCache = { ...postSnapshotCache };
        const nextCache = {};
        allPosts = [];
        snapshot.forEach(function (doc) {
            const data = doc.data();
            const normalized = normalizePostData(doc.id, data);
            allPosts.push(normalized);
            nextCache[doc.id] = data;
        });

        // Sort posts by date (newest first)
        allPosts.sort(function (a, b) { return (b.timestamp?.seconds || 0) - (a.timestamp?.seconds || 0); });

        // Fetch profiles for these posts
        fetchMissingProfiles(allPosts);

        // Initial Render
        if (isInitialLoad) {
            renderFeed();
            isInitialLoad = false;
        }

        // Live updates for specific interactions
        snapshot.docChanges().forEach(function (change) {
            if (change.type === "modified") {
                refreshSinglePostUI(change.doc.id);

                if (activePostId === change.doc.id && document.getElementById('view-thread').style.display === 'block') {
                    const prevData = previousCache[change.doc.id] || {};
                    const newData = change.doc.data();
                    if (shouldRerenderThread(newData, prevData)) {
                        renderThreadMainPost(activePostId);
                    }
                }
            }
        });

        postSnapshotCache = nextCache;
    }));

    // Start Live Stream Listener (Mock)
    if (typeof renderLive === 'function') renderLive();
}

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
        tags: Array.isArray(data.tags) ? data.tags : [],
        mentions: Array.isArray(data.mentions) ? data.mentions : [],
        content,
        categoryStatus: memberships[categoryId]?.status || 'unknown'
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
    if (!btn) return;
    btn.disabled = false;
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
            badge.textContent = '✔';
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

    // Toggle Navbar Active State
    if (viewId !== 'thread' && viewId !== 'public-profile') {
        document.querySelectorAll('.nav-item').forEach(function (el) { el.classList.remove('active'); });
        const navEl = document.getElementById('nav-' + viewId);
        if (navEl) navEl.classList.add('active');
    }

    // View Specific Logic
    if (viewId === 'feed' && pushToStack) {
        currentCategory = 'For You';
        renderFeed();
    }
    if (viewId === 'saved') { renderSaved(); }
    if (viewId === 'profile') renderProfile();
    if (viewId === 'discover') { renderDiscover(); }
    if (viewId === 'messages') { initConversations(); }
    if (viewId === 'videos') { initVideoFeed(); }
    if (viewId === 'live') { renderLiveSessions(); }
    if (viewId === 'staff') { renderStaffConsole(); }

    currentViewId = viewId;
    window.scrollTo(0, 0);
};

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
window.toggleFollow = function (c, event) {
    if (event) event.stopPropagation();

    const isFollowing = followedCategories.has(c);
    if (isFollowing) followedCategories.delete(c);
    else followedCategories.add(c);

    // Sanitize class name to match HTML
    const cleanTopic = c.replace(/[^a-zA-Z0-9]/g, '');
    const btns = document.querySelectorAll(`.js-follow-topic-${cleanTopic}`);

    btns.forEach(function (btn) {
        if (isFollowing) {
            btn.innerHTML = '<i class="ph-bold ph-plus"></i> Topic';
            btn.classList.remove('following');
        } else {
            btn.innerHTML = 'Following';
            btn.classList.add('following');
        }
    });
};

window.toggleFollowUser = async function (uid, event) {
    if (event) event.stopPropagation();

    const isFollowing = followedUsers.has(uid);
    if (isFollowing) followedUsers.delete(uid);
    else followedUsers.add(uid);

    // 1. Update Buttons immediately
    const btns = document.querySelectorAll(`.js-follow-user-${uid}`);
    btns.forEach(function (btn) {
        if (isFollowing) {
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
        } else {
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
        }
    });

    // 2. Real-time Follower Count Update
    const countEl = document.getElementById(`profile-follower-count-${uid}`);
    let newCount = 0;

    if (userCache[uid]) {
        let currentCount = userCache[uid].followersCount || 0;
        newCount = isFollowing ? Math.max(0, currentCount - 1) : currentCount + 1;
        userCache[uid].followersCount = newCount;
    }

    if (countEl) {
        countEl.textContent = newCount;
    }

    // 3. Backend Update
    try {
        if (isFollowing) {
            await updateDoc(doc(db, 'users', uid), { followersCount: increment(-1) });
            await updateDoc(doc(db, 'users', currentUser.uid), { following: arrayRemove(uid) });
        } else {
            await updateDoc(doc(db, 'users', uid), { followersCount: increment(1) });
            await updateDoc(doc(db, 'users', currentUser.uid), { following: arrayUnion(uid) });
        }
    } catch (e) { console.error(e); }
};

// --- Render Logic (The Core) ---
function getPostHTML(post) {
    try {
        const date = post.timestamp && post.timestamp.seconds
            ? new Date(post.timestamp.seconds * 1000).toLocaleDateString()
            : 'Just now';

        let authorData = userCache[post.userId] || { name: post.author, username: "loading...", photoURL: null };
        if (!authorData.name) authorData.name = "Unknown User";

        const authorVerified = userHasRole(authorData, 'verified');
        const verifiedBadge = authorVerified ? '<span class="verified-badge" aria-label="Verified account">✔</span>' : '';

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
                                <button class="follow-btn js-follow-topic-${topicClass} ${isFollowingTopic ? 'following' : ''}"onclick="event.stopPropagation(); window.toggleFollow('${post.category}', event)" style="font-size:0.65rem; padding:2px 8px;">${isFollowingTopic ? 'Following' : '<i class="ph-bold ph-plus"></i> Topic'}</button>`;

        let trustBadge = "";
        if (post.trustScore > 2) {
            trustBadge = `<div style="font-size:0.75rem; color:#8b949e; display:flex; align-items:center; gap:7px; font-weight:600;"><i class="ph-fill ph-check-circle"></i> Publicly Verified</div>`;
        } else if (post.trustScore < -1) {
            trustBadge = `<div style="font-size:0.75rem; color:#ff3d3d; display:flex; align-items:center; gap:4px; font-weight:600;"><i class="ph-fill ph-warning-circle"></i> Disputed</div>`;
        }

        const postText = typeof post.content === 'object' && post.content !== null ? (post.content.text || '') : (post.content || '');
        const formattedBody = formatContent(postText, post.tags, post.mentions);
        const tagListHtml = renderTagList(post.tags || []);

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

        return `
            <div id="post-card-${post.id}" class="social-card fade-in" style="border-left: 2px solid ${THEMES['For You']};">
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
                    <h3 class="post-title">${escapeHtml(cleanText(post.title))}</h3>
                    <p>${formattedBody}</p>
                    ${tagListHtml}
                    ${mediaContent}
                    ${commentPreviewHtml}
                    ${savedTagHtml}
                </div>
                <div class="card-actions">
                    <button id="post-like-btn-${post.id}" class="action-btn" onclick="window.toggleLike('${post.id}', event)" style="color: ${isLiked ? '#00f2ea' : 'inherit'}"><i class="${isLiked ? 'ph-fill' : 'ph'} ph-thumbs-up" style="font-size:1.1rem;"></i> ${post.likes || 0}</button>
                    <button id="post-dislike-btn-${post.id}" class="action-btn" onclick="window.toggleDislike('${post.id}', event)" style="color: ${isDisliked ? '#ff3d3d' : 'inherit'}"><i class="${isDisliked ? 'ph-fill' : 'ph'} ph-thumbs-down" style="font-size:1.1rem;"></i> ${post.dislikes || 0}</button>
                    <button class="action-btn" onclick="window.openThread('${post.id}')"><i class="ph ph-chat-circle" style="font-size:1.1rem;"></i> Discuss</button>
                    <button id="post-save-btn-${post.id}" class="action-btn" onclick="window.toggleSave('${post.id}', event)" style="color: ${isSaved ? '#00f2ea' : 'inherit'}"><i class="${isSaved ? 'ph-fill' : 'ph'} ph-bookmark-simple" style="font-size:1.1rem;"></i> ${isSaved ? 'Saved' : 'Save'}</button>
                    <button class="action-btn review-action ${reviewDisplay.className}" data-post-id="${post.id}" data-icon-size="1.1rem" onclick="event.stopPropagation(); window.openPeerReview('${post.id}')"><i class="ph ph-scales" style="font-size:1.1rem;"></i> ${reviewDisplay.label}</button>
                </div>
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
    let displayPosts = allPosts.filter(function (post) {
        if (post.visibility === 'private') return currentUser && post.userId === currentUser.uid;
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
        displayPosts = displayPosts.slice().sort(function(a, b) {
            const scoreDiff = getPostAffinityScore(b) - getPostAffinityScore(a);
            if (scoreDiff !== 0) return scoreDiff;
            return (b.timestamp?.seconds || 0) - (a.timestamp?.seconds || 0);
        });
    }

    if (displayPosts.length === 0) {
        container.innerHTML = `<div class="empty-state"><i class="ph ph-magnifying-glass" style="font-size:3rem; margin-bottom:1rem;"></i><p>No posts found.</p></div>`;
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
    const post = allPosts.find(function(p) { return p.id === postId; });
    if (!post) return;

    const likeBtn = document.getElementById(`post-like-btn-${postId}`);
    const dislikeBtn = document.getElementById(`post-dislike-btn-${postId}`);
    const saveBtn = document.getElementById(`post-save-btn-${postId}`);
    const reviewBtn = document.querySelector(`#post-card-${postId} .review-action`);

    const isLiked = post.likedBy && post.likedBy.includes(currentUser.uid);
    const isDisliked = post.dislikedBy && post.dislikedBy.includes(currentUser.uid);
    const isSaved = userProfile.savedPosts && userProfile.savedPosts.includes(postId);
    const myReview = window.myReviewCache ? window.myReviewCache[postId] : null;

    if(likeBtn) {
        likeBtn.innerHTML = `<i class="${isLiked ? 'ph-fill' : 'ph'} ph-thumbs-up" style="font-size:1.1rem;"></i> ${post.likes || 0}`;
        likeBtn.style.color = isLiked ? '#00f2ea' : 'inherit';
    }
    if(dislikeBtn) {
        dislikeBtn.innerHTML = `<i class="${isDisliked ? 'ph-fill' : 'ph'} ph-thumbs-down" style="font-size:1.1rem;"></i> ${post.dislikes || 0}`;
        dislikeBtn.style.color = isDisliked ? '#ff3d3d' : 'inherit';
    }
    if(saveBtn) { 
        // FIX: Ensure text toggles between Save and Saved
        saveBtn.innerHTML = `<i class="${isSaved ? 'ph-fill' : 'ph'} ph-bookmark-simple" style="font-size:1.1rem;"></i> ${isSaved ? 'Saved' : 'Save'}`; 
        saveBtn.style.color = isSaved ? '#00f2ea' : 'inherit'; 
    }
    if(reviewBtn) {
        applyReviewButtonState(reviewBtn, myReview);
    }

    // Update Thread View if active
    const threadLikeBtn = document.getElementById('thread-like-btn');
    const threadDislikeBtn = document.getElementById('thread-dislike-btn');
    const threadSaveBtn = document.getElementById('thread-save-btn');
    const threadTitle = document.getElementById('thread-view-title');
    const threadReviewBtn = document.getElementById('thread-review-btn');

    if(threadTitle && threadTitle.dataset.postId === postId) {
        if(threadLikeBtn) {
            threadLikeBtn.innerHTML = `<i class="${isLiked ? 'ph-fill' : 'ph'} ph-thumbs-up"></i> <span style="font-size:1rem; margin-left:5px;">${post.likes || 0}</span>`;
            threadLikeBtn.style.color = isLiked ? '#00f2ea' : 'inherit';
        }
        if(threadDislikeBtn) {
            threadDislikeBtn.innerHTML = `<i class="${isDisliked ? 'ph-fill' : 'ph'} ph-thumbs-down"></i> <span style="font-size:1rem; margin-left:5px;">${post.dislikes || 0}</span>`;
            threadDislikeBtn.style.color = isDisliked ? '#ff3d3d' : 'inherit';
        }
        if(threadSaveBtn) {
            threadSaveBtn.innerHTML = `<i class="${isSaved ? 'ph-fill' : 'ph'} ph-bookmark-simple"></i> <span style="font-size:1rem; margin-left:5px;">${isSaved ? 'Saved' : 'Save'}</span>`;
            threadSaveBtn.style.color = isSaved ? '#00f2ea' : 'inherit';
        }
        if(threadReviewBtn) {
            applyReviewButtonState(threadReviewBtn, myReview);
        }
    }
}

// --- Interaction Functions ---
window.toggleLike = async function(postId, event) {
    if(event) event.stopPropagation();
    if(!currentUser) return alert("Please log in to like posts.");

    const post = allPosts.find(function(p) { return p.id === postId; });
    if(!post) return;

    const wasLiked = post.likedBy && post.likedBy.includes(currentUser.uid);
    const hadDisliked = post.dislikedBy && post.dislikedBy.includes(currentUser.uid);

    // Optimistic Update
    if (wasLiked) {
        post.likes = Math.max(0, (post.likes || 0) - 1); // Prevent negative likes
        post.likedBy = post.likedBy.filter(function(uid) { return uid !== currentUser.uid; });
    } else {
        post.likes = (post.likes || 0) + 1;
        if (!post.likedBy) post.likedBy = [];
        post.likedBy.push(currentUser.uid);
        if (hadDisliked) {
            post.dislikes = Math.max(0, (post.dislikes || 0) - 1);
            post.dislikedBy = (post.dislikedBy || []).filter(function(uid) { return uid !== currentUser.uid; });
        }
    }

    recordTagAffinity(post.tags, wasLiked ? -1 : 1);

    refreshSinglePostUI(postId);
    const postRef = doc(db, 'posts', postId);

    try {
        if(wasLiked) {
            await updateDoc(postRef, { likes: increment(-1), likedBy: arrayRemove(currentUser.uid) });
        } else {
            const updatePayload = { likes: increment(1), likedBy: arrayUnion(currentUser.uid) };
            if (hadDisliked) {
                updatePayload.dislikes = increment(-1);
                updatePayload.dislikedBy = arrayRemove(currentUser.uid);
            }
            await updateDoc(postRef, updatePayload);
        }
    } catch(e) {
        console.error("Like error:", e); 
        // Revert on error would go here, or just let snapshot fix it
        startDataListener(); 
    }
}

window.toggleSave = async function(postId, event) {
    if(event) event.stopPropagation();
    if(!currentUser) return alert("Please log in to save posts.");

    const isSaved = userProfile.savedPosts && userProfile.savedPosts.includes(postId);

    // Optimistic Update
    if(isSaved) {
        userProfile.savedPosts = userProfile.savedPosts.filter(function(id) { return id !== postId; });
    } else {
        userProfile.savedPosts.push(postId);
    }
    userCache[currentUser.uid] = userProfile;

    refreshSinglePostUI(postId);

    const userRef = doc(db, 'users', currentUser.uid);
    try {
        if(isSaved) await updateDoc(userRef, { savedPosts: arrayRemove(postId) });
        else await updateDoc(userRef, { savedPosts: arrayUnion(postId) });
    } catch(e) { console.error("Save error:", e); }
}

// --- Creation & Upload ---
async function uploadFileToStorage(file, path) {
    if (!file) return null;
    const storageRef = ref(storage, path);
    await uploadBytes(storageRef, file);
    return await getDownloadURL(storageRef);
}

function parseTagsInput(raw = '') {
    return raw.split(',').map(function(t) { return t.trim().replace(/^#/, '').toLowerCase(); }).filter(Boolean);
}

function parseMentionsInput(raw = '') {
    return raw.split(',').map(function(t) { return t.trim().replace(/^@/, '').toLowerCase(); }).filter(Boolean);
}

function escapeRegex(str = '') {
    return (str || '').replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&');
}

function renderTagList(tags = []) {
    if (!tags.length) return '';
    return `<div style="margin-top:8px;">${tags.map(function(tag) { return `<span class="tag-chip">#${escapeHtml(tag)}</span>`; }).join('')}</div>`;
}

function formatContent(text = '', tags = [], mentions = []) {
    let safe = escapeHtml(cleanText(text));
    const mentionSet = new Set((mentions || []).map(function(m) { return m.toLowerCase(); }));
    mentionSet.forEach(function(handle) {
        const regex = new RegExp('@' + escapeRegex(handle), 'gi');
        safe = safe.replace(regex, `<a class="mention-link" onclick=\"window.openUserProfileByHandle('${handle}')\">@${escapeHtml(handle)}</a>`);
    });
    (tags || []).forEach(function(tag) {
        const regex = new RegExp('#' + escapeRegex(tag), 'gi');
        safe = safe.replace(regex, `<span class="tag-chip">#${escapeHtml(tag)}</span>`);
    });
    return safe;
}

async function resolveMentionProfiles(handles = []) {
    const cleaned = Array.from(new Set(handles.map(function(h) { return h.replace(/^@/, '').toLowerCase(); }).filter(Boolean)));
    const results = [];
    for (const handle of cleaned) {
        const cached = Object.entries(userCache).find(function([_, data]) { return (data.username || '').toLowerCase() === handle; });
        if (cached) { results.push({ uid: cached[0], handle }); continue; }
        const qSnap = await getDocs(query(collection(db, 'users'), where('username', '==', handle)));
        if (!qSnap.empty) {
            const docSnap = qSnap.docs[0];
            userCache[docSnap.id] = normalizeUserProfileData(docSnap.data());
            results.push({ uid: docSnap.id, handle });
        }
    }
    return results;
}

async function notifyMentionedUsers(resolved = [], postId) {
    const tasks = resolved.map(function(entry) {
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
    tags.forEach(function(tag) {
        affinity[tag] = (affinity[tag] || 0) + delta;
    });
    userProfile.tagAffinity = affinity;
    userCache[currentUser.uid] = userProfile;
    setDoc(doc(db, 'users', currentUser.uid), { tagAffinity: affinity }, { merge: true });
}

function getPostAffinityScore(post) {
    const affinity = userProfile.tagAffinity || {};
    const tags = Array.isArray(post.tags) ? post.tags : [];
    return tags.reduce(function(total, tag) { return total + (affinity[tag] || 0); }, 0);
}

window.createPost = async function() {
     if (!requireAuth()) return;
     const title = document.getElementById('postTitle').value;
     const content = document.getElementById('postContent').value;
     const tagsInput = document.getElementById('postTags');
     const mentionsInput = document.getElementById('postMentions');
     const tags = parseTagsInput(tagsInput ? tagsInput.value : '');
     const mentions = parseMentionsInput(mentionsInput ? mentionsInput.value : '');
     const fileInput = document.getElementById('postFile');
     const btn = document.getElementById('publishBtn');
     setComposerError('');

     let contentType = 'text';
     if (fileInput.files[0]) {
         const mime = fileInput.files[0].type;
         if (mime.startsWith('video')) contentType = 'video';
         else if (mime.startsWith('image')) contentType = 'image';
     }

     if(!title.trim() && !content.trim() && !fileInput.files[0]) {
         return alert("Please add a title, content, or media.");
     }

     btn.disabled = true;
     btn.textContent = "Uploading...";

     try {
         if (selectedCategoryId && currentUser?.uid) {
             const joined = await ensureJoinedCategory(selectedCategoryId, currentUser.uid);
             if (!joined) {
                 setComposerError('Unable to join this category. Please try again.');
                 return;
             }
         }

         const mentionProfiles = await resolveMentionProfiles(mentions);

         let mediaUrl = null;
         if(fileInput.files[0]) {
             const path = `posts/${currentUser.uid}/${Date.now()}_${fileInput.files[0].name}`;
             mediaUrl = await uploadFileToStorage(fileInput.files[0], path);
         }

         const categoryDoc = selectedCategoryId ? getCategorySnapshot(selectedCategoryId) : null;
         const visibility = 'public';
         const postPayload = {
             title,
             content,
             categoryId: selectedCategoryId || null,
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
             mentionUserIds: mentionProfiles.map(function(m) { return m.uid; }),
             likes: 0,
             likedBy: [],
             trustScore: 0,
             timestamp: serverTimestamp()
         };

         const postRef = await addDoc(collection(db, 'posts'), postPayload);
         if (mentionProfiles.length) await notifyMentionedUsers(mentionProfiles, postRef.id);

         // Reset Form
         document.getElementById('postTitle').value = "";
         document.getElementById('postContent').value = "";
         if (tagsInput) tagsInput.value = "";
         if (mentionsInput) mentionsInput.value = "";
         fileInput.value = "";
         window.clearPostImage();
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
function updateSettingsAvatarPreview(src) {
    const preview = document.getElementById('settings-avatar-preview');
    if(!preview) return;

    const tempUser = { ...userProfile, photoURL: src || '', avatarColor: userProfile.avatarColor || computeAvatarColor(currentUser?.uid || 'user') };
    applyAvatarToElement(preview, tempUser, { size: 72 });
}

function syncThemeRadios(themeValue) {
    const selected = document.querySelector(`input[name="theme-choice"][value="${themeValue}"]`);
    if(selected) selected.checked = true;
}

window.toggleCreateModal = function(show) {
    document.getElementById('create-modal').style.display = show ? 'flex' : 'none';
    if(show && currentUser) {
        const avatarEl = document.getElementById('modal-user-avatar');
        applyAvatarToElement(avatarEl, userProfile, { size: 42 });
        setComposerError('');
        renderDestinationField();
        syncPostButtonState();
    } else if (!show) {
        closeDestinationPicker();
    }
}

window.toggleSettingsModal = function(show) {
    document.getElementById('settings-modal').style.display = show ? 'flex' : 'none';
    if(show){
        document.getElementById('set-name').value = userProfile.name||"";
        document.getElementById('set-real-name').value = userProfile.realName||"";
        document.getElementById('set-username').value = userProfile.username||"";
        document.getElementById('set-bio').value = userProfile.bio||"";
        document.getElementById('set-website').value = userProfile.links||"";
        document.getElementById('set-phone').value = userProfile.phone||"";
        const genderInput = document.getElementById('set-gender');
        if(genderInput) genderInput.value = userProfile.gender||"Prefer not to say";
        document.getElementById('set-email').value = userProfile.email||"";
        document.getElementById('set-nickname').value = userProfile.nickname||"";
        document.getElementById('set-region').value = userProfile.region||"";
        const photoUrlInput = document.getElementById('set-photo-url');
        if(photoUrlInput) {
            photoUrlInput.value = userProfile.photoURL || "";
            photoUrlInput.oninput = function(e) { return updateSettingsAvatarPreview(e.target.value); };
        }
        syncThemeRadios(userProfile.theme || 'system');
        updateSettingsAvatarPreview(userProfile.photoURL);

        const uploadInput = document.getElementById('set-pic-file');
        const cameraInput = document.getElementById('set-pic-camera');
        if(uploadInput) uploadInput.onchange = function(e) { return handleSettingsFileChange(e.target); };
        if(cameraInput) cameraInput.onchange = function(e) { return handleSettingsFileChange(e.target); };

        document.querySelectorAll('input[name="theme-choice"]').forEach(function(r) {
            r.onchange = function(e) { return persistThemePreference(e.target.value); };
        });
    }
}

 window.saveSettings = async function() {
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

     if(!username) {
         return alert("Username is required.");
     }
     if(username && !/^[A-Za-z0-9._-]{3,20}$/.test(username)) {
         return alert("Username must be 3-20 characters with letters, numbers, dots, underscores, or hyphens.");
     }

     let photoURL = userProfile.photoURL;
     const newPhoto = (fileInput && fileInput.files[0]) || (cameraInput && cameraInput.files[0]);
     if(newPhoto) {
         const path = `users/${currentUser.uid}/pfp_${Date.now()}`;
         photoURL = await uploadFileToStorage(newPhoto, path);
     } else if(manualPhoto) {
         photoURL = manualPhoto;
     }

     const updates = { name, realName, nickname, username, bio, links, phone, gender, email, region, theme, photoURL };
     userProfile = { ...userProfile, ...updates };
     userCache[currentUser.uid] = userProfile;

     try {
         await setDoc(doc(db, "users", currentUser.uid), updates, { merge: true });
         if(name) await updateProfile(auth.currentUser, { displayName: name, photoURL: photoURL });
     } catch(e) {
         console.error("Save failed", e);
     }

     await persistThemePreference(theme);
     renderProfile();
     renderFeed();
     window.toggleSettingsModal(false);
 }

function handleSettingsFileChange(inputEl) {
    if(!inputEl || !inputEl.files || !inputEl.files[0]) return;
    const reader = new FileReader();
    reader.onload = function(e) { return updateSettingsAvatarPreview(e.target.result); };
    reader.readAsDataURL(inputEl.files[0]);
}

// --- Peer Review System ---
window.openPeerReview = function(postId) { 
    activePostId = postId; 
    document.getElementById('review-modal').style.display = 'flex'; 
    document.getElementById('review-stats-text').textContent = "Loading data...";

    const reviewsRef = collection(db, 'posts', postId, 'reviews'); 
    const q = query(reviewsRef); 

    ListenerRegistry.register(`reviews:post:${postId}`, onSnapshot(q, function(snapshot) {
        const container = document.getElementById('review-list');
        container.innerHTML = "";

        let scores = { verified: 0, citation: 0, misleading: 0, total: 0 };
        let userHasReview = false; 
        let myRatingData = null;

        snapshot.forEach(function(doc) {
            const data = doc.data();
            if(data.userId === currentUser.uid) { 
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
            if(data.rating === 'verified') scores.verified++; 
            if(data.rating === 'citation') scores.citation++; 
            if(data.rating === 'misleading') scores.misleading++;

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
        if(userHasReview) {
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
        if(scores.total > 0) {
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
window.submitReview = async function() {
    if(!activePostId) return alert("Error: No active post selected.");
    const ratingEl = document.getElementById('review-rating');
    const noteEl = document.getElementById('review-note');

    if(!ratingEl || !noteEl) return;

    const rating = ratingEl.value;
    const note = noteEl.value;

    if(!note.trim()) return alert("Please add a note explaining your review.");

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

    } catch(e) { 
        console.error("Review failed", e); 
        alert("Failed to submit review. Please try again.");
    }
}

// PATCH: Clear cache and reset UI color on remove
window.removeReview = async function() { 
    if(!window.currentReviewId || !activePostId) return; 

    // Clear from cache immediately
    if (window.myReviewCache) delete window.myReviewCache[activePostId];

    // Reset UI color
    refreshSinglePostUI(activePostId);
    applyMyReviewStylesToDOM();
    window.closeReview(); // Close modal

    try { 
        await deleteDoc(doc(db, 'posts', activePostId, 'reviews', window.currentReviewId)); 
    } catch(e) { 
        console.error(e); 
    } 
}

// --- Thread & Comments ---
window.openThread = function(postId) {
    activePostId = postId;
    activeReplyId = null;
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

    threadUnsubscribe = ListenerRegistry.register(`comments:${postId}`, onSnapshot(q, function(snapshot) {
        const comments = snapshot.docs.map(function(d) { return ({ id: d.id, ...d.data() }); });
        const missingCommentUsers = comments.filter(function(c) { return !userCache[c.userId]; }).map(function(c) { return ({userId: c.userId}); });
        if(missingCommentUsers.length > 0) fetchMissingProfiles(missingCommentUsers);
        renderThreadComments(comments);
    }, function(error) {
        console.error('Comments load error', error);
        container.innerHTML = `<div class="empty-state"><p>Unable to load comments right now.</p></div>`;
    }));
}

const renderCommentHtml = function(c, isReply) {
  const cAuthor = userCache[c.userId] || { name: "User", photoURL: null };

  const isLiked = Array.isArray(c.likedBy) && c.likedBy.includes(currentUser?.uid);
  const isDisliked = Array.isArray(c.dislikedBy) && c.dislikedBy.includes(currentUser?.uid);

  const avatarHtml = renderAvatar({ ...cAuthor, uid: c.userId }, { size: 36 });

  const parentCommentId = c.parentCommentId || c.parentId;
  const replyStyle = (isReply || !!parentCommentId)
    ? 'margin-left: 40px; border-left: 2px solid var(--border);'
    : '';

  const mediaHtml = c.mediaUrl
    ? `<div onclick="window.openFullscreenMedia('${c.mediaUrl}', 'image')">
         <img src="${c.mediaUrl}" style="max-width:200px; border-radius:8px; margin-top:5px; cursor:pointer;">
       </div>`
    : "";

  return `
    <div id="comment-${c.id}" style="margin-bottom: 15px; padding: 10px; border-bottom: 1px solid var(--border); ${replyStyle}">
      <div style="display:flex; gap:10px; align-items:flex-start;">
        ${avatarHtml}

        <div style="flex:1;">
          <div style="font-size:0.9rem; margin-bottom:2px;">
            <strong>${escapeHtml(cAuthor.name || 'User')}</strong>
            <span style="color:var(--text-muted); font-size:0.8rem;">
              • ${c.timestamp ? new Date(c.timestamp.seconds * 1000).toLocaleTimeString([], { hour:'2-digit', minute:'2-digit' }) : 'Now'}
            </span>
          </div>

          <div style="margin-top:2px; font-size:0.95rem; line-height:1.4;">
            ${escapeHtml(c.text || '')}
          </div>

          ${mediaHtml}

          <div style="margin-top:8px; display:flex; gap:15px; align-items:center;">
            <button onclick="window.moveInputToComment('${c.id}', '${escapeHtml(cAuthor.name || 'User')}')"
              style="background:none; border:none; color:var(--text-muted); font-size:0.8rem; cursor:pointer; display:flex; align-items:center; gap:5px;">
              <i class="ph ph-arrow-bend-up-left"></i> Reply
            </button>

            <button
              data-role="comment-like"
              data-comment-id="${c.id}"
              data-liked="${isLiked ? 'true' : 'false'}"
              data-disliked="${isDisliked ? 'true' : 'false'}"
              onclick="window.toggleCommentLike('${c.id}', event)"
              style="background:none; border:none; color:${isLiked ? '#00f2ea' : 'var(--text-muted)'}; font-size:0.8rem; cursor:pointer; display:flex; align-items:center; gap:5px;">
              <i class="${isLiked ? 'ph-fill' : 'ph'} ph-thumbs-up"></i>
              <span class="comment-like-count" id="comment-like-count-${c.id}">${c.likes || 0}</span>
            </button>

            <button
              data-role="comment-dislike"
              data-comment-id="${c.id}"
              data-liked="${isLiked ? 'true' : 'false'}"
              data-disliked="${isDisliked ? 'true' : 'false'}"
              onclick="window.toggleCommentDislike('${c.id}', event)"
              style="background:none; border:none; color:${isDisliked ? '#ff3d3d' : 'var(--text-muted)'}; font-size:0.8rem; cursor:pointer; display:flex; align-items:center; gap:5px;">
              <i class="${isDisliked ? 'ph-fill' : 'ph'} ph-thumbs-down"></i>
              <span class="comment-dislike-count" id="comment-dislike-count-${c.id}">${c.dislikes || 0}</span>
            </button>
          </div>

          <div id="reply-slot-${c.id}"></div>
        </div>
      </div>
    </div>`;
};

function renderThreadComments(comments) {
  const container = document.getElementById('thread-stream');
  if (!container) return;

  // Build parent -> replies map, and root list
  const byParent = {};
  const roots = [];

  (comments || []).forEach(function(c) {
    const parentId = c.parentCommentId || c.parentId;
    if (parentId) {
      (byParent[parentId] = byParent[parentId] || []).push(c);
    } else {
      roots.push(c);
    }
  });

  // Clear + render roots
  container.innerHTML = '';
  roots.sort(function(a, b) { return (a.timestamp?.seconds || 0) - (b.timestamp?.seconds || 0); });
  roots.forEach(function(c) { container.innerHTML += renderCommentHtml(c, false); });

  const renderReplies = function(parentId) {
    const replies = (byParent[parentId] || []).slice().sort(function(a, b) {
      return (a.timestamp?.seconds || 0) - (b.timestamp?.seconds || 0);
    });

    replies.forEach(function(reply) {
      const slot = document.getElementById(`reply-slot-${parentId}`);
      if (slot) slot.insertAdjacentHTML('beforeend', renderCommentHtml(reply, true));
      renderReplies(reply.id);
    });
  };

  roots.forEach(function(c) { renderReplies(c.id); });

  // Re-anchor input area if needed
  const inputArea = document.getElementById('thread-input-area');
  const defaultSlot = document.getElementById('thread-input-default-slot');
  if (inputArea && !inputArea.parentElement && defaultSlot) {
    defaultSlot.appendChild(inputArea);
  }

  // Move input under active reply target if set
  if (typeof activeReplyId !== 'undefined' && activeReplyId) {
    const slot = document.getElementById(`reply-slot-${activeReplyId}`);
    if (slot && inputArea && !slot.contains(inputArea)) {
      slot.appendChild(inputArea);
      const input = document.getElementById('thread-input');
      if (input) input.focus();
    }
  }
}



function renderThreadMainPost(postId) {
    const container = document.getElementById('thread-main-post');
    const post = allPosts.find(function(p) { return p.id === postId; });
    if(!post) return;

    const isLiked = post.likedBy && post.likedBy.includes(currentUser.uid);
    const isDisliked = post.dislikedBy && post.dislikedBy.includes(currentUser.uid);
    const isSaved = userProfile.savedPosts && userProfile.savedPosts.includes(postId);
    const isFollowingUser = followedUsers.has(post.userId);
    const isFollowingTopic = followedCategories.has(post.category);
    const isSelfPost = currentUser && post.userId === currentUser.uid;
    const topicClass = post.category.replace(/[^a-zA-Z0-9]/g, '');

    const authorData = userCache[post.userId] || { name: post.author, username: "user" };
    const date = post.timestamp && post.timestamp.seconds ? new Date(post.timestamp.seconds * 1000).toLocaleDateString() : 'Just now';
    const avatarHtml = renderAvatar({ ...authorData, uid: post.userId }, { size: 48 });

    const authorVerified = userHasRole(authorData, 'verified');
    const verifiedBadge = authorVerified ? '<span class="verified-badge" aria-label="Verified account">✔</span>' : '';
    const postText = typeof post.content === 'object' && post.content !== null ? (post.content.text || '') : (post.content || '');
    const formattedBody = formatContent(postText, post.tags, post.mentions);
    const tagListHtml = renderTagList(post.tags || []);
    const followButtons = isSelfPost ? '' : `
                                <button class="follow-btn js-follow-user-${post.userId} ${isFollowingUser ? 'following' : ''}" onclick="event.stopPropagation(); window.toggleFollowUser('${post.userId}', event)" style="font-size:0.75rem; padding:6px 12px;">${isFollowingUser ? 'Following' : '<i class="ph-bold ph-plus"></i> User'}</button>
                                <button class="follow-btn js-follow-topic-${topicClass} ${isFollowingTopic ? 'following' : ''}" onclick="event.stopPropagation(); window.toggleFollow('${post.category}', event)" style="font-size:0.75rem; padding:6px 12px;">${isFollowingTopic ? 'Following' : '<i class="ph-bold ph-plus"></i> Topic'}</button>`;

    let mediaContent = '';
    if (post.mediaUrl) { 
        if (post.type === 'video') mediaContent = `<div class="video-container" onclick="window.openFullscreenMedia('${post.mediaUrl}', 'video')"><video src="${post.mediaUrl}" controls class="post-media"></video></div>`; 
        else mediaContent = `<img src="${post.mediaUrl}" class="post-media" alt="Post Content" onclick="window.openFullscreenMedia('${post.mediaUrl}', 'image')">`; 
    }

    // UPDATE: Trust Badge Logic for Thread View to match Feed
    let trustBadge = "";
    if(post.trustScore > 2) {
        trustBadge = `<div style="font-size:0.75rem; color:#8b949e; display:flex; align-items:center; gap:4px; font-weight:600;"><i class="ph-fill ph-check-circle"></i> Publicly Verified</div>`;
    } else if(post.trustScore < -1) {
        trustBadge = `<div style="font-size:0.75rem; color:#ff3d3d; display:flex; align-items:center; gap:4px; font-weight:600;"><i class="ph-fill ph-warning-circle"></i> Disputed</div>`;
    }

    const myReview = window.myReviewCache ? window.myReviewCache[post.id] : null;
    const reviewDisplay = getReviewDisplay(myReview);

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
            <h2 id="thread-view-title" data-post-id="${post.id}" style="font-size: 1.4rem; font-weight: 800; margin-bottom: 0.5rem; line-height: 1.3;">${escapeHtml(post.title)}</h2>
            <p style="font-size: 1.1rem; line-height: 1.5; color: var(--text-main); margin-bottom: 1rem;">${formattedBody}</p>
            ${tagListHtml}
            ${mediaContent}
            <div style="margin-top: 1rem; padding: 10px 0; border-top: 1px solid var(--border); border-bottom: 1px solid var(--border); color: var(--text-muted); font-size: 0.9rem;">${date} • <span style="color:var(--text-main); font-weight:700;">${post.category}</span></div>
            <div class="card-actions" style="border:none; padding: 10px 0; justify-content: space-around;">
                <button id="thread-like-btn" class="action-btn" onclick="window.toggleLike('${post.id}', event)" style="color: ${isLiked ? '#00f2ea' : 'inherit'}; font-size: 1.2rem;"><i class="${isLiked ? 'ph-fill' : 'ph'} ph-thumbs-up"></i> <span style="font-size:1rem; margin-left:5px;">${post.likes || 0}</span></button>
                <button id="thread-dislike-btn" class="action-btn" onclick="window.toggleDislike('${post.id}', event)" style="color: ${isDisliked ? '#ff3d3d' : 'inherit'}; font-size: 1.2rem;"><i class="${isDisliked ? 'ph-fill' : 'ph'} ph-thumbs-down"></i> <span style="font-size:1rem; margin-left:5px;">${post.dislikes || 0}</span></button>
                <button class="action-btn" onclick="document.getElementById('thread-input').focus()" style="color: var(--primary); font-size: 1.2rem;"><i class="ph ph-chat-circle"></i> <span style="font-size:1rem; margin-left:5px;">Comment</span></button>
                <button id="thread-save-btn" class="action-btn" onclick="window.toggleSave('${post.id}', event)" style="font-size: 1.2rem; color: ${isSaved ? '#00f2ea' : 'inherit'}"><i class="${isSaved ? 'ph-fill' : 'ph'} ph-bookmark-simple"></i> <span style="font-size:1rem; margin-left:5px;">${isSaved ? 'Saved' : 'Save'}</span></button>
                <button id="thread-review-btn" class="action-btn review-action ${reviewDisplay.className}" data-post-id="${post.id}" data-icon-size="1.2rem" onclick="event.stopPropagation(); window.openPeerReview('${post.id}')" style="font-size: 1.2rem;"><i class="ph ph-scales"></i> <span style="font-size:1rem; margin-left:5px;">${reviewDisplay.label}</span></button>
            </div>
        </div>`;

    const inputPfp = document.getElementById('thread-input-pfp');
    if(inputPfp) applyAvatarToElement(inputPfp, userProfile, { size: 40 });

    const threadReviewBtn = document.getElementById('thread-review-btn');
    applyReviewButtonState(threadReviewBtn, myReview);
    applyMyReviewStylesToDOM();
}

window.sendComment = async function() {
    const input = document.getElementById('thread-input');
    const fileInput = document.getElementById('thread-file');
    const text = input.value.trim();

    if(!text && !fileInput.files[0]) return;

    const btn = document.getElementById('thread-send-btn');
    btn.disabled = true; 
    btn.textContent = "...";

    try {
        let mediaUrl = null;
        if(fileInput.files[0]) {
            const path = `comments/${currentUser.uid}/${Date.now()}_${fileInput.files[0].name}`;
            mediaUrl = await uploadFileToStorage(fileInput.files[0], path);
        }

        const parentCommentId = normalizeReplyTarget(activeReplyId);
        const payload = buildReplyRecord({ text, mediaUrl, parentCommentId, userId: currentUser.uid });
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
    } catch(e) { 
        console.error(e);
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

window.toggleCommentLike = async function(commentId, event) {
    if(event) event.stopPropagation();
    if(!activePostId || !currentUser) return;
    const commentRef = doc(db, 'posts', activePostId, 'comments', commentId);
    const btn = event?.currentTarget;
    let comment = threadComments.find(function(c) { return c.id === commentId; });

    if(!comment) {
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
        likedBy = likedBy.filter(function(uid) { return uid !== currentUser.uid; });
    } else {
        likes = likes + 1;
        if (!likedBy.includes(currentUser.uid)) likedBy.push(currentUser.uid);
        if (hadDisliked) {
            dislikes = Math.max(0, dislikes - 1);
            dislikedBy = dislikedBy.filter(function(uid) { return uid !== currentUser.uid; });
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
    } catch(e) { console.error(e); }
}

window.toggleDislike = async function(postId, event) {
    if(event) event.stopPropagation();
    if(!currentUser) return alert("Please log in to dislike posts.");

    const post = allPosts.find(function(p) { return p.id === postId; });
    if(!post) return;

    const wasDisliked = post.dislikedBy && post.dislikedBy.includes(currentUser.uid);
    const hadLiked = post.likedBy && post.likedBy.includes(currentUser.uid);

    if (wasDisliked) {
        post.dislikes = Math.max(0, (post.dislikes || 0) - 1);
        post.dislikedBy = (post.dislikedBy || []).filter(function(uid) { return uid !== currentUser.uid; });
    } else {
        post.dislikes = (post.dislikes || 0) + 1;
        if (!post.dislikedBy) post.dislikedBy = [];
        post.dislikedBy.push(currentUser.uid);
        if (hadLiked) {
            post.likes = Math.max(0, (post.likes || 0) - 1);
            post.likedBy = (post.likedBy || []).filter(function(uid) { return uid !== currentUser.uid; });
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

window.toggleCommentDislike = async function(commentId, event) {
  if (event) event.stopPropagation();
  if (!activePostId || !currentUser) return;

  const commentRef = doc(db, 'posts', activePostId, 'comments', commentId);
  const btn = event?.currentTarget;

  let comment = threadComments.find(function(c) { return c.id === commentId; });

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
    dislikedBy = dislikedBy.filter(function(uid) { return uid !== currentUser.uid; });
  } else {
    dislikes = dislikes + 1;
    if (!dislikedBy.includes(currentUser.uid)) dislikedBy.push(currentUser.uid);

    if (hadLiked) {
      likes = Math.max(0, likes - 1);
      likedBy = likedBy.filter(function(uid) { return uid !== currentUser.uid; });
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
window.renderDiscover = async function() {
    const container = document.getElementById('discover-results');
    container.innerHTML = "";

    const postsSelect = document.getElementById('posts-sort-select');
    if (postsSelect) postsSelect.value = discoverPostsSort;
    const categoriesSelect = document.getElementById('categories-sort-select');
    if (categoriesSelect) categoriesSelect.value = discoverCategoriesMode;

    const categoriesDropdown = function(id = 'section') {
        return `<div class="discover-dropdown"><label for="categories-${id}-select">Categories:</label><select id="categories-${id}-select" class="discover-select" onchange="window.handleCategoriesModeChange(event)">
            <option value="verified_first" ${discoverCategoriesMode === 'verified_first' ? 'selected' : ''}>Verified first</option>
            <option value="verified_only" ${discoverCategoriesMode === 'verified_only' ? 'selected' : ''}>Verified only</option>
            <option value="community_first" ${discoverCategoriesMode === 'community_first' ? 'selected' : ''}>Community first</option>
            <option value="community_only" ${discoverCategoriesMode === 'community_only' ? 'selected' : ''}>Community only</option>
        </select></div>`;
    };

    const renderVideosSection = async function(onlyVideos = false) {
        if(!videosCache.length) {
            const snap = await getDocs(query(collection(db, 'videos'), orderBy('createdAt', 'desc')));
            videosCache = snap.docs.map(function(d) { return ({ id: d.id, ...d.data() }); });
        }
        let filteredVideos = videosCache;
        if(discoverSearchTerm) {
            filteredVideos = filteredVideos.filter(function(v) {
                return (v.caption || '').toLowerCase().includes(discoverSearchTerm) ||
                (v.hashtags || []).some(function(tag) { return (`#${tag}`).toLowerCase().includes(discoverSearchTerm); });
            });
        }
        if(filteredVideos.length === 0) {
            if(onlyVideos) container.innerHTML = `<div class="empty-state"><p>No videos found.</p></div>`;
            return;
        }

        container.innerHTML += `<div class="discover-section-header">Videos</div>`;
        filteredVideos.forEach(function(video) {
            const tags = (video.hashtags || []).map(function(t) { return '#' + t; }).join(' ');
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

    const renderUsers = function() {
        let matches = [];
        if (discoverSearchTerm) {
            matches = Object.values(userCache).filter(function(u) {
                return (u.name && u.name.toLowerCase().includes(discoverSearchTerm)) ||
                (u.username && u.username.toLowerCase().includes(discoverSearchTerm));
            });
        } else if (discoverFilter === 'All Results') {
            matches = Object.values(userCache).slice(0, 5); 
        }

        if(matches.length > 0) {
            container.innerHTML += `<div class="discover-section-header">Users</div>`;
            matches.forEach(function(user) {
                const uid = Object.keys(userCache).find(function(key) { return userCache[key] === user; });
                if(!uid) return;
                const avatarHtml = renderAvatar({ ...user, uid }, { size: 40 });

                container.innerHTML += `
                    <div class="social-card" style="padding:1rem; cursor:pointer; display:flex; align-items:center; gap:10px; border-left: 4px solid var(--border);" onclick="window.openUserProfile('${uid}')">
                        ${avatarHtml}
                        <div>
                            <div style="font-weight:700;">${escapeHtml(user.name)}</div>
                            <div style="color:var(--text-muted); font-size:0.9rem;">@${escapeHtml(user.username)}</div>
                        </div>
                        <button class="follow-btn" style="margin-left:auto;">View</button>
                    </div>`;
            });
        } else if (discoverFilter === 'Users' && discoverSearchTerm) {
            container.innerHTML = `<div class="empty-state"><p>No users matching "${discoverSearchTerm}"</p></div>`;
        }
    };

    const renderLiveSection = function() {
        if(MOCK_LIVESTREAMS.length > 0) {
            container.innerHTML += `<div class="discover-section-header">Livestreams</div>`;
            MOCK_LIVESTREAMS.forEach(function(stream) {
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

    const renderPostsSection = function() {
        let filteredPosts = allPosts;
        if(discoverSearchTerm) {
            filteredPosts = allPosts.filter(function(p) {
                const body = typeof p.content === 'string' ? p.content : (p.content?.text || '');
                return (p.title || '').toLowerCase().includes(discoverSearchTerm) || body.toLowerCase().includes(discoverSearchTerm);
            });
        }

        if(discoverPostsSort === 'popular') {
            filteredPosts.sort(function(a,b) { return (b.likes || 0) - (a.likes || 0); });
        } else {
            filteredPosts.sort(function(a,b) { return (b.timestamp?.seconds || 0) - (a.timestamp?.seconds || 0); });
        }

        if(filteredPosts.length > 0) {
            container.innerHTML += `<div class="discover-section-header">Posts</div>`;
            filteredPosts.forEach(function(post) {
                const author = userCache[post.userId] || {name: post.author};
                const body = typeof post.content === 'string' ? post.content : (post.content?.text || '');
                container.innerHTML += `
                    <div class="social-card" style="border-left: 2px solid ${THEMES[post.category] || 'transparent'}; cursor:pointer;" onclick="window.openThread('${post.id}')">
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
                        </div>
                    </div>`;
            });
        } else if (discoverFilter === 'Posts' && discoverSearchTerm) {
            container.innerHTML = `<div class="empty-state"><p>No posts found.</p></div>`;
        }
    };

    const renderCategoriesSection = function(onlyCategories = false) {
        let filteredCategories = categories.slice();
        if (discoverSearchTerm) {
            const term = discoverSearchTerm.toLowerCase();
            filteredCategories = filteredCategories.filter(function(c) {
                return (c.name || '').toLowerCase().includes(term) || (c.slug || '').toLowerCase().includes(term) || (c.description || '').toLowerCase().includes(term);
            });
        }

        if (discoverCategoriesMode === 'verified_only') {
            filteredCategories = filteredCategories.filter(function(c) { return !!c.verified; });
        } else if (discoverCategoriesMode === 'community_only') {
            filteredCategories = filteredCategories.filter(function(c) { return (c.type || 'community') === 'community'; });
        }

        const sorted = filteredCategories.slice().sort(function(a, b) {
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
            visible.forEach(function(cat) {
                const verifiedMark = cat.verified ? '<span class="verified-badge">✔</span>' : '';
                const typeLabel = (cat.type || 'community') === 'community' ? 'Community' : 'Official';
                const memberLabel = typeof cat.memberCount === 'number' ? `${cat.memberCount} members` : '';
                container.innerHTML += `
                    <div class="social-card" style="padding:1rem; display:flex; gap:12px; align-items:center; border-left: 2px solid ${cat.verified ? '#00f2ea' : 'var(--border)'};">
                        <div class="user-avatar" style="width:46px; height:46px; background:${getColorForUser(cat.name || 'C')};">${(cat.name || 'C')[0]}</div>
                        <div style="flex:1;">
                            <div style="font-weight:800; display:flex; align-items:center; gap:6px;">${escapeHtml(cat.name || 'Category')}${verifiedMark}</div>
                            <div style="color:var(--text-muted); font-size:0.9rem; display:flex; gap:10px; align-items:center; flex-wrap:wrap;">${escapeHtml(typeLabel)}${memberLabel ? ' · ' + memberLabel : ''}</div>
                        </div>
                        <div class="category-badge">${escapeHtml(cat.slug || cat.id || '')}</div>
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
        if(container.innerHTML === "") container.innerHTML = `<div class="empty-state"><p>Start typing to search everything.</p></div>`;
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
window.openUserProfile = async function(uid, event, pushToStack = true) {
    if(event) event.stopPropagation();
    if (uid === currentUser.uid) {
        window.navigateTo('profile', pushToStack);
        return;
    } 

    viewingUserId = uid; 
    currentProfileFilter = 'All'; 
    window.navigateTo('public-profile', pushToStack); 

    let profile = userCache[uid];
    if (!profile) {
        const docSnap = await getDoc(doc(db, "users", uid));
        if (docSnap.exists()) {
            profile = normalizeUserProfileData(docSnap.data());
            userCache[uid] = profile;
        } else {
            profile = { name: "Unknown User", username: "unknown" };
        }
    }
    renderPublicProfile(uid, profile);
}

window.openUserProfileByHandle = async function(handle) {
    const normalized = (handle || '').replace(/^@/, '').toLowerCase();
    const cachedEntry = Object.entries(userCache).find(function([_, data]) { return (data.username || '').toLowerCase() === normalized; });
    if (cachedEntry) {
        openUserProfile(cachedEntry[0], null, true);
        return;
    }
    const q = query(collection(db, 'users'), where('username', '==', normalized));
    const snap = await getDocs(q);
    if (!snap.empty) {
        const docSnap = snap.docs[0];
        userCache[docSnap.id] = normalizeUserProfileData(docSnap.data());
        openUserProfile(docSnap.id, null, true);
    }
}

function renderPublicProfile(uid, profileData = userCache[uid]) {
    if(!profileData) return;
    const normalizedProfile = normalizeUserProfileData(profileData);
    const container = document.getElementById('view-public-profile');

    const avatarHtml = renderAvatar({ ...normalizedProfile, uid }, { size: 100, className: 'profile-pic' });

    const isFollowing = followedUsers.has(uid);
    const isSelfView = currentUser && currentUser.uid === uid;
    const userPosts = allPosts.filter(function(p) { return p.userId === uid && (isSelfView || p.visibility !== 'private'); });
    const filteredPosts = currentProfileFilter === 'All' ? userPosts : userPosts.filter(function(p) { return p.category === currentProfileFilter; });

    const followCta = isSelfView ? '' : `<button onclick=\"window.toggleFollowUser('${uid}', event)\" class=\"create-btn-sidebar js-follow-user-${uid}\" style=\"width: auto; padding: 0.6rem 2rem; margin-top: 0; background: ${isFollowing ? 'transparent' : 'var(--primary)'}; border: 1px solid var(--primary); color: ${isFollowing ? 'var(--primary)' : 'black'};\">${isFollowing ? 'Following' : 'Follow'}</button>`;

    let linkHtml = ''; 
    if(normalizedProfile.links) {
        let url = normalizedProfile.links;
        if(!url.startsWith('http')) url = 'https://' + url;
        linkHtml = `<a href="${url}" target="_blank" style="color: var(--primary); font-size: 0.9rem; text-decoration: none; margin-top: 5px; display: inline-block;">🔗 ${escapeHtml(normalizedProfile.links)}</a>`;
    }

    const followersCount = normalizedProfile.followersCount || 0;
    const profileRoles = getAccountRoleSet(normalizedProfile);
    const verifiedBadge = profileRoles.has('verified') ? '<span class="verified-badge" style="margin-left:6px;">✔</span>' : '';

    // FIX: Added specific ID to follower count for real-time updates
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
                <div class="stat-item"><div>${userPosts.reduce(function(acc, p) { return acc + (p.likes||0); }, 0)}</div><div>Likes</div></div>
                <div class="stat-item"><div>${userPosts.length}</div><div>Posts</div></div>
            </div>
            <div style="display:flex; gap:10px; justify-content:center; margin-top:1rem;">
                ${followCta}
                ${isSelfView ? '' : '<button class="create-btn-sidebar" style="width: auto; padding: 0.6rem 2rem; margin-top: 0; background: var(--bg-hover); color: var(--text-main); border: 1px solid var(--border);">Message</button>'}
            </div>
        </div>
        <div style="padding: 1rem; border-bottom: 1px solid var(--border);">
            <select onchange="window.setProfileFilter(this.value, '${uid}')" class="form-select" style="margin-bottom:0; width:auto;">
                <option value="All">All Posts</option>
                <option value="STEM">STEM</option>
                <option value="Coding">Coding</option>
                <option value="History">History</option>
                <option value="Gaming">Gaming</option>
            </select>
        </div>
        <div id="public-profile-feed" style="padding: 1rem; max-width: 800px; margin: 0 auto;"></div>`; 

    const feedContainer = document.getElementById('public-profile-feed'); 

    if(filteredPosts.length === 0) { 
        feedContainer.innerHTML = `<div class="empty-state"><p>No posts found in ${currentProfileFilter}.</p></div>`; 
    } else { 
            feedContainer.innerHTML = "";
            filteredPosts.forEach(function(post) {
                const date = post.timestamp && post.timestamp.seconds ? new Date(post.timestamp.seconds * 1000).toLocaleDateString() : 'Just now';
                const isLiked = post.likedBy && post.likedBy.includes(currentUser.uid);
                const isSaved = userProfile.savedPosts && userProfile.savedPosts.includes(post.id);
                const postText = typeof post.content === 'object' && post.content !== null ? (post.content.text || '') : (post.content || '');
                const formattedBody = formatContent(postText, post.tags, post.mentions);
                const tagListHtml = renderTagList(post.tags || []);

            let mediaContent = '';
            if (post.mediaUrl) {
                if (post.type === 'video') mediaContent = `<div class="video-container" onclick="window.openFullscreenMedia('${post.mediaUrl}', 'video')"><video src="${post.mediaUrl}" controls class="post-media"></video></div>`;
                else mediaContent = `<img src="${post.mediaUrl}" class="post-media" alt="Post Content" onclick="window.openFullscreenMedia('${post.mediaUrl}', 'image')">`;
            } 

                const membership = isSelfView ? memberships[post.categoryId] : null;
                const inactive = membership && membership.status !== 'active';
                const badgeClass = inactive ? 'category-badge inactive' : 'category-badge';
                const badgeLabel = inactive ? `${post.category} (Not a member)` : post.category;

                feedContainer.innerHTML += `
                <div class="social-card" style="border-left: 2px solid ${THEMES[post.category] || 'transparent'};">
                    <div class="card-content" style="padding-top:1rem; cursor: pointer;" onclick="window.openThread('${post.id}')">
                        <div style="display:flex; align-items:center; justify-content:space-between; gap:10px;">
                            <div class="${badgeClass}">${badgeLabel}</div>
                            <div style="display:flex; align-items:center; gap:8px;">
                                <span style="font-size:0.8rem; color:var(--text-muted);">${date}</span>
                                ${getPostOptionsButton(post, 'public-profile', '1rem')}
                            </div>
                        </div>
                        <h3 class="post-title">${escapeHtml(cleanText(post.title))}</h3>
                        <p>${formattedBody}</p>
                        ${tagListHtml}
                        ${mediaContent}
                    </div>
                    <div class="card-actions">
                        <button class="action-btn" onclick="window.toggleLike('${post.id}', event)" style="color: ${isLiked ? '#00f2ea' : 'inherit'}"><span>${isLiked ? '👍' : '👍'}</span> ${post.likes || 0}</button>
                        <button class="action-btn" onclick="window.openThread('${post.id}')"><span>💬</span> Discuss</button>
                        <button class="action-btn" onclick="window.toggleSave('${post.id}', event)" style="color: ${isSaved ? '#00f2ea' : 'inherit'}"><span>${isSaved ? '🔖' : '🔖'}</span> ${isSaved ? 'Saved' : 'Save'}</button>
                    </div>
                </div>`;
        });
    }
}

function renderProfile() {
    const userPosts = allPosts.filter(function(p) { return p.userId === currentUser.uid; });
    const filteredPosts = currentProfileFilter === 'All' ? userPosts : userPosts.filter(function(p) { return p.category === currentProfileFilter; });

    const displayName = userProfile.name || userProfile.nickname || "Nexera User";
    const verifiedBadge = hasGlobalRole('verified') ? '<span class="verified-badge" style="margin-left:6px;">✔</span>' : '';
    const avatarHtml = renderAvatar({ ...userProfile, uid: currentUser?.uid }, { size: 100, className: 'profile-pic' });

    let linkHtml = '';
    if(userProfile.links) {
        let url = userProfile.links;
        if(!url.startsWith('http')) url = 'https://' + url;
        linkHtml = `<a href="${url}" target="_blank" style="color: var(--primary); font-size: 0.9rem; text-decoration: none; margin-top: 5px; display: inline-flex; align-items:center; gap:5px;"> <i class="ph-bold ph-link"></i> ${escapeHtml(userProfile.links)}</a>`;
    }

    const followersCount = userProfile.followersCount || 0;
    const regionHtml = userProfile.region ? `<div class="real-name-subtext"><i class=\"ph ph-map-pin\"></i> ${escapeHtml(userProfile.region)}</div>` : '';
    const realNameHtml = userProfile.realName ? `<div class="real-name-subtext">${escapeHtml(userProfile.realName)}</div>` : '';

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
                <div class="stat-item"><div>${userPosts.reduce(function(acc, p) { return acc + (p.likes||0); }, 0)}</div><div>Likes</div></div>
                <div class="stat-item"><div>${userPosts.length}</div><div>Posts</div></div>
            </div>
            <button onclick="window.toggleSettingsModal(true)" class="create-btn-sidebar" style="width:auto; margin-top:1rem; background:transparent; border:1px solid var(--border); color:var(--text-muted);"><i class="ph ph-gear"></i> Edit Profile & Settings</button>
            <button onclick="window.handleLogout()" class="create-btn-sidebar" style="width:auto; margin-top:10px; background:transparent; border:1px solid var(--border); color:var(--text-muted);"><i class="ph ph-sign-out"></i> Log Out</button>
        </div>
        <div style="padding:1rem; border-bottom:1px solid var(--border);"><select onchange="window.setProfileFilter(this.value, 'me')" class="form-select" style="margin-bottom:0; width:auto;"><option value="All">All Posts</option><option value="STEM">STEM</option><option value="Coding">Coding</option><option value="History">History</option><option value="Gaming">Gaming</option></select></div><div id="my-profile-feed" style="padding:1rem; max-width:800px; margin:0 auto;"></div>
    `; 

    const feedContainer = document.getElementById('my-profile-feed'); 

    if(filteredPosts.length === 0) {
        feedContainer.innerHTML = `<div class="empty-state"><p>No posts.</p></div>`; 
    } else { 
        feedContainer.innerHTML = "";
        filteredPosts.forEach(function(post) {
            const date = post.timestamp ? new Date(post.timestamp.seconds * 1000).toLocaleDateString() : 'Just now';
            const membership = memberships[post.categoryId];
            const inactive = membership && membership.status !== 'active';
            const badgeClass = inactive ? 'category-badge inactive' : 'category-badge';
            const badgeLabel = inactive ? `${post.category} (Not a member anymore)` : post.category;

            feedContainer.innerHTML += `
                <div class="social-card" style="border-left: 2px solid ${THEMES[post.category] || 'transparent'};">
                    <div class="card-content" style="padding-top:1rem; cursor: pointer;" onclick="window.openThread('${post.id}')">
                        <div style="display:flex; align-items:center; justify-content:space-between; gap:10px;">
                            <div class="${badgeClass}">${badgeLabel}</div>
                            <div style="display:flex; align-items:center; gap:8px;">
                                <span style="font-size:0.8rem; color:var(--text-muted);">${date}</span>
                                ${getPostOptionsButton(post, 'profile', '1rem')}
                            </div>
                        </div>
                        <h3 class="post-title">${escapeHtml(cleanText(post.title))}</h3>
                        <p>${escapeHtml(cleanText(post.content))}</p>
                    </div>
                </div>`;
        });
    }
}

// --- Utils & Helpers ---
function collectFollowedCategoryNames() {
    const names = new Set();
    Object.keys(memberships || {}).forEach(function(id) {
        const snapshot = getCategorySnapshot(id);
        const name = snapshot?.name || snapshot?.id || id;
        if ((memberships[id]?.status || 'active') !== 'left') names.add(name);
    });
    followedCategories.forEach(function(name) { names.add(name); });
    return Array.from(names);
}

function computeTrendingCategories(limit = 8) {
    const counts = {};
    allPosts.forEach(function(post) {
        if (post.category) counts[post.category] = (counts[post.category] || 0) + 1;
    });
    return Object.entries(counts).sort(function(a, b) { return b[1] - a[1]; }).slice(0, limit).map(function(entry) { return entry[0]; });
}

function renderCategoryPills() {
    const header = document.getElementById('category-header');
    if (!header) return;
    header.innerHTML = '';

    const anchors = ['For You', 'Following'];
    anchors.forEach(function(label) {
        const pill = document.createElement('div');
        pill.className = 'category-pill' + (currentCategory === label ? ' active' : '');
        pill.textContent = label;
        pill.onclick = function() { window.setCategory(label); };
        header.appendChild(pill);
    });

    const divider = document.createElement('div');
    divider.className = 'category-divider';
    header.appendChild(divider);

    const dynamic = Array.from(new Set([...collectFollowedCategoryNames(), ...computeTrendingCategories(10)])).filter(function(name) {
        return name && !anchors.includes(name);
    }).slice(0, 10);

    dynamic.forEach(function(name) {
        const pill = document.createElement('div');
        pill.className = 'category-pill' + (currentCategory === name ? ' active' : '');
        pill.textContent = name;
        pill.onclick = function() { window.setCategory(name); };
        header.appendChild(pill);
    });
}

window.setCategory = function(c) {
    currentCategory = c;
    renderCategoryPills();
    document.documentElement.style.setProperty('--primary', '#00f2ea');
    renderFeed();
}

window.renderLive = function() { 
    const container = document.getElementById('live-grid-container'); 
    if(!container) return; 
    container.innerHTML = ""; 

    if(MOCK_LIVESTREAMS.length === 0) { 
        container.innerHTML = `<div class="empty-state">No active livestreams.</div>`; 
        return; 
    } 

    container.style.display = "grid"; 
    container.style.gridTemplateColumns = "repeat(auto-fill, minmax(280px, 1fr))"; 
    container.style.gap = "20px"; 

    MOCK_LIVESTREAMS.forEach(function(stream) { 
        container.innerHTML += `
            <div class="social-card" style="border-top: 4px solid ${stream.color}; cursor:pointer; transition:0.2s; overflow:hidden;">
                <div style="height:150px; background:${stream.color}; opacity:0.8; display:flex; align-items:center; justify-content:center; color:black; font-weight:900; font-size:1.5rem;"><i class="ph-fill ph-broadcast" style="margin-right:8px;"></i> LIVE</div>
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

function cleanText(text) { if(typeof text !== 'string') return ""; return text.replace(new RegExp(["badword", "hate"].join("|"), "gi"), "🤐"); }
function renderSaved() { currentCategory = 'Saved'; renderFeed('saved-content'); }

// Small Interaction Utils
window.setDiscoverFilter = function(filter) {
    discoverFilter = filter;
    document.querySelectorAll('.discover-pill').forEach(function(el) {
        if(el.dataset.filter === filter) el.classList.add('active');
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
window.handlePostsSortChange = function(e) { discoverPostsSort = e.target.value; renderDiscover(); }
window.handleCategoriesModeChange = function(e) { discoverCategoriesMode = e.target.value; renderDiscover(); }
window.handleSearchInput = function(e) { discoverSearchTerm = e.target.value.toLowerCase(); renderDiscover(); }
window.setSavedFilter = function(filter) { savedFilter = filter; document.querySelectorAll('.saved-pill').forEach(function(el) { if(el.textContent === filter) el.classList.add('active'); else el.classList.remove('active'); }); renderSaved(); }
window.handleSavedSearch = function(e) { savedSearchTerm = e.target.value.toLowerCase(); renderSaved(); }
window.openFullscreenMedia = function(url, type) { const modal = document.getElementById('media-modal'); const content = document.getElementById('media-modal-content'); if(!modal || !content) return; modal.style.display = 'flex'; if(type === 'video') content.innerHTML = `<video src="${url}" controls style="max-width:100%; max-height:90vh; border-radius:8px;" autoplay></video>`; else content.innerHTML = `<img src="${url}" style="max-width:100%; max-height:90vh; border-radius:8px;">`; }
window.closeFullscreenMedia = function() { const modal = document.getElementById('media-modal'); if(modal) modal.style.display = 'none'; const content = document.getElementById('media-modal-content'); if(content) content.innerHTML = ''; }
window.addTagToSaved = async function(postId) { const tag = prompt("Enter a tag for this saved post (e.g. 'Science', 'Read Later'):"); if(!tag) return; userProfile.savedTags = userProfile.savedTags || {}; userProfile.savedTags[postId] = tag; await setDoc(doc(db, "users", currentUser.uid), { savedTags: userProfile.savedTags }, { merge: true }); renderSaved(); }
window.setProfileFilter = function(category, uid) { currentProfileFilter = category; if (uid === 'me') renderProfile(); else renderPublicProfile(uid); }
window.moveInputToComment = function(commentId, authorName) { activeReplyId = commentId; const slot = document.getElementById(`reply-slot-${commentId}`); const inputArea = document.getElementById('thread-input-area'); const input = document.getElementById('thread-input'); const cancelBtn = document.getElementById('thread-cancel-btn'); if (slot && inputArea) { slot.appendChild(inputArea); input.placeholder = `Replying to ${authorName}...`; if(cancelBtn) cancelBtn.style.display = 'inline-block'; input.focus(); } }
window.resetInputBox = function() { activeReplyId = null; const defaultSlot = document.getElementById('thread-input-default-slot'); const inputArea = document.getElementById('thread-input-area'); const input = document.getElementById('thread-input'); const cancelBtn = document.getElementById('thread-cancel-btn'); if (defaultSlot && inputArea) { defaultSlot.appendChild(inputArea); input.placeholder = "Post your reply"; input.value = ""; if(cancelBtn) cancelBtn.style.display = 'none'; } }
window.triggerFileSelect = function() { document.getElementById('thread-file').click(); }
window.handleFileSelect = function(input) { const btn = document.getElementById('attach-btn-text'); if(input.files && input.files[0]) { btn.innerHTML = `<i class="ph-fill ph-file-image" style="color:var(--primary);"></i> ` + input.files[0].name.substring(0, 15) + "..."; btn.style.color = "var(--primary)"; } else { btn.innerHTML = `<i class="ph ph-paperclip"></i> Attach`; btn.style.color = "var(--text-muted)"; } }
window.previewPostImage = function(input) { if(input.files && input.files[0]) { const reader = new FileReader(); reader.onload = function(e) { document.getElementById('img-preview-tag').src = e.target.result; document.getElementById('img-preview-container').style.display = 'block'; }; reader.readAsDataURL(input.files[0]); } }
window.clearPostImage = function() { document.getElementById('postFile').value = ""; document.getElementById('img-preview-container').style.display = 'none'; document.getElementById('img-preview-tag').src = ""; }
window.togglePostOption = function(type) { const area = document.getElementById('extra-options-area'); const target = document.getElementById('post-opt-' + type); ['poll', 'gif', 'schedule', 'location'].forEach(function(t) { if(t !== type) document.getElementById('post-opt-' + t).style.display = 'none'; }); if (target.style.display === 'none') { area.style.display = 'block'; target.style.display = 'block'; } else { target.style.display = 'none'; area.style.display = 'none'; } }
window.closeReview = function() { return document.getElementById('review-modal').style.display = 'none'; };
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

window.openPostOptions = function(event, postId, ownerId, context = 'feed') {
    if(!requireAuth()) return;
    activeOptionsPost = { id: postId, ownerId, context };
    const dropdown = document.getElementById('post-options-dropdown');
    const deleteBtn = document.getElementById('dropdown-delete-btn');
    if(deleteBtn) deleteBtn.style.display = currentUser && ownerId === currentUser.uid ? 'flex' : 'none';
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

window.closePostOptions = function() { const modal = document.getElementById('post-options-modal'); if(modal) modal.style.display = 'none'; closePostOptionsDropdown(); }
window.handlePostOptionSelect = function(action) { closePostOptionsDropdown(); if(action === 'report') return window.openReportModal(); if(action === 'delete') return window.confirmDeletePost(); }
window.openReportModal = function() { closePostOptionsDropdown(); const opts = document.getElementById('post-options-modal'); if(opts) opts.style.display = 'none'; const modal = document.getElementById('report-modal'); if(modal) modal.style.display = 'flex'; }
window.closeReportModal = function() { const modal = document.getElementById('report-modal'); if(modal) modal.style.display = 'none'; }
window.submitReport = async function() { if(!requireAuth()) return; if(!activeOptionsPost || !activeOptionsPost.id || !activeOptionsPost.ownerId) return toast('No post selected', 'error'); const categoryEl = document.getElementById('report-category'); const detailEl = document.getElementById('report-details'); const category = categoryEl ? categoryEl.value : ''; const details = detailEl ? detailEl.value.trim().substring(0, 500) : ''; if(!category) return toast('Please choose a category.', 'error'); try { await addDoc(collection(db, 'reports'), { postId: activeOptionsPost.id, reportedUserId: activeOptionsPost.ownerId, reporterUserId: currentUser.uid, category, details, createdAt: serverTimestamp(), context: activeOptionsPost.context || currentViewId, type: 'post', reason: details }); if(detailEl) detailEl.value = ''; if(categoryEl) categoryEl.value = ''; window.closeReportModal(); toast('Report submitted', 'info'); } catch(e) { console.error(e); toast('Could not submit report.', 'error'); } }
window.confirmDeletePost = async function() { if(!activeOptionsPost || !activeOptionsPost.id) return; if(!currentUser || activeOptionsPost.ownerId !== currentUser.uid) return toast('You can only delete your own post.', 'error'); const ok = confirm('Are you sure?'); if(!ok) return; try { await deleteDoc(doc(db, 'posts', activeOptionsPost.id)); allPosts = allPosts.filter(function(p) { return p.id !== activeOptionsPost.id; }); renderFeed(); if(currentViewId === 'profile') renderProfile(); if(currentViewId === 'public-profile' && viewingUserId) renderPublicProfile(viewingUserId); if(activePostId === activeOptionsPost.id) { activePostId = null; window.navigateTo('feed'); const threadStream = document.getElementById('thread-stream'); if(threadStream) threadStream.innerHTML = ''; } window.closePostOptions(); toast('Post deleted', 'info'); } catch(e) { console.error('Delete error', e); toast('Failed to delete post', 'error'); } }

// --- Messaging (DMs) ---
window.toggleNewChatModal = function(show = true) {
    const modal = document.getElementById('new-chat-modal');
    if(modal) modal.style.display = show ? 'flex' : 'none';
};
window.openNewChatModal = function() { return window.toggleNewChatModal(true); };

window.searchChatUsers = async function(term = '') {
    const resultsEl = document.getElementById('chat-search-results');
    if(!resultsEl) return;
    resultsEl.innerHTML = '';
    const cleaned = term.trim().toLowerCase();
    if(cleaned.length < 2) return;
    const qSnap = await getDocs(query(collection(db, 'users'), where('username', '>=', cleaned), where('username', '<=', cleaned + '~')));
    qSnap.forEach(function(docSnap) {
        const data = docSnap.data();
        const row = document.createElement('div');
        row.className = 'conversation-item';
        row.innerHTML = `<div><strong>@${data.username || 'user'}</strong><div style="color:var(--text-muted); font-size:0.85rem;">${data.displayName || data.name || 'Nexera User'}</div></div>`;
        row.onclick = function() { return createConversationWithUser(docSnap.id, data); };
        resultsEl.appendChild(row);
    });
};

async function createConversationWithUser(targetUid, targetData = {}) {
    if(!requireAuth()) return;
    try {
        const sortedMembers = [currentUser.uid, targetUid].sort();
        const existing = conversationsCache.find(function(c) {
            const members = c.members || [];
            return members.length === 2 && sortedMembers.every(function(id) { return members.includes(id); });
        });
        if(existing) {
            setActiveConversation(existing.id, existing);
            toggleNewChatModal(false);
            return;
        }

        const convoId = `${sortedMembers[0]}_${sortedMembers[1]}`;
        const convoRef = doc(db, 'conversations', convoId);
        const existingSnap = await getDoc(convoRef);
        if(existingSnap.exists()) {
            const data = existingSnap.data();
            conversationsCache.push({ id: convoId, ...data });
            toggleNewChatModal(false);
            setActiveConversation(convoId, data);
            return;
        }

        const payload = {
            members: sortedMembers,
            createdAt: serverTimestamp(),
            updatedAt: serverTimestamp(),
            lastMessageText: '',
            lastMessageAt: serverTimestamp(),
            requestState: { [currentUser.uid]: 'inbox', [targetUid]: 'requested' }
        };

        await setDoc(convoRef, payload, { merge: true });
        conversationsCache.push({ id: convoId, ...payload });
        toggleNewChatModal(false);
        setActiveConversation(convoId, payload);
    } catch(err) {
        console.error('Conversation create error', err);
        toast('Unable to start chat. Please try again.', 'error');
    }
}

function initConversations() {
    if(!requireAuth()) return;
    if(conversationsUnsubscribe) conversationsUnsubscribe();
    const convRef = query(collection(db, 'conversations'), where('members', 'array-contains', currentUser.uid), orderBy('updatedAt', 'desc'));
    conversationsUnsubscribe = ListenerRegistry.register('messages:list', onSnapshot(convRef, function(snap) {
        conversationsCache = snap.docs.map(function(d) { return ({ id: d.id, ...d.data() }); });
        renderConversationList();
    }));
}

function renderConversationList() {
    const listEl = document.getElementById('conversation-list');
    if(!listEl) return;
    listEl.innerHTML = '';
    if(conversationsCache.length === 0) {
        listEl.innerHTML = '<div class="empty-state">No conversations yet.</div>';
        return;
    }
    conversationsCache.forEach(function(convo) {
        const partnerId = convo.members.find(function(m) { return m !== currentUser.uid; }) || currentUser.uid;
        const display = userCache[partnerId]?.username || 'user';
        const partnerProfile = userCache[partnerId] || { username: display, name: display, avatarColor: computeAvatarColor(partnerId || display) };
        const avatarHtml = renderAvatar({ ...partnerProfile, uid: partnerId }, { size: 36 });
        const item = document.createElement('div');
        item.className = 'conversation-item' + (activeConversationId === convo.id ? ' active' : '');
        item.innerHTML = `<div style="display:flex; align-items:center; gap:10px;">
            ${avatarHtml}
            <div>
                <strong>@${display}</strong>
                <div style="color:var(--text-muted); font-size:0.8rem;">${convo.lastMessageText || 'Tap to start'}</div>
            </div>
        </div>
        <span style="color:var(--text-muted); font-size:0.75rem;">${convo.requestState?.[currentUser.uid] === 'requested' ? '<span class="badge">Requested</span>' : ''}</span>`;
        item.onclick = function() { return setActiveConversation(convo.id, convo); };
        listEl.appendChild(item);
    });
}

function setActiveConversation(convoId, convoData = null) {
    activeConversationId = convoId;
    const header = document.getElementById('message-header');
    const partnerId = (convoData || conversationsCache.find(function(c) { return c.id === convoId; }) || {}).members?.find(function(m) { return m !== currentUser.uid; });
    if(header) header.textContent = partnerId ? `Chat with @${userCache[partnerId]?.username || 'user'}` : 'Conversation';
    listenToMessages(convoId);
}

function listenToMessages(convoId) {
    if(messagesUnsubscribe) messagesUnsubscribe();
    const msgRef = query(collection(db, 'conversations', convoId, 'messages'), orderBy('createdAt'));
    messagesUnsubscribe = ListenerRegistry.register(`messages:thread:${convoId}`, onSnapshot(msgRef, function(snap) {
        const msgs = snap.docs.map(function(d) { return ({ id: d.id, ...d.data() }); });
        renderMessages(msgs);
    }));
}

function renderMessages(msgs = []) {
    const body = document.getElementById('message-thread');
    if(!body) return;
    body.innerHTML = '';
    msgs.forEach(function(msg) {
        const bubble = document.createElement('div');
        bubble.className = 'message-bubble ' + (msg.senderId === currentUser.uid ? 'self' : 'other');
        bubble.innerHTML = msg.type === 'image' ? `<img src="${msg.mediaURL}" style="max-width:240px; border-radius:12px;">` : escapeHtml(msg.text || '');
        body.appendChild(bubble);
    });
    body.scrollTop = body.scrollHeight;
}

window.sendMessage = async function() {
    if(!activeConversationId || !requireAuth()) return;
    const input = document.getElementById('message-input');
    const fileInput = document.getElementById('message-media');
    const text = (input?.value || '').trim();
    if(!text && !fileInput?.files?.length) return;

    const msgRef = collection(db, 'conversations', activeConversationId, 'messages');
    let mediaURL = null;

    if(fileInput && fileInput.files && fileInput.files[0]) {
        const file = fileInput.files[0];
        const storageRef = ref(storage, `dm_media/${activeConversationId}/${Date.now()}_${file.name}`);
        await uploadBytes(storageRef, file);
        mediaURL = await getDownloadURL(storageRef);
    }

    await addDoc(msgRef, {
        senderId: currentUser.uid,
        type: mediaURL ? 'image' : 'text',
        text: mediaURL ? '' : text,
        mediaURL: mediaURL || '',
        createdAt: serverTimestamp()
    });

    // updateDoc does not accept { merge: true }; use a nested field update for requestState
    await updateDoc(doc(db, 'conversations', activeConversationId), {
        lastMessageText: mediaURL ? '📷 Photo' : text,
        lastMessageAt: serverTimestamp(),
        updatedAt: serverTimestamp(),
        [`requestState.${currentUser.uid}`]: 'inbox'
    });

    if(input) input.value = '';
    if(fileInput) fileInput.value = '';
};

// --- Videos ---
window.openVideoUploadModal = function() { return window.toggleVideoUploadModal(true); };
window.toggleVideoUploadModal = function(show = true) {
    const modal = document.getElementById('video-upload-modal');
    if(modal) modal.style.display = show ? 'flex' : 'none';
};

function ensureVideoObserver() {
    if(videoObserver) return videoObserver;
    videoObserver = new IntersectionObserver(function(entries) {
        entries.forEach(function(entry) {
            const vid = entry.target;
            if(entry.isIntersecting) {
                vid.play().catch(function() {});
                const vidId = vid.dataset.videoId;
                if(vidId && !viewedVideos.has(vidId)) {
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
    document.querySelectorAll('#video-feed video').forEach(function(v) {
        v.pause();
        if(videoObserver) videoObserver.unobserve(v);
    });
}

function initVideoFeed() {
    if(videosUnsubscribe) return; // already live
    const refVideos = query(collection(db, 'videos'), orderBy('createdAt', 'desc'));
    videosUnsubscribe = ListenerRegistry.register('videos:feed', onSnapshot(refVideos, function(snap) {
        videosCache = snap.docs.map(function(d) { return ({ id: d.id, ...d.data() }); });
        renderVideoFeed(videosCache);
    }));
}

function renderVideoFeed(videos = []) {
    const feed = document.getElementById('video-feed');
    if(!feed) return;
    pauseAllVideos();
    feed.innerHTML = '';
    if(videos.length === 0) { feed.innerHTML = '<div class="empty-state">No videos yet.</div>'; return; }

    const observer = ensureVideoObserver();

    videos.forEach(function(video) {
        const card = document.createElement('div');
        card.className = 'video-card';
        const tags = (video.hashtags || []).map(function(t) { return '#' + t; }).join(' ');

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
        likeBtn.onclick = function() { return window.likeVideo(video.id); };

        const saveBtn = document.createElement('button');
        saveBtn.className = 'icon-pill';
        saveBtn.innerHTML = '<i class="ph ph-bookmark"></i>';
        saveBtn.onclick = function() { return window.saveVideo(video.id); };

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

window.uploadVideo = async function() {
    if (!requireAuth()) return;

    const fileInput = document.getElementById('video-file');
    if (!fileInput || !fileInput.files || !fileInput.files[0]) return;

    const caption = document.getElementById('video-caption').value || '';
    const hashtags = (document.getElementById('video-tags').value || '')
        .split(',')
        .map(function(tag) { return tag.replace('#', '').trim(); })
        .filter(Boolean);
    const visibility = document.getElementById('video-visibility').value || 'public';
    const file = fileInput.files[0];
    const videoId = `${Date.now()}`;
    const storageRef = ref(storage, `videos/${currentUser.uid}/${videoId}/source.mp4`);

    try {
        const uploadTask = uploadBytesResumable(storageRef, file);
        await new Promise(function(resolve, reject) {
            uploadTask.on('state_changed', function() {}, reject, resolve);
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
        renderVideoFeed(videosCache);
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

window.likeVideo = async function(videoId) {
    if(!requireAuth()) return;
    const likeRef = doc(db, 'videos', videoId, 'likes', currentUser.uid);
    await setDoc(likeRef, { createdAt: serverTimestamp() });
    await updateDoc(doc(db, 'videos', videoId), { 'stats.likes': increment(1) });
};

window.saveVideo = async function(videoId) {
    if(!requireAuth()) return;
    await setDoc(doc(db, 'videos', videoId, 'saves', currentUser.uid), { createdAt: serverTimestamp() });
    await updateDoc(doc(db, 'videos', videoId), { 'stats.saves': increment(1) });
};

async function incrementVideoViews(videoId) {
    try {
        await updateDoc(doc(db, 'videos', videoId), { 'stats.views': increment(1) });
    } catch(e) { console.warn('view inc', e.message); }
}

// --- Live Sessions ---
window.toggleGoLiveModal = function(show = true) { const modal = document.getElementById('go-live-modal'); if(modal) modal.style.display = show ? 'flex' : 'none'; };

function renderLiveSessions() {
    if(liveSessionsUnsubscribe) return;
    const liveRef = query(collection(db, 'liveSessions'), where('status', '==', 'live'), orderBy('createdAt', 'desc'));
    liveSessionsUnsubscribe = ListenerRegistry.register('live:sessions', onSnapshot(liveRef, function(snap) {
        const sessions = snap.docs.map(function(d) { return ({ id: d.id, ...d.data() }); });
        const container = document.getElementById('live-grid-container');
        if(!container) return;
        container.innerHTML = '';
        if(sessions.length === 0) { container.innerHTML = '<div class="empty-state">No live sessions.</div>'; return; }
        sessions.forEach(function(s) {
            const card = document.createElement('div');
            card.className = 'live-card';
            card.innerHTML = `<div class="live-card-title">${escapeHtml(s.title || 'Live Session')}</div><div class="live-card-meta"><span>${escapeHtml(s.category || '')}</span><span>${(s.tags||[]).join(', ')}</span></div><div style="margin-top:10px;"><button class="icon-pill" onclick="window.openLiveSession('${s.id}')"><i class="ph ph-play"></i> Watch</button></div>`;
            container.appendChild(card);
        });
    }));
}

window.createLiveSession = async function() {
    if(!requireAuth()) return;
    const title = document.getElementById('live-title').value;
    const category = document.getElementById('live-category').value;
    const tags = (document.getElementById('live-tags').value || '').split(',').map(function(t) { return t.trim(); }).filter(Boolean);
    const streamEmbedURL = document.getElementById('live-url').value;
    await addDoc(collection(db, 'liveSessions'), {
        hostId: currentUser.uid,
        title,
        category,
        tags,
        status: 'live',
        streamEmbedURL,
        createdAt: serverTimestamp()
    });
    toggleGoLiveModal(false);
};

window.openLiveSession = function(sessionId) {
    if (activeLiveSessionId && activeLiveSessionId !== sessionId) {
        ListenerRegistry.unregister(`live:chat:${activeLiveSessionId}`);
    }
    activeLiveSessionId = sessionId;
    const container = document.getElementById('live-grid-container');
    if(!container) return;
    const sessionCard = document.createElement('div');
    sessionCard.className = 'social-card';
    sessionCard.innerHTML = `<div style="padding:1rem;"><div id="live-player" style="margin-bottom:10px;"></div><div id="live-chat" style="max-height:200px; overflow:auto;"></div><div style="display:flex; gap:8px; margin-top:8px;"><input id="live-chat-input" class="form-input" placeholder="Chat"/><button class="create-btn-sidebar" style="width:auto;" onclick="window.sendLiveChat('${sessionId}')">Send</button></div></div>`;
    container.prepend(sessionCard);
    listenLiveChat(sessionId);
};

function listenLiveChat(sessionId) {
    const chatRef = query(collection(db, 'liveSessions', sessionId, 'chat'), orderBy('createdAt'));
    ListenerRegistry.register(`live:chat:${sessionId}`, onSnapshot(chatRef, function(snap) {
        const chatEl = document.getElementById('live-chat');
        if(!chatEl) return;
        chatEl.innerHTML = '';
        snap.docs.forEach(function(docSnap) {
            const data = docSnap.data();
            const row = document.createElement('div');
            row.textContent = `${userCache[data.senderId]?.username || 'user'}: ${data.text}`;
            chatEl.appendChild(row);
        });
    }));
}

window.sendLiveChat = async function(sessionId) {
    if(!requireAuth()) return;
    const input = document.getElementById('live-chat-input');
    if(!input || !input.value.trim()) return;
    await addDoc(collection(db, 'liveSessions', sessionId, 'chat'), { senderId: currentUser.uid, text: input.value, createdAt: serverTimestamp() });
    input.value = '';
};

// --- Staff Console ---
function renderStaffConsole() {
    const warning = document.getElementById('staff-access-warning');
    const panels = document.getElementById('staff-panels');
    const isStaff = hasGlobalRole('staff') || hasGlobalRole('admin') || hasFounderClaimClient();
    if(!isStaff) {
        if(warning) warning.style.display = 'block';
        if(panels) panels.style.display = 'none';
        return;
    }
    if(warning) warning.style.display = 'none';
    if(panels) panels.style.display = 'block';
    listenVerificationRequests();
    listenReports();
    listenAdminLogs();
}

function listenVerificationRequests() {
    if(staffRequestsUnsub) return;
    staffRequestsUnsub = ListenerRegistry.register('staff:verificationRequests', onSnapshot(collection(db, 'verificationRequests'), function(snap) {
        const container = document.getElementById('verification-requests');
        if(!container) return;
        container.innerHTML = '';
        snap.docs.forEach(function(docSnap) {
            const data = docSnap.data();
            const card = document.createElement('div');
            card.className = 'social-card';
            card.innerHTML = `<div style="padding:1rem;"><div style="font-weight:800;">${data.category}</div><div style="font-size:0.9rem; color:var(--text-muted);">${(data.evidenceLinks||[]).join('<br>')}</div><div style="margin-top:6px; display:flex; gap:8px;"><button class="icon-pill" onclick="window.approveVerification('${docSnap.id}', '${data.userId}')">Approve</button><button class="icon-pill" onclick="window.denyVerification('${docSnap.id}')">Deny</button></div></div>`;
            container.appendChild(card);
        });
    }));
}

window.approveVerification = async function(requestId, userId) {
    await updateDoc(doc(db, 'verificationRequests', requestId), { status: 'approved', reviewedAt: serverTimestamp() });
    if(userId) await setDoc(doc(db, 'users', userId), { accountRoles: arrayUnion('verified'), verified: true, updatedAt: serverTimestamp() }, { merge: true });
    await addDoc(collection(db, 'adminLogs'), { actorId: currentUser.uid, action: 'approveVerification', targetRef: requestId, createdAt: serverTimestamp() });
};

window.denyVerification = async function(requestId) {
    await updateDoc(doc(db, 'verificationRequests', requestId), { status: 'denied', reviewedAt: serverTimestamp() });
    await addDoc(collection(db, 'adminLogs'), { actorId: currentUser.uid, action: 'denyVerification', targetRef: requestId, createdAt: serverTimestamp() });
};

function listenReports() {
    if(staffReportsUnsub) return;
    staffReportsUnsub = ListenerRegistry.register('staff:reports', onSnapshot(collection(db, 'reports'), function(snap) {
        const container = document.getElementById('reports-queue');
        if(!container) return;
        container.innerHTML = '';
        snap.docs.forEach(function(docSnap) {
            const data = docSnap.data();
            const card = document.createElement('div');
            card.className = 'social-card';
            card.innerHTML = `<div style="padding:1rem;"><div style="font-weight:800;">${data.type || 'report'}</div><div style="color:var(--text-muted); font-size:0.9rem;">${data.reason || ''}</div></div>`;
            container.appendChild(card);
        });
    }));
}

function listenAdminLogs() {
    if(staffLogsUnsub) return;
    staffLogsUnsub = ListenerRegistry.register('staff:adminLogs', onSnapshot(collection(db, 'adminLogs'), function(snap) {
        const container = document.getElementById('admin-logs');
        if(!container) return;
        container.innerHTML = '';
        snap.docs.forEach(function(docSnap) {
            const data = docSnap.data();
            const row = document.createElement('div');
            row.textContent = `${data.actorId}: ${data.action}`;
            container.appendChild(row);
        });
    }));
}

// --- Verification Request ---
window.openVerificationRequest = function() { toggleVerificationModal(true); };
window.toggleVerificationModal = function(show = true) { const modal = document.getElementById('verification-modal'); if(modal) modal.style.display = show ? 'flex' : 'none'; };
window.submitVerificationRequest = async function() {
    if(!requireAuth()) return;
    const category = document.getElementById('verify-category').value;
    const links = (document.getElementById('verify-links').value || '').split('\n').map(function(l) { return l.trim(); }).filter(Boolean);
    const notes = document.getElementById('verify-notes').value;
    await addDoc(collection(db, 'verificationRequests'), { userId: currentUser.uid, category, evidenceLinks: links, notes, status: 'pending', createdAt: serverTimestamp() });
    toggleVerificationModal(false);
};

// --- Security Rules Snippet (reference) ---
// See firestore.rules for suggested rules ensuring users write their own content and staff-only access.

// Start App
initApp();
