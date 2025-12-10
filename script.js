import { initializeApp } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-app.js";
import { getAuth, signInWithEmailAndPassword, createUserWithEmailAndPassword, signInAnonymously, onAuthStateChanged, signOut, updateProfile } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-auth.js";
import { getFirestore, collection, addDoc, onSnapshot, query, orderBy, serverTimestamp, doc, setDoc, getDoc, updateDoc, deleteDoc, arrayUnion, arrayRemove, increment, where, getDocs, collectionGroup } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";
import { getStorage, ref, uploadBytes, getDownloadURL } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-storage.js";

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
let savedSearchTerm = ''; 
let savedFilter = 'All Saved'; 
let isInitialLoad = true; 

// Optimistic UI Sets
let followedCategories = new Set(['STEM', 'Coding']);
let followedUsers = new Set();

// Snapshot cache to diff changes for thread rendering
let postSnapshotCache = {};

const REVIEW_CLASSES = ['review-verified', 'review-citation', 'review-misleading'];

function getReviewDisplay(reviewValue) {
    if(reviewValue === 'verified') {
        return { label: 'Verified', className: 'review-verified' };
    }
    if(reviewValue === 'citation') {
        return { label: 'Needs Citations', className: 'review-citation' };
    }
    if(reviewValue === 'misleading') {
        return { label: 'Misleading/False', className: 'review-misleading' };
    }
    return { label: 'Review', className: '' };
}

function applyReviewButtonState(buttonEl, reviewValue) {
    if(!buttonEl) return;
    const { label, className } = getReviewDisplay(reviewValue);
    const iconSize = buttonEl.dataset.iconSize || '1.1rem';
    buttonEl.classList.remove(...REVIEW_CLASSES);
    if(className) buttonEl.classList.add(className);
    buttonEl.innerHTML = `<i class="ph ph-scales" style="font-size:${iconSize};"></i> ${label}`;
}

let userProfile = {
    name: "Nexara User",
    realName: "",
    nickname: "",
    username: "nexara_explorer",
    bio: "Stream, Socialize, and Strive.",
    links: "mysite.com",
    email: "",
    phone: "",
    gender: "Prefer not to say",
    region: "",
    photoURL: "",
    theme: "system",
    savedPosts: [],
    following: [],
    followersCount: 0
};

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
let liveSessionsUnsubscribe = null;
let staffRequestsUnsub = null;
let staffReportsUnsub = null;
let staffLogsUnsub = null;

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
    'For You': '#00f2ea',    'Following': '#ffffff',  'STEM': '#00f2ea',
    'History': '#ffd700',    'Coding': '#00ff41',     'Art': '#ff0050',
    'Random': '#bd00ff',     'Brainrot': '#ff00ff',   'Sports': '#ff4500',
    'Gaming': '#7000ff',     'News': '#ff3d3d',       'Music': '#00bfff'
};

// Shared state + render helpers
window.getCurrentUser = () => currentUser;
window.getUserDoc = async (uid) => getDoc(doc(db, 'users', uid));
window.requireAuth = () => {
    if(!currentUser) {
        document.getElementById('auth-screen').style.display = 'flex';
        document.getElementById('app-layout').style.display = 'none';
        return false;
    }
    return true;
};
window.setView = (name) => window.navigateTo(name);
window.toast = (msg, type = 'info') => {
    console.log(`[${type}]`, msg);
    const overlay = document.createElement('div');
    overlay.textContent = msg;
    overlay.className = 'toast-msg';
    document.body.appendChild(overlay);
    setTimeout(() => overlay.remove(), 2500);
};

// --- Initialization & Auth Listener ---
function initApp() {
    onAuthStateChanged(auth, async (user) => {
        const loadingOverlay = document.getElementById('loading-overlay');
        const authScreen = document.getElementById('auth-screen');
        const appLayout = document.getElementById('app-layout');

        if (user) {
            currentUser = user;
            console.log("User logged in:", user.uid);

            try {
                const ensuredSnap = await ensureUserDocument(user);
                const docSnap = ensuredSnap;
                // Fetch User Profile
                if (docSnap.exists()) {
                    userProfile = { ...userProfile, ...docSnap.data() };
                    userCache[user.uid] = userProfile;

                    // Apply stored theme preference
                    const savedTheme = userProfile.theme || getStoredThemePreference() || 'system';
                    userProfile.theme = savedTheme;
                    applyTheme(savedTheme);

                    // Restore 'following' state locally
                    if (userProfile.following) {
                        userProfile.following.forEach(uid => followedUsers.add(uid));
                    }
                    const staffNav = document.getElementById('nav-staff');
                    if(staffNav) staffNav.style.display = (userProfile.role === 'staff' || userProfile.role === 'admin') ? 'flex' : 'none';
                } else {
                    // Create new profile placeholder if it doesn't exist
                    userProfile.email = user.email || "";
                    userProfile.name = user.displayName || "Nexara User";
                    const storedTheme = getStoredThemePreference() || userProfile.theme || 'system';
                    userProfile.theme = storedTheme;
                    applyTheme(storedTheme);
                    const staffNav = document.getElementById('nav-staff');
                    if(staffNav) staffNav.style.display = 'none';
                }
            } catch (e) { 
                console.error("Profile Load Error", e); 
            }

            // UI Transitions
            if (authScreen) authScreen.style.display = 'none';
            if (appLayout) appLayout.style.display = 'flex';
            if (loadingOverlay) loadingOverlay.style.display = 'none';

            // Start Logic
            startDataListener();
            startUserReviewListener(user.uid); // PATCH: Listen for USER reviews globally on load
            updateTimeCapsule(); 
            window.navigateTo('feed', false); 
            renderProfile(); // Pre-render profile
        } else {
            currentUser = null;
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

    if(dateEl) dateEl.textContent = dateString;
    if(eventEl) eventEl.textContent = eventText;
}

function getSystemTheme() {
    return window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
}

function getStoredThemePreference() {
    try {
        return localStorage.getItem('nexara-theme');
    } catch(e) {
        return null;
    }
}

function applyTheme(preference = 'system') {
    const resolved = preference === 'system' ? getSystemTheme() : preference;
    document.body.classList.toggle('light-mode', resolved === 'light');
    document.body.dataset.themePreference = preference;
    try { localStorage.setItem('nexara-theme', preference); } catch(e) { console.warn('Theme storage blocked'); }
}

async function persistThemePreference(preference = 'system') {
    userProfile.theme = preference;
    applyTheme(preference);
    if(currentUser) {
        try {
            await setDoc(doc(db, "users", currentUser.uid), { theme: preference }, { merge: true });
        } catch(e) {
            console.warn('Theme save failed', e.message);
        }
    }
}

async function ensureUserDocument(user) {
    const ref = doc(db, "users", user.uid);
    const snap = await getDoc(ref);
    const now = serverTimestamp();
    if(!snap.exists()) {
        await setDoc(ref, {
            displayName: user.displayName || "Nexara User",
            username: user.email ? user.email.split('@')[0] : `user_${user.uid.slice(0,6)}`,
            photoURL: user.photoURL || "",
            bio: "",
            website: "",
            region: "",
            email: user.email || "",
            role: user.role || "user",
            createdAt: now,
            updatedAt: now
        }, { merge: true });
        return await getDoc(ref);
    }
    await setDoc(ref, { updatedAt: now }, { merge: true });
    return await getDoc(ref);
}

function shouldRerenderThread(newData, prevData = {}) {
    const fieldsToWatch = ['title', 'content', 'mediaUrl', 'type', 'category', 'trustScore'];
    return fieldsToWatch.some(key => newData[key] !== prevData[key]);
}

// --- Auth Functions ---
window.handleLogin = async (e) => {
    e.preventDefault();
    try {
        await signInWithEmailAndPassword(auth, document.getElementById('email').value, document.getElementById('password').value);
    } catch (err) { 
        document.getElementById('auth-error').textContent = err.message; 
    }
}

window.handleSignup = async (e) => {
    e.preventDefault();
    try {
        const cred = await createUserWithEmailAndPassword(auth, document.getElementById('email').value, document.getElementById('password').value);
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
            bio: "",
            website: "",
            region: "",
            role: "user"
        });
    } catch (err) {
        document.getElementById('auth-error').textContent = err.message;
    }
}

window.handleAnon = async () => { 
    try { 
        await signInAnonymously(auth); 
    } catch(e){ 
        console.error(e); 
    } 
}

window.handleLogout = () => { 
    signOut(auth); 
    location.reload(); 
}

// --- Data Fetching & Caching ---
async function fetchMissingProfiles(posts) {
    const missingIds = new Set();
    posts.forEach(post => { 
        if (post.userId && !userCache[post.userId]) {
            missingIds.add(post.userId); 
        }
    });

    if (missingIds.size === 0) return;

    // Fetch up to 10 at a time or simple Promise.all
    const fetchPromises = Array.from(missingIds).map(uid => getDoc(doc(db, "users", uid)));

    try {
        const userDocs = await Promise.all(fetchPromises);
        userDocs.forEach(docSnap => {
            if (docSnap.exists()) {
                userCache[docSnap.id] = docSnap.data();
            } else {
                userCache[docSnap.id] = { name: "Unknown User", username: "unknown" };
            }
        });

        // Re-render dependent views once data arrives
        renderFeed();
        if(activePostId) renderThreadMainPost(activePostId);
    } catch (e) { 
        console.error("Error fetching profiles:", e); 
    }
}

function startDataListener() {
    const postsRef = collection(db, 'posts');
    const q = query(postsRef);

    onSnapshot(q, (snapshot) => {
        const previousCache = { ...postSnapshotCache };
        const nextCache = {};
        allPosts = [];
        snapshot.forEach((doc) => {
            const data = doc.data();
            allPosts.push({ id: doc.id, ...data });
            nextCache[doc.id] = data;
        });

        // Sort posts by date (newest first)
        allPosts.sort((a, b) => (b.timestamp?.seconds || 0) - (a.timestamp?.seconds || 0));

        // Fetch profiles for these posts
        fetchMissingProfiles(allPosts);

        // Initial Render
        if(isInitialLoad) { 
            renderFeed(); 
            isInitialLoad = false; 
        }

        // Live updates for specific interactions
        snapshot.docChanges().forEach((change) => {
            if (change.type === "modified") {
                refreshSinglePostUI(change.doc.id);

                if(activePostId === change.doc.id && document.getElementById('view-thread').style.display === 'block') {
                    const prevData = previousCache[change.doc.id] || {};
                    const newData = change.doc.data();
                    if(shouldRerenderThread(newData, prevData)) {
                        renderThreadMainPost(activePostId);
                    }
                }
            }
        });

        postSnapshotCache = nextCache;
    });

    // Start Live Stream Listener (Mock)
    if(typeof renderLive === 'function') renderLive(); 
}

// PATCH: New listener to fetch user's reviews across all posts
function startUserReviewListener(uid) {
    const q = query(collectionGroup(db, 'reviews'), where('userId', '==', uid));
    onSnapshot(q, (snapshot) => {
        window.myReviewCache = {};
        snapshot.forEach((doc) => {
            // In a Collection Group query, we can access the parent Post ID
            const parentPostRef = doc.ref.parent.parent;
            if(parentPostRef) {
                window.myReviewCache[parentPostRef.id] = doc.data().rating;
            }
        });

        // Refresh UI for all loaded posts to apply colors
        allPosts.forEach(post => refreshSinglePostUI(post.id));
    }, (error) => {
        console.log("Review listener note:", error.message);
    });
}

// --- Navigation Logic ---
window.navigateTo = function(viewId, pushToStack = true) {
    // Cleanup previous listeners if leaving thread
    if(viewId !== 'thread' && threadUnsubscribe) { 
        threadUnsubscribe(); 
        threadUnsubscribe = null; 
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
    document.querySelectorAll('.view-section').forEach(el => el.style.display = 'none');
    const targetView = document.getElementById('view-' + viewId);
    if (targetView) targetView.style.display = 'block';

    // Toggle Navbar Active State
    if(viewId !== 'thread' && viewId !== 'public-profile') {
        document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
        const navEl = document.getElementById('nav-' + viewId);
        if(navEl) navEl.classList.add('active');
    }

    // View Specific Logic
    if(viewId === 'feed' && pushToStack) {
        currentCategory = 'For You';
        renderFeed();
    }
    if(viewId === 'saved') { renderSaved(); }
    if(viewId === 'profile') renderProfile();
    if(viewId === 'discover') { renderDiscover(); }
    if(viewId === 'messages') { initConversations(); }
    if(viewId === 'videos') { initVideoFeed(); }
    if(viewId === 'live') { renderLiveSessions(); }
    if(viewId === 'staff') { renderStaffConsole(); }

    currentViewId = viewId;
    window.scrollTo(0,0);
}

window.goBack = function() {
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
}

// --- Follow Logic (Optimistic UI) ---
window.toggleFollow = function(c, event) {
    if(event) event.stopPropagation();

    const isFollowing = followedCategories.has(c);
    if(isFollowing) followedCategories.delete(c); 
    else followedCategories.add(c);

    // Sanitize class name to match HTML
    const cleanTopic = c.replace(/[^a-zA-Z0-9]/g, ''); 
    const btns = document.querySelectorAll(`.js-follow-topic-${cleanTopic}`);

    btns.forEach(btn => {
        if (isFollowing) { 
            btn.innerHTML = '<i class="ph-bold ph-plus"></i> Topic';
            btn.classList.remove('following');
        } else { 
            btn.innerHTML = 'Following';
            btn.classList.add('following');
        }
    });
}

window.toggleFollowUser = async function(uid, event) {
    if(event) event.stopPropagation();

    const isFollowing = followedUsers.has(uid);
    if(isFollowing) followedUsers.delete(uid); 
    else followedUsers.add(uid);

    // 1. Update Buttons immediately
    const btns = document.querySelectorAll(`.js-follow-user-${uid}`);
    btns.forEach(btn => {
        if (isFollowing) { 
            // We were following, now we are NOT (Unfollow action completed)
            // State: Not Following -> BUTTON SHOULD BE FILLED (Option to follow)
            btn.innerHTML = '<i class="ph-bold ph-plus"></i> User';
            btn.classList.remove('following');

            // Generic Button: Filled-ish look
            btn.style.background = 'rgba(255,255,255,0.1)'; 
            btn.style.borderColor = 'transparent'; 
            btn.style.color = 'var(--text-main)';

             if(btn.classList.contains('create-btn-sidebar')) {
                 btn.textContent = "Follow";
                 // Profile Button: Filled Primary
                 btn.style.background = "var(--primary)";
                 btn.style.color = "black";
                 btn.style.borderColor = "var(--primary)";
            }
        } else { 
            // We were NOT following, now we ARE (Follow action completed)
            // State: Following -> BUTTON SHOULD BE OUTLINED
            btn.innerHTML = 'Following';
            btn.classList.add('following');

            // Generic Button: Transparent/Outlined
            btn.style.background = 'transparent'; 
            btn.style.borderColor = 'var(--border)'; 
            btn.style.color = 'var(--text-muted)'; // or var(--text-main) if preferred

            if(btn.classList.contains('create-btn-sidebar')) {
                 btn.textContent = "Following";
                 // Profile Button: Outlined Primary
                 btn.style.background = "transparent";
                 btn.style.color = "var(--primary)";
                 btn.style.borderColor = "var(--primary)";
            }
        }
    });

    // 2. Real-time Follower Count Update
    const countEl = document.getElementById(`profile-follower-count-${uid}`);
    let newCount = 0;

    // Update Cache immediately so navigation preserves state
    if (userCache[uid]) {
        let currentCount = userCache[uid].followersCount || 0;
        newCount = isFollowing ? Math.max(0, currentCount - 1) : currentCount + 1;
        userCache[uid].followersCount = newCount;
    }

    // Update DOM if present
    if(countEl) {
        countEl.textContent = newCount;
    }

    // 3. Backend Update
    try {
        if(isFollowing) {
            await updateDoc(doc(db, 'users', uid), { followersCount: increment(-1) });
            await updateDoc(doc(db, 'users', currentUser.uid), { following: arrayRemove(uid) });
        } else {
            await updateDoc(doc(db, 'users', uid), { followersCount: increment(1) });
            await updateDoc(doc(db, 'users', currentUser.uid), { following: arrayUnion(uid) });
        }
    } catch(e) { console.error(e); }
}

// --- Render Logic (The Core) ---
function getPostHTML(post) {
    try {
        const date = post.timestamp && post.timestamp.seconds 
            ? new Date(post.timestamp.seconds * 1000).toLocaleDateString() 
            : 'Just now';

        let authorData = userCache[post.userId] || { name: post.author, username: "loading...", photoURL: null };
        if (!authorData.name) authorData.name = "Unknown User"; 

        const avatarStyle = authorData.photoURL 
            ? `background-image: url('${authorData.photoURL}'); background-size: cover; color: transparent;` 
            : `background: ${getColorForUser(authorData.name)}`;

        const isLiked = post.likedBy && post.likedBy.includes(currentUser.uid);
        // FIX: Check 'Saved' state immediately for initial render
        const isSaved = userProfile.savedPosts && userProfile.savedPosts.includes(post.id);
        const isFollowingUser = followedUsers.has(post.userId);
        const isFollowingTopic = followedCategories.has(post.category);
        const topicClass = post.category.replace(/[^a-zA-Z0-9]/g, '');

        // UPDATE: Trust Badge Logic - "Publicly Verified" & Gray Styling
        let trustBadge = "";
        if(post.trustScore > 2) {
            // Updated style: Gray color (#8b949e), no border/background, consistent text
            trustBadge = `<div style="font-size:0.75rem; color:#8b949e; display:flex; align-items:center; gap:7px; font-weight:600;"><i class="ph-fill ph-check-circle; padding-right: 35px; padding-top: 15px;"></i> Publicly Verified</div>`;
        } else if(post.trustScore < -1) {
            trustBadge = `<div style="font-size:0.75rem; color:#ff3d3d; display:flex; align-items:center; gap:4px; font-weight:600;"><i class="ph-fill ph-warning-circle"></i> Disputed</div>`;
        }

        // Media Logic
        let mediaContent = '';
        if (post.mediaUrl) {
            if (post.type === 'video') {
                mediaContent = `<div class="video-container" onclick="window.openFullscreenMedia('${post.mediaUrl}', 'video')"><video src="${post.mediaUrl}" controls class="post-media"></video></div>`;
            } else {
                mediaContent = `<img src="${post.mediaUrl}" class="post-media" alt="Post Content" onclick="window.openFullscreenMedia('${post.mediaUrl}', 'image')">`;
            }
        }

        // Preview Comment Logic
        let commentPreviewHtml = '';
        if(post.previewComment) {
            commentPreviewHtml = `
                <div style="margin-top:10px; padding:8px; background:rgba(255,255,255,0.05); border-radius:8px; font-size:0.85rem; color:var(--text-muted); display:flex; gap:6px;">
                    <span style="font-weight:bold; color:var(--text-main);">${escapeHtml(post.previewComment.author)}:</span> 
                    <span>${escapeHtml(post.previewComment.text)}</span>
                    ${post.previewComment.likes ? `<span style="margin-left:auto; font-size:0.75rem; display:flex; align-items:center; gap:3px;"><i class="ph-fill ph-thumbs-up"></i> ${post.previewComment.likes}</span>` : ''}
                </div>`;
        }

        // Saved Tag Logic
        let savedTagHtml = "";
        if (currentCategory === 'Saved') {
            const tag = (userProfile.savedTags && userProfile.savedTags[post.id]) || "";
            savedTagHtml = `<div style="margin-top:5px;"><button onclick="event.stopPropagation(); window.addTagToSaved('${post.id}')" style="background:var(--bg-hover); border:1px dashed var(--border); font-size:0.7rem; padding:2px 8px; border-radius:4px; color:var(--text-muted); cursor:pointer; display:flex; align-items:center; gap:4px;">${tag ? '<i class="ph-fill ph-tag"></i> ' + escapeHtml(tag) : '<i class="ph ph-plus"></i> Add Tag'}</button></div>`;
        }

        // Review Button Color Logic (Read from global cache)
        const myReview = window.myReviewCache ? window.myReviewCache[post.id] : null;
        const reviewDisplay = getReviewDisplay(myReview);

        // UPDATED HTML STRUCTURE: Verified Badge moved to right side under buttons
        return `
            <div id="post-card-${post.id}" class="social-card fade-in" style="border-left: 2px solid ${THEMES['For You']};">
                <div class="card-header">
                    <div class="author-wrapper" onclick="window.openUserProfile('${post.userId}', event)">
                        <div class="user-avatar" style="${avatarStyle}">${authorData.photoURL ? '' : authorData.name[0]}</div>
                        <div class="header-info"><span class="author-name">${escapeHtml(authorData.name)}</span><span class="post-meta">@${escapeHtml(authorData.username)} • ${date}</span></div>
                    </div>
                    <div style="flex:1; display:flex; flex-direction:column; align-items:flex-end; gap:2px;">
                        <div style="display:flex; gap:5px;">
                            <button class="follow-btn js-follow-user-${post.userId} ${isFollowingUser ? 'following' : ''}" onclick="event.stopPropagation(); window.toggleFollowUser('${post.userId}', event)" style="font-size:0.65rem; padding:2px 8px;">${isFollowingUser ? 'Following' : '<i class="ph-bold ph-plus"></i> User'}</button>
                            <button class="follow-btn js-follow-topic-${topicClass} ${isFollowingTopic ? 'following' : ''}" onclick="event.stopPropagation(); window.toggleFollow('${post.category}', event)" style="font-size:0.65rem; padding:2px 8px;">${isFollowingTopic ? 'Following' : '<i class="ph-bold ph-plus"></i> Topic'}</button>
                        </div>
                        ${trustBadge}
                    </div>
                </div>
                <div class="card-content" onclick="window.openThread('${post.id}')">
                    <div class="category-badge">${post.category}</div>
                    <h3 class="post-title">${escapeHtml(cleanText(post.title))}</h3>
                    <p>${escapeHtml(cleanText(post.content))}</p>
                    ${mediaContent} 
                    ${commentPreviewHtml} 
                    ${savedTagHtml}
                </div>
                <div class="card-actions">
                    <button class="action-btn" onclick="window.toggleLike('${post.id}', event)" style="color: ${isLiked ? '#00f2ea' : 'inherit'}"><i class="${isLiked ? 'ph-fill' : 'ph'} ph-thumbs-up" style="font-size:1.1rem;"></i> ${post.likes || 0}</button>
                    <button class="action-btn" onclick="window.openThread('${post.id}')"><i class="ph ph-chat-circle" style="font-size:1.1rem;"></i> Discuss</button>
                    <button class="action-btn" onclick="window.toggleSave('${post.id}', event)" style="color: ${isSaved ? '#00f2ea' : 'inherit'}"><i class="${isSaved ? 'ph-fill' : 'ph'} ph-bookmark-simple" style="font-size:1.1rem;"></i> ${isSaved ? 'Saved' : 'Save'}</button>
                    <button class="action-btn review-action ${reviewDisplay.className}" data-icon-size="1.1rem" onclick="event.stopPropagation(); window.openPeerReview('${post.id}')"><i class="ph ph-scales" style="font-size:1.1rem;"></i> ${reviewDisplay.label}</button>
                </div>
            </div>`;
    } catch(e) { 
        console.error("Error generating post HTML", e); 
        return ""; 
    }
}

function renderFeed(targetId = 'feed-content') {
    const container = document.getElementById(targetId);
    if (!container) return;

    container.innerHTML = ""; 
    let displayPosts = allPosts;

    // Filter Logic
    if (currentCategory === 'Following') {
        displayPosts = allPosts.filter(post => followedCategories.has(post.category));
    } else if (currentCategory === 'Saved') {
         displayPosts = allPosts.filter(post => userProfile.savedPosts && userProfile.savedPosts.includes(post.id));
         // Sub-filtering for Saved view
         if (savedSearchTerm) displayPosts = displayPosts.filter(post => post.title.toLowerCase().includes(savedSearchTerm));
         if (savedFilter === 'Recent') displayPosts.sort((a, b) => userProfile.savedPosts.indexOf(b.id) - userProfile.savedPosts.indexOf(a.id));
         else if (savedFilter === 'Oldest') displayPosts.sort((a, b) => userProfile.savedPosts.indexOf(a.id) - userProfile.savedPosts.indexOf(b.id));
         else if (savedFilter === 'Videos') displayPosts = displayPosts.filter(p => p.type === 'video'); 
         else if (savedFilter === 'Images') displayPosts = displayPosts.filter(p => p.type === 'image');
    } else if (currentCategory !== 'For You') {
        displayPosts = allPosts.filter(post => post.category === currentCategory);
    }

    if (displayPosts.length === 0) { 
        container.innerHTML = `<div class="empty-state"><i class="ph ph-magnifying-glass" style="font-size:3rem; margin-bottom:1rem;"></i><p>No posts found.</p></div>`; 
        return; 
    }

    // Render loop
    displayPosts.forEach(post => {
        container.innerHTML += getPostHTML(post);
    });

    // Apply review state to freshly rendered posts using cached data
    displayPosts.forEach(post => {
        const reviewBtn = document.querySelector(`#post-card-${post.id} .review-action`);
        const reviewValue = window.myReviewCache ? window.myReviewCache[post.id] : null;
        applyReviewButtonState(reviewBtn, reviewValue);
    });

    // PATCH: Apply colors immediately after rendering
    // This catches cases where review data loaded BEFORE the feed rendered
    setTimeout(() => {
        // window.applyReviewColors(); // Removed as it was undefined in provided code, relying on getPostHTML logic
    }, 50);
}

function refreshSinglePostUI(postId) {
    const post = allPosts.find(p => p.id === postId);
    if (!post) return;

    const likeBtn = document.querySelector(`#post-card-${postId} .action-btn:nth-child(1)`);
    const saveBtn = document.querySelector(`#post-card-${postId} .action-btn:nth-child(3)`);
    const reviewBtn = document.querySelector(`#post-card-${postId} .review-action`);

    const isLiked = post.likedBy && post.likedBy.includes(currentUser.uid);
    const isSaved = userProfile.savedPosts && userProfile.savedPosts.includes(postId);
    const myReview = window.myReviewCache ? window.myReviewCache[postId] : null;

    if(likeBtn) { 
        likeBtn.innerHTML = `<i class="${isLiked ? 'ph-fill' : 'ph'} ph-thumbs-up" style="font-size:1.1rem;"></i> ${post.likes || 0}`; 
        likeBtn.style.color = isLiked ? '#00f2ea' : 'inherit'; 
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
    const threadSaveBtn = document.getElementById('thread-save-btn');
    const threadTitle = document.getElementById('thread-view-title');
    const threadReviewBtn = document.getElementById('thread-review-btn');

    if(threadTitle && threadTitle.dataset.postId === postId) {
        if(threadLikeBtn) { 
            threadLikeBtn.innerHTML = `<i class="${isLiked ? 'ph-fill' : 'ph'} ph-thumbs-up"></i> <span style="font-size:1rem; margin-left:5px;">${post.likes || 0}</span>`; 
            threadLikeBtn.style.color = isLiked ? '#00f2ea' : 'inherit'; 
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

    const post = allPosts.find(p => p.id === postId);
    if(!post) return;

    const wasLiked = post.likedBy && post.likedBy.includes(currentUser.uid);

    // Optimistic Update
    if (wasLiked) { 
        post.likes = (post.likes || 0) - 1; 
        post.likedBy = post.likedBy.filter(uid => uid !== currentUser.uid); 
    } else { 
        post.likes = (post.likes || 0) + 1; 
        if (!post.likedBy) post.likedBy = []; 
        post.likedBy.push(currentUser.uid); 
    }

    refreshSinglePostUI(postId);
    const postRef = doc(db, 'posts', postId);

    try {
        if(wasLiked) {
            await updateDoc(postRef, { likes: increment(-1), likedBy: arrayRemove(currentUser.uid) });
        } else {
            await updateDoc(postRef, { likes: increment(1), likedBy: arrayUnion(currentUser.uid) });
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
        userProfile.savedPosts = userProfile.savedPosts.filter(id => id !== postId);
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

window.createPost = async function() { 
     const title = document.getElementById('postTitle').value;
     const content = document.getElementById('postContent').value;
     const category = document.getElementById('postCategory').value;
     const fileInput = document.getElementById('postFile');
     const btn = document.getElementById('publishBtn');

     let type = 'text';
     if (fileInput.files[0]) {
         const mime = fileInput.files[0].type;
         if (mime.startsWith('video')) type = 'video'; 
         else if (mime.startsWith('image')) type = 'image';
     }

     if(!title.trim() && !content.trim() && !fileInput.files[0]) {
         return alert("Please add a title, content, or media.");
     }

     btn.disabled = true; 
     btn.textContent = "Uploading...";

     try {
         let mediaUrl = null;
         if(fileInput.files[0]) {
             const path = `posts/${currentUser.uid}/${Date.now()}_${fileInput.files[0].name}`;
             mediaUrl = await uploadFileToStorage(fileInput.files[0], path);
         }

         await addDoc(collection(db, 'posts'), { 
             title, 
             content, 
             category, 
             type, 
             mediaUrl, 
             author: userProfile.name, 
             userId: currentUser.uid, 
             likes: 0, 
             likedBy: [], 
             trustScore: 0, 
             timestamp: serverTimestamp() 
         });

         // Reset Form
         document.getElementById('postTitle').value = ""; 
         document.getElementById('postContent').value = ""; 
         fileInput.value = ""; 
         window.clearPostImage(); 
         window.toggleCreateModal(false); 
         window.navigateTo('feed');

     } catch (e) { 
         console.error(e); 
         alert("Post failed: " + e.message); 
     } finally { 
         btn.disabled = false; 
         btn.textContent = "Post"; 
     }
}

// --- Settings & Modals ---
function updateSettingsAvatarPreview(src) {
    const preview = document.getElementById('settings-avatar-preview');
    if(!preview) return;

    if(src) {
        preview.style.backgroundImage = `url('${src}')`;
        preview.style.backgroundSize = 'cover';
        preview.textContent = '';
    } else {
        preview.style.backgroundImage = 'none';
        preview.style.backgroundColor = getColorForUser(userProfile.name || 'U');
        preview.textContent = (userProfile.name || 'U')[0];
    }
}

function syncThemeRadios(themeValue) {
    const selected = document.querySelector(`input[name="theme-choice"][value="${themeValue}"]`);
    if(selected) selected.checked = true;
}

window.toggleCreateModal = (show) => {
    document.getElementById('create-modal').style.display = show ? 'flex' : 'none';
    if(show && currentUser) {
        const avatarEl = document.getElementById('modal-user-avatar');
        if(userProfile.photoURL) {
            avatarEl.style.backgroundImage = `url('${userProfile.photoURL}')`; 
            avatarEl.textContent = ''; 
        } else { 
            avatarEl.style.backgroundImage = 'none'; 
            avatarEl.style.backgroundColor = getColorForUser(userProfile.name); 
            avatarEl.textContent = userProfile.name[0]; 
            avatarEl.style.color = 'black'; 
        } 
    } 
}

window.toggleSettingsModal = (show) => {
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
            photoUrlInput.oninput = (e) => updateSettingsAvatarPreview(e.target.value);
        }
        syncThemeRadios(userProfile.theme || 'system');
        updateSettingsAvatarPreview(userProfile.photoURL);

        const uploadInput = document.getElementById('set-pic-file');
        const cameraInput = document.getElementById('set-pic-camera');
        if(uploadInput) uploadInput.onchange = (e) => handleSettingsFileChange(e.target);
        if(cameraInput) cameraInput.onchange = (e) => handleSettingsFileChange(e.target);

        document.querySelectorAll('input[name="theme-choice"]').forEach(r => {
            r.onchange = (e) => persistThemePreference(e.target.value);
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
    reader.onload = (e) => updateSettingsAvatarPreview(e.target.result);
    reader.readAsDataURL(inputEl.files[0]);
}

// --- Peer Review System ---
window.openPeerReview = function(postId) { 
    activePostId = postId; 
    document.getElementById('review-modal').style.display = 'flex'; 
    document.getElementById('review-stats-text').textContent = "Loading data...";

    const reviewsRef = collection(db, 'posts', postId, 'reviews'); 
    const q = query(reviewsRef); 

    onSnapshot(q, (snapshot) => {
        const container = document.getElementById('review-list'); 
        container.innerHTML = "";

        let scores = { verified: 0, citation: 0, misleading: 0, total: 0 };
        let userHasReview = false; 
        let myRatingData = null;

        snapshot.forEach(doc => {
            const data = doc.data();
            if(data.userId === currentUser.uid) { 
                userHasReview = true; 
                window.currentReviewId = doc.id; 
                myRatingData = data; 

                // Cache my review to update the feed button color
                window.myReviewCache[activePostId] = data.rating;
                refreshSinglePostUI(activePostId);
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
    });
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

    const commentsRef = collection(db, 'posts', postId, 'comments'); 
    const q = query(commentsRef); 

    if (threadUnsubscribe) threadUnsubscribe();

    threadUnsubscribe = onSnapshot(q, (snapshot) => {
        const container = document.getElementById('thread-stream'); 
        container.innerHTML = "";

        const comments = []; 
        snapshot.forEach(d => comments.push({id: d.id, ...d.data()}));
        comments.sort((a,b) => (a.timestamp?.seconds||0) - (b.timestamp?.seconds||0));

        const missingCommentUsers = comments.filter(c => !userCache[c.userId]).map(c => ({userId: c.userId}));
        if(missingCommentUsers.length > 0) fetchMissingProfiles(missingCommentUsers);

        if (comments.length === 0) {
            container.innerHTML = `<div style="text-align:center; padding:2rem; color:var(--text-muted);">No comments yet. Be the first to reply!</div>`;
        }

        comments.forEach(c => {
            const cAuthor = userCache[c.userId] || { name: "User", photoURL: null };
            const isReply = c.parentId ? 'margin-left: 40px; border-left: 2px solid var(--border);' : '';
            const isLiked = c.likedBy && c.likedBy.includes(currentUser.uid);

            let mediaHtml = c.mediaUrl 
                ? `<div onclick="window.openFullscreenMedia('${c.mediaUrl}', 'image')"><img src="${c.mediaUrl}" style="max-width:200px; border-radius:8px; margin-top:5px; cursor:pointer;"></div>` 
                : "";

            container.innerHTML += `
                <div id="comment-${c.id}" style="margin-bottom: 15px; padding: 10px; border-bottom: 1px solid var(--border); ${isReply}">
                    <div style="display:flex; gap:10px; align-items:flex-start;">
                        <div class="user-avatar" style="width:36px; height:36px; font-size:0.9rem; background-image:url('${cAuthor.photoURL||''}'); background-size:cover; background-color:#333;">${cAuthor.photoURL ? '' : cAuthor.name[0]}</div>
                        <div style="flex:1;">
                            <div style="font-size:0.9rem; margin-bottom:2px;"><strong>${escapeHtml(cAuthor.name)}</strong> <span style="color:var(--text-muted); font-size:0.8rem;">• ${c.timestamp ? new Date(c.timestamp.seconds*1000).toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'}) : 'Now'}</span></div>
                            <div style="margin-top:2px; font-size:0.95rem; line-height:1.4;">${escapeHtml(c.text)}</div>
                            ${mediaHtml}
                            <div style="margin-top:8px; display:flex; gap:15px; align-items:center;">
                                <button onclick="window.moveInputToComment('${c.id}', '${escapeHtml(cAuthor.name)}')" style="background:none; border:none; color:var(--text-muted); font-size:0.8rem; cursor:pointer; display:flex; align-items:center; gap:5px;"><i class="ph ph-arrow-bend-up-left"></i> Reply</button>
                                <button onclick="window.toggleCommentLike('${c.id}', event)" style="background:none; border:none; color:${isLiked ? '#00f2ea' : 'var(--text-muted)'}; font-size:0.8rem; cursor:pointer; display:flex; align-items:center; gap:5px;"><i class="${isLiked ? 'ph-fill' : 'ph'} ph-thumbs-up"></i> ${c.likes || 0}</button>
                            </div>
                            <div id="reply-slot-${c.id}"></div>
                        </div>
                    </div>
                </div>`;
        });

        if (activeReplyId) { 
            const slot = document.getElementById(`reply-slot-${activeReplyId}`); 
            const inputArea = document.getElementById('thread-input-area'); 
            if (slot && inputArea && !slot.contains(inputArea)) { 
                slot.appendChild(inputArea); 
                document.getElementById('thread-input').focus(); 
            } 
        }
    });
}

function renderThreadMainPost(postId) {
    const container = document.getElementById('thread-main-post');
    const post = allPosts.find(p => p.id === postId);
    if(!post) return;

    const isLiked = post.likedBy && post.likedBy.includes(currentUser.uid);
    const isSaved = userProfile.savedPosts && userProfile.savedPosts.includes(postId);
    const isFollowingUser = followedUsers.has(post.userId);
    const isFollowingTopic = followedCategories.has(post.category);
    const topicClass = post.category.replace(/[^a-zA-Z0-9]/g, '');

    const authorData = userCache[post.userId] || { name: post.author, username: "user" };
    const date = post.timestamp && post.timestamp.seconds ? new Date(post.timestamp.seconds * 1000).toLocaleDateString() : 'Just now';
    const avatarStyle = authorData.photoURL ? `background-image: url('${authorData.photoURL}'); background-size: cover; color: transparent;` : `background: ${getColorForUser(authorData.name)}`;

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
                    <div class="user-avatar" style="${avatarStyle}; width:48px; height:48px; font-size:1.2rem;">${authorData.photoURL ? '' : authorData.name[0]}</div>
                    <div>
                        <div class="author-name" style="font-size:1rem;">${escapeHtml(authorData.name)}</div>
                        <div class="post-meta">@${escapeHtml(authorData.username)}</div>
                    </div>
                </div>
                <div style="display:flex; flex-direction:column; align-items:flex-end; gap:2px;">
                    <div style="display:flex; gap:5px;">
                        <button class="follow-btn js-follow-user-${post.userId} ${isFollowingUser ? 'following' : ''}" onclick="event.stopPropagation(); window.toggleFollowUser('${post.userId}', event)" style="font-size:0.75rem; padding:6px 12px;">${isFollowingUser ? 'Following' : '<i class="ph-bold ph-plus"></i> User'}</button>
                        <button class="follow-btn js-follow-topic-${topicClass} ${isFollowingTopic ? 'following' : ''}" onclick="event.stopPropagation(); window.toggleFollow('${post.category}', event)" style="font-size:0.75rem; padding:6px 12px;">${isFollowingTopic ? 'Following' : '<i class="ph-bold ph-plus"></i> Topic'}</button>
                    </div>
                    ${trustBadge}
                </div>
            </div>
            <h2 id="thread-view-title" data-post-id="${post.id}" style="font-size: 1.4rem; font-weight: 800; margin-bottom: 0.5rem; line-height: 1.3;">${escapeHtml(post.title)}</h2>
            <p style="font-size: 1.1rem; line-height: 1.5; color: var(--text-main); margin-bottom: 1rem;">${escapeHtml(post.content)}</p>
            ${mediaContent}
            <div style="margin-top: 1rem; padding: 10px 0; border-top: 1px solid var(--border); border-bottom: 1px solid var(--border); color: var(--text-muted); font-size: 0.9rem;">${date} • <span style="color:var(--text-main); font-weight:700;">${post.category}</span></div>
            <div class="card-actions" style="border:none; padding: 10px 0; justify-content: space-around;">
                <button id="thread-like-btn" class="action-btn" onclick="window.toggleLike('${post.id}', event)" style="color: ${isLiked ? '#00f2ea' : 'inherit'}; font-size: 1.2rem;"><i class="${isLiked ? 'ph-fill' : 'ph'} ph-thumbs-up"></i> <span style="font-size:1rem; margin-left:5px;">${post.likes || 0}</span></button>
                <button class="action-btn" onclick="document.getElementById('thread-input').focus()" style="color: var(--primary); font-size: 1.2rem;"><i class="ph ph-chat-circle"></i> <span style="font-size:1rem; margin-left:5px;">Comment</span></button>
                <button id="thread-save-btn" class="action-btn" onclick="window.toggleSave('${post.id}', event)" style="font-size: 1.2rem; color: ${isSaved ? '#00f2ea' : 'inherit'}"><i class="${isSaved ? 'ph-fill' : 'ph'} ph-bookmark-simple"></i> <span style="font-size:1rem; margin-left:5px;">${isSaved ? 'Saved' : 'Save'}</span></button>
                <button id="thread-review-btn" class="action-btn review-action ${reviewDisplay.className}" data-icon-size="1.2rem" onclick="event.stopPropagation(); window.openPeerReview('${post.id}')" style="font-size: 1.2rem;"><i class="ph ph-scales"></i> <span style="font-size:1rem; margin-left:5px;">${reviewDisplay.label}</span></button>
            </div>
        </div>`;

    const myPfp = userProfile.photoURL 
        ? `background-image: url('${userProfile.photoURL}'); background-size: cover; color: transparent;` 
        : `background: ${getColorForUser(userProfile.name)}`;
    const inputPfp = document.getElementById('thread-input-pfp');
    if(inputPfp) {
        inputPfp.style.cssText = `width:40px; height:40px; border-radius:12px; display:flex; align-items:center; justify-content:center; font-weight:bold; ${myPfp}`;
        inputPfp.innerHTML = userProfile.photoURL ? '' : userProfile.name[0];
    }

    const threadReviewBtn = document.getElementById('thread-review-btn');
    applyReviewButtonState(threadReviewBtn, myReview);
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

        await addDoc(collection(db, 'posts', activePostId, 'comments'), { 
            text, 
            mediaUrl, 
            parentId: activeReplyId, 
            userId: currentUser.uid, 
            timestamp: serverTimestamp(), 
            likes: 0, 
            likedBy: [] 
        });

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
    }
}

window.toggleCommentLike = async function(commentId, event) {
    if(event) event.stopPropagation();
    if(!activePostId) return;
    const commentRef = doc(db, 'posts', activePostId, 'comments', commentId);
    const btn = event.currentTarget;
    const isLiked = btn.style.color === 'rgb(0, 242, 234)'; 
    try {
        if(isLiked) await updateDoc(commentRef, { likes: increment(-1), likedBy: arrayRemove(currentUser.uid) });
        else await updateDoc(commentRef, { likes: increment(1), likedBy: arrayUnion(currentUser.uid) });
    } catch(e) { console.error(e); }
}

// --- Discovery & Search ---
window.renderDiscover = async function() {
    const container = document.getElementById('discover-results'); 
    container.innerHTML = "";

    const renderUsers = () => {
        let matches = [];
        if (discoverSearchTerm) {
            matches = Object.values(userCache).filter(u => 
                (u.name && u.name.toLowerCase().includes(discoverSearchTerm)) || 
                (u.username && u.username.toLowerCase().includes(discoverSearchTerm))
            );
        } else if (discoverFilter === 'All Results') {
            matches = Object.values(userCache).slice(0, 5); 
        }

        if(matches.length > 0) {
            container.innerHTML += `<div class="discover-section-header">Users</div>`;
            matches.forEach(user => {
                const uid = Object.keys(userCache).find(key => userCache[key] === user); 
                if(!uid) return;
                const pfpStyle = user.photoURL 
                    ? `background-image: url('${user.photoURL}'); background-size: cover; color: transparent;` 
                    : `background: ${getColorForUser(user.name)}`;

                container.innerHTML += `
                    <div class="social-card" style="padding:1rem; cursor:pointer; display:flex; align-items:center; gap:10px; border-left: 4px solid var(--border);" onclick="window.openUserProfile('${uid}')">
                        <div class="user-avatar" style="width:40px; height:40px; ${pfpStyle}">${user.photoURL?'':user.name[0]}</div>
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

    const renderLiveSection = () => {
        if(MOCK_LIVESTREAMS.length > 0) {
            container.innerHTML += `<div class="discover-section-header">Livestreams</div>`;
            MOCK_LIVESTREAMS.forEach(stream => { 
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

    const renderPostsSection = () => {
        let filteredPosts = allPosts;
        if(discoverSearchTerm) {
            filteredPosts = allPosts.filter(p => p.title.toLowerCase().includes(discoverSearchTerm) || p.content.toLowerCase().includes(discoverSearchTerm));
        }

        if(discoverFilter === 'Popular Posts') {
            filteredPosts.sort((a,b) => (b.likes || 0) - (a.likes || 0)); 
        } else {
            filteredPosts.sort((a,b) => (b.timestamp?.seconds || 0) - (a.timestamp?.seconds || 0));
        }

        if(filteredPosts.length > 0) {
            container.innerHTML += `<div class="discover-section-header">Posts</div>`;
            filteredPosts.forEach(post => {
                const author = userCache[post.userId] || {name: post.author};
                container.innerHTML += `
                    <div class="social-card" style="border-left: 2px solid ${THEMES[post.category] || 'transparent'}; cursor:pointer;" onclick="window.openThread('${post.id}')">
                        <div class="card-content" style="padding:1rem;">
                            <div class="category-badge">${post.category}</div>
                            <span style="float:right; font-size:0.8rem; color:var(--text-muted);">by ${escapeHtml(author.name)}</span>
                            <h3 class="post-title">${escapeHtml(cleanText(post.title))}</h3>
                            <p style="font-size:0.9rem; color:var(--text-muted);">${escapeHtml(cleanText(post.content).substring(0, 100))}...</p>
                        </div>
                    </div>`;
            });
        } else if ((discoverFilter === 'Recent Posts' || discoverFilter === 'Popular Posts') && discoverSearchTerm) {
            container.innerHTML = `<div class="empty-state"><p>No posts found.</p></div>`;
        }
    };

    if (discoverFilter === 'All Results') { 
        renderLiveSection(); 
        renderUsers(); 
        renderPostsSection(); 
        if(container.innerHTML === "") container.innerHTML = `<div class="empty-state"><p>Start typing to search everything.</p></div>`; 
    } else if (discoverFilter === 'Users') {
        renderUsers(); 
    } else if (discoverFilter === 'Livestreams') {
        renderLiveSection(); 
    } else {
        renderPostsSection();
    }
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
            profile = docSnap.data(); 
            userCache[uid] = profile; 
        } else { 
            profile = { name: "Unknown User", username: "unknown" }; 
        } 
    } 
    renderPublicProfile(uid, profile); 
}

function renderPublicProfile(uid, profileData = userCache[uid]) { 
    if(!profileData) return; 
    const container = document.getElementById('view-public-profile'); 

    const pfpStyle = profileData.photoURL 
        ? `background-image: url('${profileData.photoURL}'); background-size: cover; color: transparent;` 
        : `background: ${getColorForUser(profileData.name)}`; 

    const isFollowing = followedUsers.has(uid); 
    const userPosts = allPosts.filter(p => p.userId === uid); 
    const filteredPosts = currentProfileFilter === 'All' ? userPosts : userPosts.filter(p => p.category === currentProfileFilter); 

    let linkHtml = ''; 
    if(profileData.links) { 
        let url = profileData.links; 
        if(!url.startsWith('http')) url = 'https://' + url; 
        linkHtml = `<a href="${url}" target="_blank" style="color: var(--primary); font-size: 0.9rem; text-decoration: none; margin-top: 5px; display: inline-block;">🔗 ${escapeHtml(profileData.links)}</a>`; 
    } 

    const followersCount = profileData.followersCount || 0; 

    // FIX: Added specific ID to follower count for real-time updates
    container.innerHTML = `
        <div class="glass-panel" style="position: sticky; top: 0; z-index: 20; padding: 1rem; display: flex; align-items: center; gap: 15px;">
            <button onclick="window.goBack()" class="back-btn-outline" style="background: none; color: var(--text-main); cursor: pointer; display: flex; align-items: center; gap: 5px;"><span>←</span> Back</button>
            <h2 style="font-weight: 800; font-size: 1.2rem;">${escapeHtml(profileData.username)}</h2>
        </div>
        <div class="profile-header" style="padding-top:1rem;">
            <div class="profile-pic" style="${pfpStyle}; border: 3px solid var(--bg-card); box-shadow: 0 0 0 2px var(--primary);">${profileData.photoURL ? '' : profileData.name[0]}</div>
            <h2 style="font-weight: 800; margin-bottom: 5px;">${escapeHtml(profileData.name)}</h2>
            <p style="color: var(--text-muted);">@${escapeHtml(profileData.username)}</p>
            <p style="margin-top: 10px; max-width: 400px; margin-left: auto; margin-right: auto;">${escapeHtml(profileData.bio || "No bio yet.")}</p>
            ${linkHtml}
            <div class="stats-row">
                <div class="stat-item"><div id="profile-follower-count-${uid}">${followersCount}</div><div>Followers</div></div>
                <div class="stat-item"><div>${userPosts.reduce((acc, p) => acc + (p.likes||0), 0)}</div><div>Likes</div></div>
                <div class="stat-item"><div>${userPosts.length}</div><div>Posts</div></div>
            </div>
            <div style="display:flex; gap:10px; justify-content:center; margin-top:1rem;">
                <button onclick="window.toggleFollowUser('${uid}', event)" class="create-btn-sidebar js-follow-user-${uid}" style="width: auto; padding: 0.6rem 2rem; margin-top: 0; background: ${isFollowing ? 'transparent' : 'var(--primary)'}; border: 1px solid var(--primary); color: ${isFollowing ? 'var(--primary)' : 'black'};">${isFollowing ? 'Following' : 'Follow'}</button>
                <button class="create-btn-sidebar" style="width: auto; padding: 0.6rem 2rem; margin-top: 0; background: var(--bg-hover); color: var(--text-main); border: 1px solid var(--border);">Message</button>
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
        filteredPosts.forEach(post => { 
            const date = post.timestamp && post.timestamp.seconds ? new Date(post.timestamp.seconds * 1000).toLocaleDateString() : 'Just now'; 
            const isLiked = post.likedBy && post.likedBy.includes(currentUser.uid); 
            const isSaved = userProfile.savedPosts && userProfile.savedPosts.includes(post.id); 

            let mediaContent = ''; 
            if (post.mediaUrl) { 
                if (post.type === 'video') mediaContent = `<div class="video-container" onclick="window.openFullscreenMedia('${post.mediaUrl}', 'video')"><video src="${post.mediaUrl}" controls class="post-media"></video></div>`; 
                else mediaContent = `<img src="${post.mediaUrl}" class="post-media" alt="Post Content" onclick="window.openFullscreenMedia('${post.mediaUrl}', 'image')">`; 
            } 

            feedContainer.innerHTML += `
                <div class="social-card" style="border-left: 2px solid ${THEMES[post.category] || 'transparent'};">
                    <div class="card-content" style="padding-top:1rem; cursor: pointer;" onclick="window.openThread('${post.id}')">
                        <div class="category-badge">${post.category}</div>
                        <span style="font-size:0.8rem; color:var(--text-muted); float:right;">${date}</span>
                        <h3 class="post-title">${escapeHtml(cleanText(post.title))}</h3>
                        <p>${escapeHtml(cleanText(post.content))}</p>
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
    const userPosts = allPosts.filter(p => p.userId === currentUser.uid);
    const filteredPosts = currentProfileFilter === 'All' ? userPosts : userPosts.filter(p => p.category === currentProfileFilter);

    const displayName = userProfile.name || userProfile.nickname || "Nexara User";

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
            <div class="profile-pic" style="background-image:url('${userProfile.photoURL||''}'); background-size:cover; background-color:var(--primary);">${userProfile.photoURL?'':displayName[0]}</div>
            <h2 style="font-weight:800;">${escapeHtml(displayName)}</h2>
            ${realNameHtml}
            <p style="color:var(--text-muted);">@${escapeHtml(userProfile.username)}</p>
            <p style="margin-top:10px;">${escapeHtml(userProfile.bio)}</p>
            ${regionHtml}
            ${linkHtml}
            <div class="stats-row">
                <div class="stat-item"><div>${followersCount}</div><div>Followers</div></div>
                <div class="stat-item"><div>${userPosts.reduce((acc, p) => acc + (p.likes||0), 0)}</div><div>Likes</div></div>
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
        filteredPosts.forEach(post => { 
            const date = post.timestamp ? new Date(post.timestamp.seconds * 1000).toLocaleDateString() : 'Just now'; 
            feedContainer.innerHTML += `
                <div class="social-card" style="border-left: 2px solid ${THEMES[post.category] || 'transparent'};">
                    <div class="card-content" style="padding-top:1rem; cursor: pointer;" onclick="window.openThread('${post.id}')">
                        <div class="category-badge">${post.category}</div>
                        <span style="float:right; font-size:0.8rem; color:var(--text-muted);">${date}</span>
                        <h3 class="post-title">${escapeHtml(cleanText(post.title))}</h3>
                        <p>${escapeHtml(cleanText(post.content))}</p>
                    </div>
                </div>`; 
        }); 
    } 
}

// --- Utils & Helpers ---
window.setCategory = function(c) {
    currentCategory = c;
    document.querySelectorAll('.category-pill').forEach(el => {
        if(el.textContent.includes(c) || (c === 'For You' && el.textContent === 'For You')) el.classList.add('active'); 
        else el.classList.remove('active');
    });
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

    MOCK_LIVESTREAMS.forEach(stream => { 
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
function escapeHtml(text) { return text ? text.replace(/&/g, "&amp;").replace(/</g, "&lt;") : ""; }
function cleanText(text) { if(typeof text !== 'string') return ""; return text.replace(new RegExp(["badword", "hate"].join("|"), "gi"), "🤐"); }
function renderSaved() { currentCategory = 'Saved'; renderFeed('saved-content'); }

// Small Interaction Utils
window.setDiscoverFilter = function(filter) { discoverFilter = filter; document.querySelectorAll('.discover-pill').forEach(el => { if(el.textContent.includes(filter)) el.classList.add('active'); else el.classList.remove('active'); }); renderDiscover(); }
window.handleSearchInput = function(e) { discoverSearchTerm = e.target.value.toLowerCase(); renderDiscover(); }
window.setSavedFilter = function(filter) { savedFilter = filter; document.querySelectorAll('.saved-pill').forEach(el => { if(el.textContent === filter) el.classList.add('active'); else el.classList.remove('active'); }); renderSaved(); }
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
window.togglePostOption = function(type) { const area = document.getElementById('extra-options-area'); const target = document.getElementById('post-opt-' + type); ['poll', 'gif', 'schedule', 'location'].forEach(t => { if(t !== type) document.getElementById('post-opt-' + t).style.display = 'none'; }); if (target.style.display === 'none') { area.style.display = 'block'; target.style.display = 'block'; } else { target.style.display = 'none'; area.style.display = 'none'; } }
window.closeReview = () => document.getElementById('review-modal').style.display = 'none';

// --- Messaging (DMs) ---
window.toggleNewChatModal = function(show = true) {
    const modal = document.getElementById('new-chat-modal');
    if(modal) modal.style.display = show ? 'flex' : 'none';
};
window.openNewChatModal = () => window.toggleNewChatModal(true);

window.searchChatUsers = async function(term = '') {
    const resultsEl = document.getElementById('chat-search-results');
    if(!resultsEl) return;
    resultsEl.innerHTML = '';
    const cleaned = term.trim().toLowerCase();
    if(cleaned.length < 2) return;
    const qSnap = await getDocs(query(collection(db, 'users'), where('username', '>=', cleaned), where('username', '<=', cleaned + '~')));
    qSnap.forEach(docSnap => {
        const data = docSnap.data();
        const row = document.createElement('div');
        row.className = 'conversation-item';
        row.innerHTML = `<div><strong>@${data.username || 'user'}</strong><div style="color:var(--text-muted); font-size:0.85rem;">${data.displayName || data.name || 'Nexara User'}</div></div>`;
        row.onclick = () => createConversationWithUser(docSnap.id, data);
        resultsEl.appendChild(row);
    });
};

async function createConversationWithUser(targetUid, targetData = {}) {
    if(!requireAuth()) return;
    const existing = conversationsCache.find(c => c.members && c.members.includes(targetUid));
    if(existing) {
        setActiveConversation(existing.id, existing);
        toggleNewChatModal(false);
        return;
    }
    const payload = {
        members: [currentUser.uid, targetUid],
        createdAt: serverTimestamp(),
        updatedAt: serverTimestamp(),
        lastMessageText: '',
        lastMessageAt: serverTimestamp(),
        requestState: { [currentUser.uid]: 'inbox', [targetUid]: 'requested' }
    };
    const convoRef = await addDoc(collection(db, 'conversations'), payload);
    conversationsCache.push({ id: convoRef.id, ...payload });
    toggleNewChatModal(false);
    setActiveConversation(convoRef.id, payload);
}

function initConversations() {
    if(!requireAuth()) return;
    if(conversationsUnsubscribe) conversationsUnsubscribe();
    const convRef = query(collection(db, 'conversations'), where('members', 'array-contains', currentUser.uid), orderBy('updatedAt', 'desc'));
    conversationsUnsubscribe = onSnapshot(convRef, snap => {
        conversationsCache = snap.docs.map(d => ({ id: d.id, ...d.data() }));
        renderConversationList();
    });
}

function renderConversationList() {
    const listEl = document.getElementById('conversation-list');
    if(!listEl) return;
    listEl.innerHTML = '';
    if(conversationsCache.length === 0) {
        listEl.innerHTML = '<div class="empty-state">No conversations yet.</div>';
        return;
    }
    conversationsCache.forEach(convo => {
        const partnerId = convo.members.find(m => m !== currentUser.uid) || currentUser.uid;
        const display = userCache[partnerId]?.username || 'user';
        const item = document.createElement('div');
        item.className = 'conversation-item' + (activeConversationId === convo.id ? ' active' : '');
        item.innerHTML = `<div><strong>@${display}</strong><div style="color:var(--text-muted); font-size:0.8rem;">${convo.lastMessageText || 'Tap to start'}</div></div><span style="color:var(--text-muted); font-size:0.75rem;">${convo.requestState?.[currentUser.uid] === 'requested' ? '<span class="badge">Requested</span>' : ''}</span>`;
        item.onclick = () => setActiveConversation(convo.id, convo);
        listEl.appendChild(item);
    });
}

function setActiveConversation(convoId, convoData = null) {
    activeConversationId = convoId;
    const header = document.getElementById('message-header');
    const partnerId = (convoData || conversationsCache.find(c => c.id === convoId) || {}).members?.find(m => m !== currentUser.uid);
    header.textContent = partnerId ? `Chat with @${userCache[partnerId]?.username || 'user'}` : 'Conversation';
    listenToMessages(convoId);
}

function listenToMessages(convoId) {
    if(messagesUnsubscribe) messagesUnsubscribe();
    const msgRef = query(collection(db, 'conversations', convoId, 'messages'), orderBy('createdAt'));
    messagesUnsubscribe = onSnapshot(msgRef, snap => {
        const msgs = snap.docs.map(d => ({ id: d.id, ...d.data() }));
        renderMessages(msgs);
    });
}

function renderMessages(msgs = []) {
    const body = document.getElementById('message-thread');
    if(!body) return;
    body.innerHTML = '';
    msgs.forEach(msg => {
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
    await updateDoc(doc(db, 'conversations', activeConversationId), {
        lastMessageText: mediaURL ? '📷 Photo' : text,
        lastMessageAt: serverTimestamp(),
        updatedAt: serverTimestamp(),
        requestState: { [currentUser.uid]: 'inbox' }
    }, { merge: true });
    if(input) input.value = '';
    if(fileInput) fileInput.value = '';
};

// --- Videos ---
window.openVideoUploadModal = () => window.toggleVideoUploadModal(true);
window.toggleVideoUploadModal = function(show = true) {
    const modal = document.getElementById('video-upload-modal');
    if(modal) modal.style.display = show ? 'flex' : 'none';
};

function initVideoFeed() {
    if(videosUnsubscribe) return; // already live
    const refVideos = query(collection(db, 'videos'), orderBy('createdAt', 'desc'));
    videosUnsubscribe = onSnapshot(refVideos, snap => {
        const videos = snap.docs.map(d => ({ id: d.id, ...d.data() }));
        renderVideoFeed(videos);
    });
}

function renderVideoFeed(videos = []) {
    const feed = document.getElementById('video-feed');
    if(!feed) return;
    feed.innerHTML = '';
    if(videos.length === 0) { feed.innerHTML = '<div class="empty-state">No videos yet.</div>'; return; }
    videos.forEach(video => {
        const card = document.createElement('div');
        card.className = 'video-card';
        card.innerHTML = `
            <video src="${video.videoURL}" playsinline loop muted></video>
            <div class="video-meta">
                <div style="display:flex; justify-content:space-between; align-items:center;">
                    <div>
                        <div style="font-weight:800;">${escapeHtml(video.caption || '')}</div>
                        <div style="color:var(--text-muted); font-size:0.85rem;">${(video.hashtags || []).map(t => '#' + t).join(' ')}</div>
                    </div>
                    <div style="display:flex; gap:8px;">
                        <button class="icon-pill" onclick="window.likeVideo('${video.id}')"><i class="ph ph-heart"></i> ${video.stats?.likes || 0}</button>
                        <button class="icon-pill" onclick="window.saveVideo('${video.id}')"><i class="ph ph-bookmark"></i></button>
                    </div>
                </div>
            </div>`;
        feed.appendChild(card);
    });
    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            const vid = entry.target;
            if(entry.isIntersecting) { vid.play(); incrementVideoViews(vid.dataset.videoId); }
            else vid.pause();
        });
    }, { threshold: 0.6 });
    feed.querySelectorAll('video').forEach((v, idx) => { v.dataset.videoId = videos[idx].id; observer.observe(v); });
}

window.uploadVideo = async function() {
    if(!requireAuth()) return;
    const fileInput = document.getElementById('video-file');
    if(!fileInput || !fileInput.files || !fileInput.files[0]) return;
    const file = fileInput.files[0];
    const caption = document.getElementById('video-caption').value || '';
    const hashtags = (document.getElementById('video-tags').value || '').split(',').map(t => t.replace('#','').trim()).filter(Boolean);
    const visibility = document.getElementById('video-visibility').value || 'public';
    const videoId = `${Date.now()}`;
    const storageRef = ref(storage, `videos/${currentUser.uid}/${videoId}/source.mp4`);
    await uploadBytes(storageRef, file);
    const videoURL = await getDownloadURL(storageRef);
    await setDoc(doc(db, 'videos', videoId), {
        ownerId: currentUser.uid,
        caption,
        hashtags,
        createdAt: serverTimestamp(),
        videoURL,
        thumbURL: '',
        visibility,
        stats: { likes: 0, comments: 0, saves: 0, views: 0 }
    });
    toggleVideoUploadModal(false);
};

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
    liveSessionsUnsubscribe = onSnapshot(liveRef, snap => {
        const sessions = snap.docs.map(d => ({ id: d.id, ...d.data() }));
        const container = document.getElementById('live-grid-container');
        if(!container) return;
        container.innerHTML = '';
        if(sessions.length === 0) { container.innerHTML = '<div class="empty-state">No live sessions.</div>'; return; }
        sessions.forEach(s => {
            const card = document.createElement('div');
            card.className = 'live-card';
            card.innerHTML = `<div class="live-card-title">${escapeHtml(s.title || 'Live Session')}</div><div class="live-card-meta"><span>${escapeHtml(s.category || '')}</span><span>${(s.tags||[]).join(', ')}</span></div><div style="margin-top:10px;"><button class="icon-pill" onclick="window.openLiveSession('${s.id}')"><i class="ph ph-play"></i> Watch</button></div>`;
            container.appendChild(card);
        });
    });
}

window.createLiveSession = async function() {
    if(!requireAuth()) return;
    const title = document.getElementById('live-title').value;
    const category = document.getElementById('live-category').value;
    const tags = (document.getElementById('live-tags').value || '').split(',').map(t => t.trim()).filter(Boolean);
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
    onSnapshot(chatRef, snap => {
        const chatEl = document.getElementById('live-chat');
        if(!chatEl) return;
        chatEl.innerHTML = '';
        snap.docs.forEach(docSnap => {
            const data = docSnap.data();
            const row = document.createElement('div');
            row.textContent = `${userCache[data.senderId]?.username || 'user'}: ${data.text}`;
            chatEl.appendChild(row);
        });
    });
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
    const isStaff = userProfile.role === 'staff' || userProfile.role === 'admin';
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
    staffRequestsUnsub = onSnapshot(collection(db, 'verificationRequests'), snap => {
        const container = document.getElementById('verification-requests');
        if(!container) return;
        container.innerHTML = '';
        snap.docs.forEach(docSnap => {
            const data = docSnap.data();
            const card = document.createElement('div');
            card.className = 'social-card';
            card.innerHTML = `<div style="padding:1rem;"><div style="font-weight:800;">${data.category}</div><div style="font-size:0.9rem; color:var(--text-muted);">${(data.evidenceLinks||[]).join('<br>')}</div><div style="margin-top:6px; display:flex; gap:8px;"><button class="icon-pill" onclick="window.approveVerification('${docSnap.id}', '${data.userId}')">Approve</button><button class="icon-pill" onclick="window.denyVerification('${docSnap.id}')">Deny</button></div></div>`;
            container.appendChild(card);
        });
    });
}

window.approveVerification = async function(requestId, userId) {
    await updateDoc(doc(db, 'verificationRequests', requestId), { status: 'approved', reviewedAt: serverTimestamp() });
    if(userId) await setDoc(doc(db, 'users', userId), { verified: true, updatedAt: serverTimestamp() }, { merge: true });
    await addDoc(collection(db, 'adminLogs'), { actorId: currentUser.uid, action: 'approveVerification', targetRef: requestId, createdAt: serverTimestamp() });
};

window.denyVerification = async function(requestId) {
    await updateDoc(doc(db, 'verificationRequests', requestId), { status: 'denied', reviewedAt: serverTimestamp() });
    await addDoc(collection(db, 'adminLogs'), { actorId: currentUser.uid, action: 'denyVerification', targetRef: requestId, createdAt: serverTimestamp() });
};

function listenReports() {
    if(staffReportsUnsub) return;
    staffReportsUnsub = onSnapshot(collection(db, 'reports'), snap => {
        const container = document.getElementById('reports-queue');
        if(!container) return;
        container.innerHTML = '';
        snap.docs.forEach(docSnap => {
            const data = docSnap.data();
            const card = document.createElement('div');
            card.className = 'social-card';
            card.innerHTML = `<div style="padding:1rem;"><div style="font-weight:800;">${data.type || 'report'}</div><div style="color:var(--text-muted); font-size:0.9rem;">${data.reason || ''}</div></div>`;
            container.appendChild(card);
        });
    });
}

function listenAdminLogs() {
    if(staffLogsUnsub) return;
    staffLogsUnsub = onSnapshot(collection(db, 'adminLogs'), snap => {
        const container = document.getElementById('admin-logs');
        if(!container) return;
        container.innerHTML = '';
        snap.docs.forEach(docSnap => {
            const data = docSnap.data();
            const row = document.createElement('div');
            row.textContent = `${data.actorId}: ${data.action}`;
            container.appendChild(row);
        });
    });
}

// --- Verification Request ---
window.openVerificationRequest = function() { toggleVerificationModal(true); };
window.toggleVerificationModal = function(show = true) { const modal = document.getElementById('verification-modal'); if(modal) modal.style.display = show ? 'flex' : 'none'; };
window.submitVerificationRequest = async function() {
    if(!requireAuth()) return;
    const category = document.getElementById('verify-category').value;
    const links = (document.getElementById('verify-links').value || '').split('\n').map(l => l.trim()).filter(Boolean);
    const notes = document.getElementById('verify-notes').value;
    await addDoc(collection(db, 'verificationRequests'), { userId: currentUser.uid, category, evidenceLinks: links, notes, status: 'pending', createdAt: serverTimestamp() });
    toggleVerificationModal(false);
};

// --- Security Rules Snippet (reference) ---
// See firestore.rules for suggested rules ensuring users write their own content and staff-only access.

// Start App
initApp();