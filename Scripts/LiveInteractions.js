import {
    initializeApp,
    getApps
} from 'https://www.gstatic.com/firebasejs/11.6.1/firebase-app.js';
import {
    getFirestore,
    collection,
    doc,
    onSnapshot,
    addDoc,
    serverTimestamp,
    setDoc
} from 'https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js';
import { getAuth } from 'https://www.gstatic.com/firebasejs/11.6.1/firebase-auth.js';

const firebaseConfig = {
    apiKey: 'AIzaSyDg9Duz3xicI3pvvOtLCrV1DJRWDI0NtYA',
    authDomain: 'spike-streaming-service.firebaseapp.com',
    projectId: 'spike-streaming-service',
    storageBucket: 'spike-streaming-service.firebasestorage.app',
    messagingSenderId: '592955741032',
    appId: '1:592955741032:web:dbd629cc957b67fc69bcdd',
    measurementId: 'G-BF3GFFY3D6'
};

function getFirebase() {
    if (!getApps().length) {
        initializeApp(firebaseConfig);
    }
    const app = getApps()[0];
    return {
        auth: getAuth(app),
        db: getFirestore(app)
    };
}

function createToast(message, stack) {
    if (!stack) return;
    const toast = document.createElement('div');
    toast.className = 'live-toast';
    toast.textContent = message;
    stack.appendChild(toast);
    setTimeout(() => toast.classList.add('visible'), 10);
    setTimeout(() => {
        toast.classList.remove('visible');
        setTimeout(() => toast.remove(), 260);
    }, 2600);
}

export class LiveInteractions {
    constructor(db, auth) {
        const firebase = db && auth ? { db, auth } : getFirebase();
        this.db = firebase.db;
        this.auth = firebase.auth;
        this.likesUnsub = null;
        this.chatUnsub = null;
        this.followUnsub = null;
        this.streamUnsub = null;
        this.rateLimitWindow = [];
        this.likeCount = 0;
        this.followerCount = 0;
    }

    bind(streamId, streamData, controls, chatRefs) {
        this.streamId = streamId;
        this.streamData = streamData;
        this.controls = controls;
        this.chatRefs = chatRefs;
        this.attachListeners();
        this.attachChat();
        this.attachLikes();
        this.attachFollows();
        this.attachShare();
        this.updateViewerLabel();
    }

    updateStreamData(data) {
        this.streamData = { ...this.streamData, ...data };
        this.updateViewerLabel();
    }

    updateViewerLabel() {
        if (!this.controls?.viewerLabel) return;
        const viewers = this.streamData?.viewerCount ?? '--';
        this.controls.viewerLabel.textContent = `Viewers: ${viewers} | Likes: ${this.likeCount || 0} | Followers: ${this.followerCount || 0}`;
    }

    attachListeners() {
        const { likeBtn, followBtn } = this.controls;
        if (likeBtn) {
            likeBtn.addEventListener('click', () => this.toggleLike());
        }
        if (followBtn) {
            followBtn.addEventListener('click', () => this.toggleFollow());
        }
    }

    attachShare() {
        const { shareBtn, toastStack } = this.controls;
        if (!shareBtn) return;
        shareBtn.addEventListener('click', async () => {
            const link = `${window.location.origin}/live/watch/${this.streamId}`;
            await navigator.clipboard?.writeText(link);
            createToast('Link copied', toastStack);
        });
    }

    attachChat() {
        if (!this.chatRefs?.formEl) return;
        this.chatRefs.formEl.addEventListener('submit', (e) => {
            e.preventDefault();
            this.sendMessage();
        });
        const chatCol = collection(this.db, 'liveStreams', this.streamId, 'chat');
        this.chatUnsub = onSnapshot(chatCol, (snapshot) => {
            const messages = snapshot.docs
                .map(doc => ({ id: doc.id, ...doc.data() }))
                .sort((a, b) => (a.timestamp?.seconds || 0) - (b.timestamp?.seconds || 0));
            this.renderMessages(messages);
        });
    }

    renderMessages(messages) {
        const list = this.chatRefs.messagesEl;
        list.innerHTML = '';
        messages.forEach((msg) => {
            const item = document.createElement('div');
            item.className = 'chat-message fade-in';
            item.innerHTML = `
                <span class="chat-author ${msg.isStreamer ? 'chat-author-streamer' : ''}">${msg.isStreamer ? 'Host' : msg.uid || 'Guest'}</span>
                <span class="chat-text">${msg.text}</span>
            `;
            list.appendChild(item);
        });
        list.scrollTop = list.scrollHeight;
    }

    canSend() {
        const now = Date.now();
        this.rateLimitWindow = this.rateLimitWindow.filter(ts => now - ts < 5000);
        return this.rateLimitWindow.length < 3;
    }

    async sendMessage() {
        const input = this.chatRefs.inputEl;
        const text = input.value.trim();
        if (!text) return;
        if (!this.canSend()) {
            createToast('You are sending messages too quickly', this.controls.toastStack);
            return;
        }
        const user = this.auth.currentUser;
        const chatCol = collection(this.db, 'liveStreams', this.streamId, 'chat');
        await addDoc(chatCol, {
            uid: user?.uid || 'guest',
            text,
            timestamp: serverTimestamp(),
            isStreamer: user?.uid === this.streamData?.broadcaster?.uid
        });
        this.rateLimitWindow.push(Date.now());
        input.value = '';
    }

    attachLikes() {
        const likesCol = collection(this.db, 'liveStreams', this.streamId, 'likes');
        this.likesUnsub = onSnapshot(likesCol, (snapshot) => {
            const count = snapshot.size;
            const currentUid = this.auth.currentUser?.uid;
            const liked = snapshot.docs.some(doc => doc.id === currentUid);
            const { likeBtn } = this.controls;
            if (likeBtn) {
                likeBtn.classList.toggle('liked', liked);
                likeBtn.innerHTML = liked ? '❤️ Liked' : '🤍 Like';
            }
            this.likeCount = count;
            this.liked = liked;
            this.updateViewerLabel();
            this.controls.likeBtn?.classList.add('pulse');
            setTimeout(() => this.controls.likeBtn?.classList.remove('pulse'), 360);
        });
    }

    async toggleLike() {
        if (!this.controls?.likeBtn) return;
        const current = this.auth.currentUser;
        if (!current) {
            createToast('Sign in to like streams', this.controls.toastStack);
            return;
        }
        const likeDoc = doc(this.db, 'liveStreams', this.streamId, 'likes', current.uid);
        await setDoc(likeDoc, { likedAt: serverTimestamp() });
        createToast('Appreciated!', this.controls.toastStack);
    }

    attachFollows() {
        const targetId = this.streamData?.broadcaster?.uid;
        if (!targetId) return;
        const followerCol = collection(this.db, 'users', targetId, 'followers');
        this.followUnsub = onSnapshot(followerCol, (snapshot) => {
            const currentUid = this.auth.currentUser?.uid;
            const isFollowing = snapshot.docs.some(doc => doc.id === currentUid);
            const count = snapshot.size;
            const { followBtn, toastStack } = this.controls;
            if (followBtn) {
                followBtn.classList.toggle('following', isFollowing);
                followBtn.textContent = isFollowing ? 'Following' : 'Follow';
                followBtn.dataset.count = count;
            }
            this.followerCount = count;
            this.updateViewerLabel();
        });
    }

    async toggleFollow() {
        const targetId = this.streamData?.broadcaster?.uid;
        if (!targetId) return;
        const current = this.auth.currentUser;
        if (!current) {
            createToast('Sign in to follow creators', this.controls.toastStack);
            return;
        }
        const followerDoc = doc(this.db, 'users', targetId, 'followers', current.uid);
        await setDoc(followerDoc, { followedAt: serverTimestamp() });
        createToast('Followed creator', this.controls.toastStack);
    }

    destroy() {
        if (this.likesUnsub) this.likesUnsub();
        if (this.chatUnsub) this.chatUnsub();
        if (this.followUnsub) this.followUnsub();
        if (this.streamUnsub) this.streamUnsub();
    }
}
