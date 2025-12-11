import { getApp } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-app.js";
import { getFirestore, collection, addDoc, serverTimestamp, doc, updateDoc, increment } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";
import { getAuth } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-auth.js";

function getDb() {
    try {
        const app = getApp();
        return getFirestore(app);
    } catch (error) {
        console.error('Firestore unavailable', error);
        return null;
    }
}

function getUserId() {
    try {
        const auth = getAuth();
        return auth.currentUser ? auth.currentUser.uid : null;
    } catch (error) {
        console.error('Auth unavailable', error);
        return null;
    }
}

function renderChatMessage(message, userId) {
    const chatPanel = document.querySelector('.live-chat-panel');
    if (!chatPanel) return;
    const item = document.createElement('div');
    item.className = 'live-chat-message';
    const authorLabel = message.userId || userId || 'guest';
    item.innerHTML = `<div class="live-chat-author">${authorLabel}</div><div class="live-chat-text">${message.message || ''}</div>`;
    chatPanel.appendChild(item);
    chatPanel.scrollTop = chatPanel.scrollHeight;
}

export async function sendLike(streamId) {
    const db = getDb();
    if (!db || !streamId) return;
    try {
        await updateDoc(doc(db, 'liveStreams', streamId), { likes: increment(1) });
    } catch (error) {
        console.error('Failed to send like', error);
    }
}

export async function followStreamer(streamId) {
    const db = getDb();
    if (!db || !streamId) return;
    try {
        await updateDoc(doc(db, 'liveStreams', streamId), { followers: increment(1) });
    } catch (error) {
        console.error('Failed to follow streamer', error);
    }
}

export async function submitChatMessage(streamId, message) {
    const db = getDb();
    const uid = getUserId();
    if (!db || !streamId || !message) return;
    try {
        const payload = { message, createdAt: serverTimestamp(), userId: uid };
        await addDoc(collection(db, 'liveStreams', streamId, 'chat'), payload);
        renderChatMessage(payload, uid);
    } catch (error) {
        console.error('Failed to submit chat message', error);
    }
}

export function initialize() {}
export function teardown() {}
