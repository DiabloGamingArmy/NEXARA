// scripts/LiveChat.js
// Live chat controller for liveStreams

import {
    getFirestore,
    collection,
    query,
    orderBy,
    limitToLast,
    onSnapshot,
} from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";
import { getFunctions, httpsCallable } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-functions.js";

export function initLiveChat(streamId, currentUser) {
    const messagesEl = document.getElementById("live-chat-messages");
    const inputEl = document.getElementById("live-chat-input");
    const sendBtn = document.getElementById("live-chat-send");

    if (!streamId || !messagesEl) return null;

    const db = getFirestore();
    const functions = getFunctions();
    const sendLiveChatMessage = httpsCallable(functions, "sendLiveChatMessage");

    const chatQuery = query(
        collection(db, "liveSessions", streamId, "chat"),
        orderBy("createdAt", "asc"),
        limitToLast(200)
    );

    const renderedIds = new Set();
    const unsubscribe = onSnapshot(chatQuery, snap => {
        const shouldStick = messagesEl.scrollHeight - messagesEl.scrollTop - messagesEl.clientHeight <= 80;
        snap.docChanges().forEach(change => {
            if (change.type !== "added") return;
            if (renderedIds.has(change.doc.id)) return;
            renderedIds.add(change.doc.id);
            const data = change.doc.data() || {};
            const row = document.createElement("div");
            row.className = "live-chat-row";
            row.innerHTML = `
                <span class="live-chat-name">${(data.displayName || data.uid || "User").toString()}</span>
                <span class="live-chat-message">${(data.message || data.text || "").toString()}</span>
            `;
            messagesEl.appendChild(row);
        });
        if (shouldStick) {
            messagesEl.scrollTop = messagesEl.scrollHeight;
        }
    });

    const sendMessage = async () => {
        if (!inputEl || !inputEl.value.trim()) return;

        try {
            await sendLiveChatMessage({ sessionId: streamId, text: inputEl.value.trim() });
        } catch (error) {
            console.error("Live chat send failed", error);
            return;
        }

        inputEl.value = "";
        messagesEl.scrollTop = messagesEl.scrollHeight;
    };

    if (sendBtn) {
        sendBtn.onclick = sendMessage;
    }

    if (inputEl) {
        inputEl.addEventListener("keypress", event => {
            if (event.key === "Enter") {
                event.preventDefault();
                sendMessage();
            }
        });
    }

    return () => {
        unsubscribe();
        if (sendBtn) sendBtn.onclick = null;
        if (inputEl) {
            inputEl.replaceWith(inputEl.cloneNode(true));
        }
    };
}
