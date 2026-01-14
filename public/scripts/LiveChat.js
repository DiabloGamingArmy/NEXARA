// scripts/LiveChat.js
// Live chat controller for liveStreams

import {
    getFirestore,
    collection,
    query,
    orderBy,
    onSnapshot,
    addDoc,
    serverTimestamp,
} from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";

export function initLiveChat(streamId, currentUser) {
    const messagesEl = document.getElementById("live-chat-messages");
    const inputEl = document.getElementById("live-chat-input");
    const sendBtn = document.getElementById("live-chat-send");

    if (!streamId || !messagesEl) return null;

    const db = getFirestore();

    const chatQuery = query(
        collection(db, "liveStreams", streamId, "chat"),
        orderBy("createdAt")
    );

    const unsubscribe = onSnapshot(chatQuery, snap => {
        messagesEl.innerHTML = "";

        snap.forEach(docSnap => {
            const data = docSnap.data() || {};
            const row = document.createElement("div");
            row.className = "live-chat-row";
            row.innerHTML = `
                <span class="live-chat-name">${(data.displayName || data.uid || "User").toString()}</span>
                <span class="live-chat-message">${(data.message || data.text || "").toString()}</span>
            `;
            messagesEl.appendChild(row);
        });

        messagesEl.scrollTop = messagesEl.scrollHeight;
    });

    const sendMessage = async () => {
        if (!inputEl || !inputEl.value.trim()) return;

        await addDoc(collection(db, "liveStreams", streamId, "chat"), {
            uid: currentUser?.uid || "",
            displayName: currentUser?.displayName || "",
            message: inputEl.value.trim(),
            createdAt: serverTimestamp(),
        });

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
