// scripts/Live.js
// Live viewing controller for Nexera liveStreams

import { getFirestore, doc, onSnapshot } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";
import { NexeraLivePlayer } from "./VideoPlayer.js";
import { initLiveChat } from "./LiveChat.js";
import { sendLike, followStreamer } from "./LiveInteractions.js";

let player = null;
let playerLoaded = false;
let liveUnsubscribe = null;
let chatCleanup = null;
let activeStreamId = null;
let activeHostId = null;
let likeButtonHandler = null;
let followButtonHandler = null;

function resolveStreamId() {
    const path = window.location.pathname || "";
    const segments = path.split("/").filter(Boolean);
    const liveIndex = segments.indexOf("live");
    if (liveIndex !== -1 && segments[liveIndex + 1]) {
        return segments[liveIndex + 1];
    }

    const hash = (window.location.hash || "").replace(/^#/, "");
    if (hash.startsWith("live/")) {
        return hash.split("/")[1];
    }

    return null;
}

function toggleOfflineBanner(show) {
    const banner = document.getElementById("live-offline-banner");
    if (banner) {
        banner.style.display = show ? "block" : "none";
    }
}

function updateLikeCount(likes = 0) {
    const likeCountEl = document.getElementById("live-like-count");
    if (likeCountEl) {
        likeCountEl.textContent = likes ? likes.toString() : "0";
    }
}

function bindInteractionButtons(streamId, hostId, currentUser) {
    const likeBtn = document.getElementById("live-like-button");
    if (likeBtn && !likeButtonHandler) {
        likeButtonHandler = () => sendLike(streamId);
        likeBtn.addEventListener("click", likeButtonHandler);
    }

    const followBtn = document.getElementById("live-follow-button");
    if (followBtn && hostId && !followButtonHandler) {
        followButtonHandler = () => followStreamer(hostId, currentUser);
        followBtn.addEventListener("click", followButtonHandler);
    }
}

export function initialize(streamId, currentUser) {
    teardown();

    const resolvedStreamId = streamId || resolveStreamId();
    if (!resolvedStreamId) return;

    activeStreamId = resolvedStreamId;

    const root = document.getElementById("live-player-root");
    if (root) {
        root.style.display = "block";
        player = new NexeraLivePlayer(root);
    }

    const db = getFirestore();

    liveUnsubscribe = onSnapshot(
        doc(db, "liveStreams", resolvedStreamId),
        async snap => {
            try {
                if (!snap.exists()) {
                    if (player) player.renderOffline();
                    toggleOfflineBanner(true);
                    return;
                }

                const data = snap.data() || {};

                updateLikeCount(data.likes);
                activeHostId = data.hostId || data.userId || data.author || data.ownerId || null;

                if (!chatCleanup) {
                    chatCleanup = initLiveChat(resolvedStreamId, currentUser);
                }

                bindInteractionButtons(resolvedStreamId, activeHostId, currentUser);

                if (!data.isLive) {
                    if (player) player.renderOffline();
                    toggleOfflineBanner(true);
                    playerLoaded = false;
                    return;
                }

                toggleOfflineBanner(false);

                if (!playerLoaded && player) {
                    await player.load({
                        playbackUrl: data.playbackUrl,
                        visibility: data.visibility,
                        channelArn: data.channelArn,
                    });
                    playerLoaded = true;
                }
            } catch (err) {
                console.error("Live stream snapshot handling failed", err);
            }
        },
        error => {
            console.error("Live stream snapshot error", error);
            if (player) player.renderOffline();
            toggleOfflineBanner(true);
        }
    );
}

export function teardown() {
    if (liveUnsubscribe) {
        liveUnsubscribe();
        liveUnsubscribe = null;
    }

    if (chatCleanup) {
        chatCleanup();
        chatCleanup = null;
    }

    if (player) {
        player.destroy();
        player = null;
    }

    const likeBtn = document.getElementById("live-like-button");
    if (likeBtn && likeButtonHandler) {
        likeBtn.removeEventListener("click", likeButtonHandler);
    }
    likeButtonHandler = null;

    const followBtn = document.getElementById("live-follow-button");
    if (followBtn && followButtonHandler) {
        followBtn.removeEventListener("click", followButtonHandler);
    }
    followButtonHandler = null;

    activeStreamId = null;
    activeHostId = null;
    playerLoaded = false;
}
