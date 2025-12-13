// scripts/VideoPlayer.js
// Nexera Live Video Player (Viewer-Side)

import { getFunctions, httpsCallable } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-functions.js";

const IVS_PLAYER_SRC =
    "https://player.live-video.net/1.0.0/amazon-ivs-player.min.js";

function loadIVSPlayer() {
    if (window.IVSPlayer) return Promise.resolve(window.IVSPlayer);
    return new Promise((resolve, reject) => {
        const script = document.createElement("script");
        script.src = IVS_PLAYER_SRC;
        script.async = true;
        script.onload = () => resolve(window.IVSPlayer);
        script.onerror = reject;
        document.head.appendChild(script);
    });
}

export class NexeraLivePlayer {
    constructor(root) {
        this.root = root;
        this.player = null;
        this.video = null;
        this.functions = getFunctions();
    }

    async load({ playbackUrl, visibility, channelArn }) {
        await loadIVSPlayer();

        if (!window.IVSPlayer.isPlayerSupported()) {
            this.renderError("Your browser does not support live playback.");
            return;
        }

        this.root.innerHTML = "";
        this.video = document.createElement("video");
        this.video.autoplay = true;
        this.video.playsInline = true;
        this.video.controls = true;

        this.root.appendChild(this.video);

        this.player = window.IVSPlayer.create();
        this.player.attachHTMLVideoElement(this.video);

        if (visibility !== "public") {
            const getToken = httpsCallable(
                this.functions,
                "generatePlaybackToken"
            );

            const res = await getToken({ channelArn, visibility });
            if (res.data?.token) {
                this.player.setPlaybackAuthToken(res.data.token);
            }
        }

        this.player.load(playbackUrl);
        this.player.play();
    }

    renderOffline() {
        this.destroy();
        this.root.innerHTML =
            `<div class="live-offline-banner">This stream is offline</div>`;
    }

    renderError(msg) {
        this.destroy();
        this.root.innerHTML =
            `<div class="live-error">${msg}</div>`;
    }

    destroy() {
        if (this.player) {
            this.player.delete();
            this.player = null;
        }
        if (this.video) {
            this.video.remove();
            this.video = null;
        }
        this.root.innerHTML = "";
    }
}
