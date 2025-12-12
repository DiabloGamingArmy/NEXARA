const IVS_PLAYER_SRC = 'https://player.live-video.net/1.16.0/amazon-ivs-player.min.js';

async function loadIVS() {
    if (window.IVSPlayer) return window.IVSPlayer;
    await new Promise((resolve, reject) => {
        const script = document.createElement('script');
        script.src = IVS_PLAYER_SRC;
        script.onload = () => resolve();
        script.onerror = (err) => reject(err);
        document.head.appendChild(script);
    });
    return window.IVSPlayer;
}

export class NexeraVideoPlayer {
    constructor() {
        this.player = null;
        this.container = null;
    }

    async mount(container) {
        if (!container) return;
        this.container = container;
        if (this.player) return;
        const IVSPlayer = await loadIVS();
        if (!IVSPlayer.isPlayerSupported()) return;
        this.player = IVSPlayer.create();
        const videoEl = document.createElement('video');
        videoEl.className = 'live-video';
        videoEl.setAttribute('playsinline', 'true');
        videoEl.setAttribute('muted', 'true');
        videoEl.autoplay = true;
        this.container.innerHTML = '';
        this.container.appendChild(videoEl);
        this.player.attachHTMLVideoElement(videoEl);
    }

    async play(playbackUrl) {
        if (!this.player) return;
        await this.player.load(playbackUrl);
        this.player.play();
    }

    destroy() {
        if (this.player) {
            this.player.pause();
            this.player.delete();
            this.player = null;
        }
        if (this.container) {
            this.container.innerHTML = '';
        }
    }
}
