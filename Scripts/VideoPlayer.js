const IVS_SCRIPT_SRC = 'https://player.live-video.net/1.0.0/amazon-ivs-player.min.js';

function loadIVSScript() {
    if (window.IVSPlayer) return Promise.resolve(window.IVSPlayer);
    return new Promise(function (resolve, reject) {
        const existing = document.querySelector('script[data-ivs-player]');
        if (existing) {
            existing.addEventListener('load', function () { resolve(window.IVSPlayer); });
            existing.addEventListener('error', function (e) { reject(e); });
            return;
        }
        const script = document.createElement('script');
        script.src = IVS_SCRIPT_SRC;
        script.async = true;
        script.dataset.ivsPlayer = 'true';
        script.onload = function () { resolve(window.IVSPlayer); };
        script.onerror = function (e) { reject(e); };
        document.head.appendChild(script);
    });
}

export class NexeraLivePlayer {
    constructor({ playbackUrl, visibility, title, ownerName, avatarUrl }) {
        this.playbackUrl = playbackUrl;
        this.visibility = visibility;
        this.title = title;
        this.ownerName = ownerName;
        this.avatarUrl = avatarUrl;
        this.player = null;
        this.videoElement = null;
        this.root = document.getElementById('live-player-root');
        this.IVS = null;
    }

    async load() {
        if (!this.root) return;
        this.root.style.display = 'block';
        this.root.innerHTML = '';
        const shell = document.createElement('div');
        shell.className = 'live-player-shell';
        shell.innerHTML = `
            <div class="live-video-frame"></div>
            <div class="live-interaction-bar"></div>
            <div class="live-chat-panel"></div>
        `;
        this.root.appendChild(shell);

        const frame = shell.querySelector('.live-video-frame');
        if (!frame) return;

        try {
            this.IVS = await loadIVSScript();
        } catch (error) {
            console.error('IVS script failed to load', error);
            this.buildFallbackVideo(frame);
            return;
        }

        if (!this.IVS || !this.IVS.isPlayerSupported || !this.IVS.isPlayerSupported()) {
            this.buildFallbackVideo(frame);
            return;
        }

        this.videoElement = document.createElement('video');
        this.videoElement.setAttribute('playsinline', '');
        this.videoElement.setAttribute('muted', '');
        this.videoElement.autoplay = true;
        this.videoElement.controls = true;
        frame.appendChild(this.videoElement);

        this.player = this.IVS.create();
        this.player.attachHTMLVideoElement(this.videoElement);
        if (this.player.setLiveLowLatency) {
            this.player.setLiveLowLatency(true);
        }
        if (typeof this.player.setAutoQualityMode === 'function') {
            this.player.setAutoQualityMode(true);
        }
        if (typeof this.player.setVolume === 'function') {
            this.player.setVolume(0.5);
        }
        this.player.load(this.playbackUrl);
        if (typeof this.player.play === 'function') {
            this.player.play();
        }
    }

    buildFallbackVideo(frame) {
        this.videoElement = document.createElement('video');
        this.videoElement.setAttribute('playsinline', '');
        this.videoElement.controls = true;
        this.videoElement.src = this.playbackUrl || '';
        frame.appendChild(this.videoElement);
        if (this.videoElement.play) {
            this.videoElement.play().catch(function () { /* ignore autoplay errors */ });
        }
    }

    togglePlay() {
        if (this.player && this.IVS && this.player.getState) {
            const state = this.player.getState();
            const playing = this.IVS.PlayerState && state === this.IVS.PlayerState.PLAYING;
            if (playing && this.player.pause) {
                this.player.pause();
            } else if (this.player.play) {
                this.player.play();
            }
            return;
        }

        if (this.videoElement) {
            if (this.videoElement.paused) {
                this.videoElement.play();
            } else {
                this.videoElement.pause();
            }
        }
    }

    setVolume(value = 1) {
        const volume = Math.max(0, Math.min(1, value));
        if (this.player && typeof this.player.setVolume === 'function') {
            this.player.setVolume(volume);
            return;
        }
        if (this.videoElement) {
            this.videoElement.volume = volume;
        }
    }

    destroy() {
        if (this.player && typeof this.player.delete === 'function') {
            this.player.delete();
        }
        if (this.videoElement && this.videoElement.parentElement) {
            this.videoElement.parentElement.removeChild(this.videoElement);
        }
        if (this.root) {
            this.root.innerHTML = '';
            this.root.style.display = 'none';
        }
        this.player = null;
        this.videoElement = null;
    }
}

export function initialize() {}
export function teardown() {}
