/**
 * UI Component: VideoViewerPanel
 * Adds three-column viewer scaffolding with controls, up-next list, and comments placeholders.
 */
export function buildVideoViewerLayout() {
    const wrapper = document.createElement('div');
    wrapper.className = 'video-viewer-shell';
    wrapper.innerHTML = `
        <div class="video-viewer-main">
            <div class="video-viewer-player video-modal-player">
                <div class="video-player-frame" id="video-player-frame">
                    <video id="video-modal-player" playsinline preload="metadata"></video>
                    <div class="video-player-spinner" id="video-player-spinner" aria-live="polite">
                        <span class="splash-spinner" aria-hidden="true"></span>
                    </div>
                    <div class="video-control-overlay" id="video-control-overlay" role="group" aria-label="Video controls">
                        <div class="video-control-scrub">
                            <div class="video-control-scrub-track" aria-hidden="true">
                                <div class="video-control-scrub-buffer"></div>
                                <div class="video-control-scrub-progress"></div>
                            </div>
                            <input id="video-control-scrub" type="range" min="0" max="100" value="0" aria-label="Seek bar">
                        </div>
                        <div class="video-control-actions">
                            <div class="video-control-actions-left">
                                <button id="video-control-play" class="icon-pill" aria-label="Play or pause">
                                    <i class="ph ph-play"></i>
                                </button>
                                <div class="video-volume-group" id="video-control-volume-group">
                                    <button id="video-control-volume" class="icon-pill" aria-label="Mute or unmute">
                                        <i class="ph ph-speaker-high"></i>
                                    </button>
                                    <div class="video-volume-popover" role="group" aria-label="Volume">
                                        <input id="video-control-volume-range" type="range" min="0" max="100" value="100" aria-label="Volume">
                                    </div>
                                </div>
                            </div>
                            <div class="video-control-actions-right">
                                <div class="video-control-popover-group" data-popover="captions">
                                    <button id="video-control-captions" class="icon-pill" aria-label="Captions">
                                        <i class="ph ph-closed-captioning"></i>
                                    </button>
                                    <div class="video-control-popover video-control-captions-popover" role="dialog" aria-label="Captions">
                                        <div class="video-control-popover-title">Subtitles</div>
                                        <button class="video-control-popover-item" type="button">No subtitles available</button>
                                    </div>
                                </div>
                                <div class="video-control-popover-group" data-popover="settings">
                                    <button id="video-control-settings" class="icon-pill" aria-label="Settings">
                                        <i class="ph ph-gear"></i>
                                    </button>
                                    <div class="video-control-popover video-control-settings-popover" role="dialog" aria-label="Settings">
                                        <div class="video-control-popover-title">Playback speed</div>
                                        <div class="video-control-popover-list">
                                            <button class="video-control-popover-item" type="button">0.5×</button>
                                            <button class="video-control-popover-item" type="button">1×</button>
                                            <button class="video-control-popover-item" type="button">1.5×</button>
                                            <button class="video-control-popover-item" type="button">2×</button>
                                        </div>
                                        <div class="video-control-popover-title">Quality</div>
                                        <div class="video-control-popover-list">
                                            <button class="video-control-popover-item" type="button">480p</button>
                                            <button class="video-control-popover-item" type="button">720p</button>
                                            <button class="video-control-popover-item" type="button">1080p</button>
                                        </div>
                                    </div>
                                </div>
                                <button id="video-control-theater" class="icon-pill" aria-label="Toggle theater mode">
                                    <i class="ph ph-rectangle"></i>
                                </button>
                                <button id="video-control-fullscreen" class="icon-pill" aria-label="Fullscreen (theater)">
                                    <i class="ph ph-arrows-out"></i>
                                </button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <div class="video-viewer-meta">
                <div class="video-modal-title" id="video-modal-title"></div>
                <div class="video-modal-description" id="video-modal-description"></div>
                <div class="video-action-row" role="group" aria-label="Video actions">
                    <button id="video-modal-like" class="icon-pill" aria-label="Like video"></button>
                    <button id="video-modal-dislike" class="icon-pill" aria-label="Dislike video"></button>
                    <button id="video-modal-share" class="icon-pill" aria-label="Share video"></button>
                    <button id="video-modal-save" class="icon-pill" aria-label="Save video"></button>
                    <button class="icon-pill" aria-label="Download (coming soon)" disabled><i class="ph ph-download"></i> Download</button>
                    <button class="icon-pill" aria-label="Report video" onclick="window.handleUiStubAction?.('video-report')"><i class="ph ph-flag"></i> Report</button>
                    <button class="icon-pill" aria-label="More options" onclick="window.handleUiStubAction?.('video-more')"><i class="ph ph-dots-three"></i></button>
                    <div class="video-modal-views" id="video-modal-views"></div>
                </div>
            </div>
            <div class="video-modal-channel-card" id="video-modal-channel-card">
                <div class="video-modal-channel-header">
                    <div id="video-modal-avatar" class="video-modal-avatar"></div>
                    <div class="video-modal-channel-info">
                        <div class="video-modal-channel-name" id="video-modal-channel-name"></div>
                        <div class="video-modal-channel-handle" id="video-modal-channel-handle"></div>
                    </div>
                    <button id="video-modal-follow" class="create-btn-sidebar">Follow</button>
                </div>
                <div id="video-modal-channel-bio" class="video-modal-channel-bio"></div>
                <div id="video-modal-channel-links" class="video-modal-channel-links"></div>
            </div>
            <div class="video-viewer-comments">
                <div class="video-comments-header">
                    <div>Comments</div>
                    <select id="video-comments-sort" class="discover-select" aria-label="Sort comments">
                        <option value="top">Top</option>
                        <option value="newest">Newest</option>
                    </select>
                </div>
                <div id="video-comment-pinned" class="video-comment-pinned" aria-label="Pinned comment">
                    <strong>Pinned</strong> • Share your thoughts to start the conversation.
                </div>
                <div class="video-comment-input">
                    <input id="video-comment-input" class="form-input" placeholder="Add a comment..." aria-label="Add a comment">
                    <button class="icon-pill" aria-label="Post comment" onclick="window.handleUiStubAction?.('video-comment')"><i class="ph ph-paper-plane-right"></i></button>
                </div>
                <div id="video-comments-list" class="video-comments-list"></div>
                <button id="video-comments-load" class="icon-pill" aria-label="Load more comments" onclick="window.handleUiStubAction?.('video-comments-load')">Load more</button>
            </div>
        </div>
        <aside class="video-viewer-aside">
            <div class="video-up-next-header">
                <h3>Up Next</h3>
                <button class="icon-pill" aria-label="Refresh up next" onclick="window.handleUiStubAction?.('video-upnext-refresh')"><i class="ph ph-arrow-clockwise"></i></button>
            </div>
            <div class="video-up-next-toggle">
                <span>Autoplay next</span>
                <label class="toggle-switch" aria-label="Autoplay next">
                    <input id="video-up-next-autoplay" type="checkbox" checked>
                    <span class="toggle-slider"></span>
                </label>
            </div>
            <div id="video-up-next-list" class="video-up-next-list"></div>
        </aside>
    `;
    return wrapper;
}
