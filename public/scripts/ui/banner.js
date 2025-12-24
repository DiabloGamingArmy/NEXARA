import { doc } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";
import { safeOnSnapshot } from "/scripts/firestoreSafe.js";

const BANNER_ID = 'nexera-alert-banner';
const DISMISS_KEY = 'nexeraBannerDismissedVersion';

function getIconClass(type = 0) {
    const map = {
        0: 'ph-info',
        1: 'ph-check-circle',
        2: 'ph-warning',
        3: 'ph-x-circle',
        4: 'ph-megaphone'
    };
    return map[type] || 'ph-info';
}

function ensureBannerElement() {
    let banner = document.getElementById(BANNER_ID);
    if (banner) return banner;
    banner = document.createElement('div');
    banner.id = BANNER_ID;
    banner.className = 'nexera-alert-banner banner-overlay';
    banner.innerHTML = `
        <div class="banner-content">
            <i class="ph banner-icon"></i>
            <span class="banner-text"></span>
        </div>
        <button class="banner-close" type="button" aria-label="Dismiss banner">
            <i class="ph ph-x"></i>
        </button>
    `;
    document.body.prepend(banner);
    return banner;
}

function hideBanner(banner) {
    if (!banner) return;
    banner.style.display = 'none';
}

function showBanner(banner, { text, color, type, dismissible, version }) {
    if (!banner) return;
    const icon = banner.querySelector('.banner-icon');
    const textEl = banner.querySelector('.banner-text');
    const closeBtn = banner.querySelector('.banner-close');
    if (icon) icon.className = `ph banner-icon ${getIconClass(type)}`;
    if (textEl) textEl.textContent = text || '';
    banner.style.background = color || 'var(--bg-card)';
    banner.classList.toggle('banner-overlay', !!dismissible);
    banner.classList.toggle('banner-static', !dismissible);
    if (closeBtn) {
        closeBtn.style.display = dismissible ? 'inline-flex' : 'none';
        closeBtn.onclick = function () {
            if (!dismissible) return;
            localStorage.setItem(DISMISS_KEY, String(version || ''));
            hideBanner(banner);
        };
    }
    banner.style.display = 'flex';
}

function shouldDismiss(version) {
    if (version === null || version === undefined) return false;
    return localStorage.getItem(DISMISS_KEY) === String(version);
}

async function waitForDb() {
    if (window.Nexera?.db) return window.Nexera.db;
    return new Promise(function (resolve) {
        const started = Date.now();
        const timer = setInterval(function () {
            if (window.Nexera?.db) {
                clearInterval(timer);
                resolve(window.Nexera.db);
            }
            if (Date.now() - started > 5000) {
                clearInterval(timer);
                resolve(null);
            }
        }, 50);
    });
}

async function initBanner() {
    const db = await waitForDb();
    if (!db) return;
    const banner = ensureBannerElement();
    const configRef = doc(db, 'app_config', 'ui');
    safeOnSnapshot('app_config:ui', configRef, function (snap) {
        if (!snap.exists()) {
            hideBanner(banner);
            return;
        }
        const data = snap.data() || {};
        const active = !!data.bannerActive;
        const dismissible = !!data.bannerCloseOption;
        const version = data.bannerVersion ?? '';

        if (!active) {
            hideBanner(banner);
            return;
        }

        if (dismissible && shouldDismiss(version)) {
            hideBanner(banner);
            return;
        }

        showBanner(banner, {
            text: data.bannerText || '',
            color: data.bannerColor || 'var(--bg-card)',
            type: Number(data.bannerType || 0),
            dismissible,
            version
        });
    }, function (error) {
        console.warn('[Banner] Unable to load banner config', error);
        hideBanner(banner);
    });
}

initBanner();
