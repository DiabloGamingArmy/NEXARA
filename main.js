// Import all your new files to make sure they run
import './firebase-config.js';
import './state.js';
import './utils.js';
import './auth.js';
import './data.js';
import './navigation.js';
import './feed.js';
import './composer.js';
import './profile.js';
import './thread.js';
import './discover.js';
import './media.js';
import './messaging.js';
import './admin.js';

// DOM Ready Listener (From your original lines 3287-3294)
document.addEventListener('DOMContentLoaded', function() {
    // We need to make sure bindMobileNav is available on window or imported
    if(window.bindMobileNav) window.bindMobileNav();
    if(window.syncMobileComposerState) window.syncMobileComposerState();
    
    const title = document.getElementById('postTitle');
    const content = document.getElementById('postContent');
    
    // Ensure syncPostButtonState is available globally or imported
    if (title && window.syncPostButtonState) title.addEventListener('input', window.syncPostButtonState);
    if (content && window.syncPostButtonState) content.addEventListener('input', window.syncPostButtonState);
    
    if(window.initializeNexeraApp) window.initializeNexeraApp();
});
