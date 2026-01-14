# Developer Notes

- Hosting deploys from `/public`; do not edit duplicates outside `/public`.
- All user-facing web assets (HTML, CSS, JS) that deploy to Firebase Hosting live under `/public`. Edit and test changes only in those files so the live site reflects updates, and avoid recreating copies elsewhere in the repo.
- Account notifications are generated in two ways: the client writes them via `queueAccountNotification` (create-only writes are required by Firestore rules), and the `onUserProfileUpdate` Cloud Function (see `functions/index.js`) acts as a fallback for profile changes outside the web client. Notifications include a `priority` field and open a details modal from the Inbox “Account” tab.
- Desktop navigation now uses a top app bar with a hamburger toggle, a slide-in sidebar overlay, and an icon-only quick-action bar on left-edge hover.
