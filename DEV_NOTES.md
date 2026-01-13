# Developer Notes

- Hosting deploys from `/public`; do not edit duplicates outside `/public`.
- All user-facing web assets (HTML, CSS, JS) that deploy to Firebase Hosting live under `/public`. Edit and test changes only in those files so the live site reflects updates, and avoid recreating copies elsewhere in the repo.
- Account notifications are generated in two ways: the client writes them via `queueAccountNotification` (create-only writes are required by Firestore rules), and the `onUserProfileUpdate` Cloud Function (see `functions/index.js`) acts as a fallback for profile changes outside the web client. Notifications display in the Inbox “Account” tab.
