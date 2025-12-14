# Developer Notes

- Hosting deploys from `/public`; do not edit duplicates outside `/public`.
- All user-facing web assets (HTML, CSS, JS) that deploy to Firebase Hosting live under `/public`. Edit and test changes only in those files so the live site reflects updates, and avoid recreating copies elsewhere in the repo.
