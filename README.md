# Nexera

## Local development

```bash
npm install
npm run build:css
npm run dev
```

## Build

```bash
npm run build
```

## Firebase configuration

* Client config is embedded in `public/scripts/app.js`.
* Supporting Firebase module layout lives under `public/scripts/`.

## Push notifications (FCM Web Push)

* Set the VAPID key in the client by defining `window.NEXERA_FCM_VAPID_KEY` (Firebase Console → Cloud Messaging → Web Push certificates).
  * Example: add a small inline script in your hosting HTML before `app.js` loads, or inject via your hosting environment.

Local test flow:
1) Enable notifications via Profile & Settings → Notifications.
2) Confirm a token doc appears at `users/{uid}/pushTokens/{tokenDocId}`.
3) Send a DM from another account and verify the in-app badge updates and a push appears (when the tab is backgrounded).

## Rules and deployment

* Firestore rules: `firestore.rules`
* Firestore indexes: `firestore.indexes.json`
* Storage rules: `storage.rules`

Deploy (examples):

```bash
firebase deploy --only firestore:rules
firebase deploy --only storage
firebase deploy --only hosting
```

## Common pitfalls

* Storage URLs should be resolved via the Firebase Storage SDK when possible; token URLs can expire.
* Permission-denied errors often indicate that Firestore/Storage rules need to be updated or that the user is not authenticated.
* Tailwind output is committed in `public/assets/tailwind.css`; regenerate it with `npm run build:css` after utility changes.
