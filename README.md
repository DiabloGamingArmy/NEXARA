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
