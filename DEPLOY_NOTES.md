# Deploy Notes

## Deploy
1) Deploy Cloud Functions:
   ```sh
   firebase deploy --only functions
   ```

2) Deploy Firestore rules:
   ```sh
   firebase deploy --only firestore:rules
   ```

3) Deploy Hosting:
   ```sh
   firebase deploy --only hosting
   ```

## Verify
- Firebase Console → Functions → confirm functions are deployed and recent invocations exist.
- Firestore → check `users/{uid}/notifications` documents appear after a like/comment.
- Firebase Functions logs:
  ```sh
  firebase functions:log
  ```
