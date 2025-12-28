/* global importScripts, firebase */
importScripts('https://www.gstatic.com/firebasejs/11.6.1/firebase-app-compat.js');
importScripts('https://www.gstatic.com/firebasejs/11.6.1/firebase-messaging-compat.js');

const firebaseConfig = {
  apiKey: "AIzaSyDg9Duz3xicI3pvvOtLCrV1DJRWDI0NtYA",
  authDomain: "spike-streaming-service.firebaseapp.com",
  projectId: "spike-streaming-service",
  storageBucket: "spike-streaming-service.firebasestorage.app",
  messagingSenderId: "592955741032",
  appId: "1:592955741032:web:dbd629cc957b67fc69bcdd",
  measurementId: "G-BF3GFFY3D6",
};

firebase.initializeApp(firebaseConfig);
const messaging = firebase.messaging();

messaging.onBackgroundMessage((payload) => {
  const title = payload?.notification?.title || "New message";
  const body = payload?.notification?.body || "You have a new message.";
  const data = payload?.data || {};
  self.registration.showNotification(title, {
    body,
    data,
  });
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const data = event.notification?.data || {};
  const conversationId = data.conversationId || '';
  const targetUrl = conversationId ? `/inbox/messages/${conversationId}` : '/inbox';
  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clientList) => {
      for (const client of clientList) {
        if (client.url.includes(targetUrl) && 'focus' in client) {
          return client.focus();
        }
      }
      if (clients.openWindow) return clients.openWindow(targetUrl);
      return null;
    })
  );
});
