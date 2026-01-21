import { initializeApp } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-app.js";
import { getAuth } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-auth.js";
import { initializeFirestore, getFirestore } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";
import { getStorage } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-storage.js";
import { getFunctions } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-functions.js";
import { getPerformance } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-performance.js";
import { initializeAppCheck, ReCaptchaV3Provider } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-app-check.js";

const firebaseConfig = {
    apiKey: "AIzaSyDg9Duz3xicI3pvvOtLCrV1DJRWDI0NtYA",
    authDomain: "spike-streaming-service.firebaseapp.com",
    projectId: "spike-streaming-service",
    storageBucket: "spike-streaming-service.firebasestorage.app",
    messagingSenderId: "592955741032",
    appId: "1:592955741032:web:dbd629cc957b67fc69bcdd",
    measurementId: "G-BF3GFFY3D6"
};

export const app = initializeApp(firebaseConfig);
export const auth = getAuth(app);
const isSafari = /^((?!chrome|android).)*safari/i.test(navigator.userAgent);
export const db = isSafari
    ? initializeFirestore(app, { experimentalForceLongPolling: true, useFetchStreams: false })
    : getFirestore(app);
export const storage = getStorage(app);
export const functions = getFunctions(app);
export const perf = getPerformance(app);

const appCheckKey = window?.NEXERA_APP_CHECK_KEY || window?.__NEXERA_APP_CHECK_KEY || "";
if (appCheckKey) {
    try {
        initializeAppCheck(app, {
            provider: new ReCaptchaV3Provider(appCheckKey),
            isTokenAutoRefreshEnabled: true,
        });
    } catch (error) {
        console.warn("App Check init failed", error);
    }
} else {
    console.warn("App Check site key missing; set window.NEXERA_APP_CHECK_KEY to enable App Check.");
}
