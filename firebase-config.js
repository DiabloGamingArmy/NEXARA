import { initializeApp } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-app.js";
import { getAuth, signInWithEmailAndPassword, createUserWithEmailAndPassword, signInAnonymously, onAuthStateChanged, signOut, updateProfile } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-auth.js";
import { getFirestore, collection, addDoc, onSnapshot, query, orderBy, serverTimestamp, doc, setDoc, getDoc, updateDoc, deleteDoc, arrayUnion, arrayRemove, increment, where, getDocs, collectionGroup, limit, startAt, endAt, Timestamp } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";
import { getStorage, ref, uploadBytes, uploadBytesResumable, getDownloadURL, deleteObject } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-storage.js";
import { normalizeReplyTarget, buildReplyRecord, groupCommentsByParent } from "./commentUtils.js";

// --- Firebase Configuration --- 
const firebaseConfig = {
    apiKey: "AIzaSyDg9Duz3xicI3pvvOtLCrV1DJRWDI0NtYA",
    authDomain: "spike-streaming-service.firebaseapp.com",
    projectId: "spike-streaming-service",
    storageBucket: "spike-streaming-service.firebasestorage.app",
    messagingSenderId: "592955741032",
    appId: "1:592955741032:web:dbd629cc957b67fc69bcdd",
    measurementId: "G-BF3GFFY3D6"
};

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const db = getFirestore(app);
const storage = getStorage(app);

export { app, auth, db, storage };
