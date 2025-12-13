import {
    getFirestore,
    doc,
    updateDoc,
    increment,
    setDoc,
    serverTimestamp,
} from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";

export async function sendLike(streamId) {
    if (!streamId) return;

    const db = getFirestore();
    const streamRef = doc(db, "liveStreams", streamId);

    try {
        await updateDoc(streamRef, { likes: increment(1) });
    } catch (e) {
        await setDoc(streamRef, { likes: 1 }, { merge: true });
    }
}

export async function followStreamer(hostId, currentUser) {
    const viewer = currentUser || window.currentUser;
    if (!viewer || !viewer.uid || !hostId) return;

    const db = getFirestore();
    const followRef = doc(db, "users", viewer.uid, "following", hostId);

    await setDoc(
        followRef,
        {
            followedAt: serverTimestamp(),
        },
        { merge: true }
    );
}
