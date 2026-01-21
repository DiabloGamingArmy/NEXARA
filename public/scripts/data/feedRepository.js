import { collection, getDocs, limit, orderBy, query, startAfter, where } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";
import { db } from "../core/firebase.js";

export function createFeedRepository({ pageSize = 10 } = {}) {
    let lastDoc = null;
    let done = false;

    async function fetchNext({ limit: pageLimit = pageSize } = {}) {
        if (done) return { items: [], lastDoc, done: true };
        const constraints = [
            where('visibility', '==', 'public'),
            where('moderation.status', '==', 'approved'),
            orderBy('createdAt', 'desc'),
            limit(pageLimit)
        ];
        if (lastDoc) {
            constraints.splice(3, 0, startAfter(lastDoc));
        }
        const snapshot = await getDocs(query(collection(db, 'posts'), ...constraints));
        lastDoc = snapshot.docs[snapshot.docs.length - 1] || lastDoc;
        if (snapshot.docs.length < pageLimit) {
            done = true;
        }
        return { items: snapshot.docs, lastDoc, done };
    }

    function reset() {
        lastDoc = null;
        done = false;
    }

    return { fetchNext, reset };
}
