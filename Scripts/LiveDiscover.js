// Scripts/LiveDiscover.js
// Live Discover Page Controller

import {
  getFirestore,
  collection,
  query,
  where,
  orderBy,
  onSnapshot
} from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";

export function initializeLiveDiscover() {
  const root = document.getElementById("live-discover-root");
  if (!root) return;

  const db = getFirestore();

  const q = query(
    collection(db, "liveStreams"),
    where("isLive", "==", true),
    where("visibility", "==", "public"),
    orderBy("startedAt", "desc")
  );

  onSnapshot(q, snapshot => {
    root.innerHTML = "";

    if (snapshot.empty) {
      root.innerHTML = `<div class="no-live-streams">No one is live right now</div>`;
      return;
    }

    snapshot.forEach(docSnap => {
      const data = docSnap.data();
      const card = document.createElement("div");
      card.className = "live-card";
      card.innerHTML = `
        <div class="live-badge">LIVE</div>
        <div class="live-title">${data.title || "Live Stream"}</div>
        <div class="live-meta">
          ${data.category || "Uncategorized"}
        </div>
      `;

      card.onclick = () => {
        window.navigateTo(`live/${docSnap.id}`);
      };

      root.appendChild(card);
    });
  });
}
