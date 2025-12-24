// scripts/LiveDiscover.js
// Live Discover Page Controller

import {
  getFirestore,
  collection,
  query,
  where,
} from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";
import { safeOnSnapshot } from "/scripts/firestoreSafe.js";

export function initializeLiveDiscover() {
  const root = document.getElementById("live-discover-root");
  if (!root) return;

  const db = getFirestore();

  const q = query(
    collection(db, "liveStreams"),
    where("isLive", "==", true),
    where("visibility", "==", "public")
  );

  safeOnSnapshot(
    "live:discover",
    q,
    snapshot => {
      try {
        root.innerHTML = "";

        if (snapshot.empty) {
          root.innerHTML = `<div class="no-live-streams">No one is live right now</div>`;
          return;
        }

        const orderedDocs = snapshot.docs.slice().sort((a, b) => {
          const normalize = value => {
            if (value && typeof value.toMillis === "function") {
              return value.toMillis();
            }
            if (typeof value === "number") return value;
            return 0;
          };

          const aDate = normalize(a.data()?.startedAt);
          const bDate = normalize(b.data()?.startedAt);
          return bDate - aDate;
        });

        orderedDocs.forEach(docSnap => {
          const data = docSnap.data() || {};
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
      } catch (err) {
        console.error("Live discover snapshot handling failed", err);
      }
    },
    error => {
      console.error("Live discover snapshot error", error);
      root.innerHTML = `<div class="no-live-streams">Unable to load live streams</div>`;
    }
  );
}
