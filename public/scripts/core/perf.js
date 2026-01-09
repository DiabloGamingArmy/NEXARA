import { trace as perfTrace } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-performance.js";
import { perf } from "./firebase.js";

export function startTrace(name) {
    if (!perf || !name) return null;
    try {
        const t = perfTrace(perf, name);
        t.start();
        return t;
    } catch (err) {
        return null;
    }
}

export function stopTrace(traceHandle, { error } = {}) {
    if (!traceHandle) return;
    try {
        if (error) traceHandle.putAttribute('error', error);
        traceHandle.stop();
    } catch (err) {
        // swallow
    }
}
