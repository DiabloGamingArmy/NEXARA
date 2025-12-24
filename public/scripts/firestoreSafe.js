import { onSnapshot } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";

const loggedSnapshotErrors = new Set();

function safeSerialize(value) {
    try {
        return JSON.stringify(value);
    } catch (err) {
        return String(value);
    }
}

function describeQueryTarget(target) {
    if (!target) return { kind: 'unknown', path: null };
    if (target.path) return { kind: 'doc', path: target.path };

    const query = target._query || target;
    const rawPath = query?.path;
    let path = null;
    if (rawPath?.canonicalString) path = rawPath.canonicalString();
    if (!path && Array.isArray(rawPath?.segments)) path = rawPath.segments.join('/');
    if (!path && typeof rawPath === 'string') path = rawPath;
    if (!path && typeof target.toString === 'function') path = target.toString();

    const orderBy = (query?.explicitOrderBy || query?.orderBy || []).map(function (entry) {
        const field = entry?.field?.canonicalString ? entry.field.canonicalString() : entry?.field?.toString?.() || entry?.field?.segments?.join('.') || '';
        return `${field}:${entry?.dir || entry?.direction || 'asc'}`;
    });

    const filters = (query?.filters || []).map(function (filter) {
        if (filter?.field?.canonicalString) {
            return `${filter.field.canonicalString()} ${filter?.op || filter?.operator || ''} ${filter?.value}`;
        }
        return safeSerialize(filter);
    });

    const limit = query?.limit || null;
    const limitType = query?.limitType || null;

    return {
        kind: 'query',
        path,
        orderBy: orderBy.length ? orderBy : undefined,
        filters: filters.length ? filters : undefined,
        limit,
        limitType
    };
}

function reportSnapshotError(label, error, target) {
    const key = `${label}:${error?.code || 'unknown'}`;
    if (!loggedSnapshotErrors.has(key)) {
        loggedSnapshotErrors.add(key);
        const details = describeQueryTarget(target);
        console.error('[Firestore] onSnapshot error', {
            label,
            code: error?.code,
            message: error?.message,
            target: details
        });
        if (window?.NEXERA_DEBUG && typeof window.toast === 'function') {
            window.toast(`Feature disabled (${label})`, 'info');
        }
    }
}

export function safeOnSnapshot(label, queryOrRef, onNext, onError) {
    try {
        return onSnapshot(
            queryOrRef,
            onNext,
            function (error) {
                reportSnapshotError(label, error, queryOrRef);
                if (typeof onError === 'function') {
                    onError(error);
                }
            }
        );
    } catch (error) {
        reportSnapshotError(label, error, queryOrRef);
        if (typeof onError === 'function') {
            onError(error);
        }
        return function noop() {};
    }
}

window.safeOnSnapshot = safeOnSnapshot;
