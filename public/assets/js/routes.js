const DEFAULT_MESSAGES_BASE = '/inbox/messages';
const DEFAULT_PROFILE_BASE = '/profile';

function buildQueryString(params = {}) {
  const search = new URLSearchParams();
  Object.entries(params || {}).forEach(([key, value]) => {
    if (value === undefined || value === null || value === '') return;
    search.set(key, String(value));
  });
  const suffix = search.toString();
  return suffix ? `?${suffix}` : '';
}

export function buildMessagesUrl({ conversationId = null, params = {} } = {}) {
  const suffix = buildQueryString(params);
  if (conversationId) {
    return `${DEFAULT_MESSAGES_BASE}/${encodeURIComponent(conversationId)}${suffix}`;
  }
  return `${DEFAULT_MESSAGES_BASE}${suffix}`;
}

export function buildProfileUrl({ uid = null, handle = null, params = {} } = {}) {
  const safeHandle = handle || null;
  const suffix = buildQueryString({ ...params, ...(safeHandle ? { handle: safeHandle } : {}) });
  if (uid) {
    return `${DEFAULT_PROFILE_BASE}/${encodeURIComponent(uid)}${suffix}`;
  }
  return `${DEFAULT_PROFILE_BASE}${suffix}`;
}
