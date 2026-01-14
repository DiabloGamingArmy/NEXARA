import assert from 'node:assert/strict';
import { buildMessagesUrl, buildProfileUrl } from './public/assets/js/routes.js';

assert.equal(buildMessagesUrl(), '/inbox/messages');
assert.equal(buildMessagesUrl({ conversationId: 'abc' }), '/inbox/messages/abc');
assert.equal(
    buildMessagesUrl({ conversationId: 'space name', params: { tab: 'media' } }),
    '/inbox/messages/space%20name?tab=media'
);

assert.equal(buildProfileUrl(), '/profile');
assert.equal(buildProfileUrl({ handle: 'nexera' }), '/profile?handle=nexera');
assert.equal(
    buildProfileUrl({ uid: 'user/1', params: { tab: 'posts' } }),
    '/profile/user%2F1?tab=posts'
);

console.log('routes helpers: ok');
