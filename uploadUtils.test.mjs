import assert from 'node:assert/strict';
import {
    buildChatMediaPath,
    sanitizeFileName,
    validateChatAttachment,
    CHAT_IMAGE_MAX_BYTES
} from './public/assets/js/upload-utils.js';

const fakeFile = (overrides = {}) => ({
    name: 'My File (1).png',
    size: 1024,
    type: 'image/png',
    ...overrides
});

assert.equal(sanitizeFileName('My File (1).png'), 'My_File_1.png');
assert.ok(buildChatMediaPath({
    conversationId: 'convo123',
    messageId: 'msg456',
    timestamp: 1700000000000,
    filename: 'My File (1).png'
}).includes('chats/convo123/messages/msg456/1700000000000_My_File_1.png'));

assert.deepEqual(validateChatAttachment(fakeFile(), { maxBytes: CHAT_IMAGE_MAX_BYTES }), { ok: true });
assert.equal(
    validateChatAttachment(fakeFile({ type: 'application/pdf' }), { maxBytes: CHAT_IMAGE_MAX_BYTES }).ok,
    false
);
assert.equal(
    validateChatAttachment(fakeFile({ size: CHAT_IMAGE_MAX_BYTES + 1 }), { maxBytes: CHAT_IMAGE_MAX_BYTES }).ok,
    false
);

console.log('upload utils: ok');
