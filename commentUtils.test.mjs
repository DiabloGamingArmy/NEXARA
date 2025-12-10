import assert from 'node:assert/strict';
import { normalizeReplyTarget, buildReplyRecord } from './commentUtils.js';

const comments = [
    { id: 'c1', userId: 'user1', text: 'first' },
    { id: 'c2', userId: 'user1', text: 'second' }
];

const replyToFirst = buildReplyRecord({
    text: 'reply-1',
    parentCommentId: normalizeReplyTarget(comments[0].id),
    userId: 'user2'
});

const replyToSecond = buildReplyRecord({
    text: 'reply-2',
    parentCommentId: normalizeReplyTarget(comments[1].id),
    userId: 'user2'
});

assert.equal(replyToFirst.parentCommentId, comments[0].id);
assert.equal(replyToSecond.parentCommentId, comments[1].id);
assert.notEqual(replyToFirst.parentCommentId, replyToSecond.parentCommentId);

console.log('comment reply targeting: ok');
