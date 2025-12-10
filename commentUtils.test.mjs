import assert from 'node:assert/strict';
import { normalizeReplyTarget, buildReplyRecord, groupCommentsByParent } from './commentUtils.js';

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

const grouped = groupCommentsByParent([...comments, { id: 'r1', ...replyToFirst }, { id: 'r2', ...replyToSecond }]);
assert.equal(grouped.roots.length, 2);
assert.equal(grouped.byParent[comments[0].id][0].id, 'r1');
assert.equal(grouped.byParent[comments[1].id][0].id, 'r2');
assert.ok(!grouped.byParent[comments[0].id].find(r => r.id === 'r2'));

console.log('comment reply targeting: ok');
