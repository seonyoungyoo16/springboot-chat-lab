import assert from 'node:assert/strict'
import {ChatSendQueue} from '../src/main/resources/static/chat-queue.js'

for(const mode of ['direct','kafka']){
  let pending=[1,2,3].map(n=>({roomId:'lobby',requestId:mode+'-original-'+n,body:'message '+n,mode}))
  const original=structuredClone(pending),queue=new ChatSendQueue(),sent=[]
  const first=queue.next(pending,'lobby');sent.push(first.requestId)
  // Adding more requests, a broadcast or an unrelated ACK cannot send the tail.
  pending.push({roomId:'lobby',requestId:mode+'-original-4',body:'message 4',mode})
  assert.equal(queue.next(pending,'lobby'),null)
  assert.equal(queue.acknowledge(pending,{roomId:'lobby',requestId:pending[1].requestId}),pending)
  assert.equal(queue.acknowledge(pending,{roomId:'design',requestId:first.requestId}),pending)
  // Failed DB write / socket close retains exactly the same head after reload.
  queue.reset();pending=JSON.parse(JSON.stringify(pending));assert.deepEqual(pending.slice(0,3),original)
  const retried=queue.next(pending,'lobby');assert.equal(retried.requestId,first.requestId)
  pending=queue.acknowledge(pending,retried)
  for(let n=2;n<=4;n++){
    const request=queue.next(pending,'lobby');sent.push(request.requestId)
    assert.equal(request.requestId,mode+'-original-'+n);assert.equal(request.mode,mode)
    assert.equal(queue.next(pending,'lobby'),null)
    pending=queue.acknowledge(pending,request)
  }
  assert.equal(pending.length,0);assert.equal(queue.next(pending,'lobby'),null)
  assert.equal(new Set(sent).size,4)
}
const rooms=new ChatSendQueue(),pending=[{roomId:'design',requestId:'other-room'},{roomId:'lobby',requestId:'current-room'}]
assert.equal(rooms.next(pending,'lobby').requestId,'current-room')
console.log('Client FIFO: single flight, matching ACK only, enqueue during send, original IDs/modes, failure/reload and room isolation passed.')
