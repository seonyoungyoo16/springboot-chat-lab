import assert from 'node:assert/strict'
import {ChatSendQueue} from '../src/main/resources/static/chat-queue.js'

for(const mode of ['direct','kafka']){
  let pending=[1,2,3].map(n=>({roomId:'lobby',requestId:mode+'-original-'+n,body:'message '+n,mode,cancelable:true}))
  const original=structuredClone(pending);original[0].cancelable=false;const queue=new ChatSendQueue(),sent=[]
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

for(const mode of ['direct','kafka']){
  const queue=new ChatSendQueue();let pending=[1,2,3,4].map(n=>({roomId:'lobby',requestId:mode+'-cancel-'+n,body:'message '+n,mode,cancelable:true}))
  const cancelled=pending[1].requestId;
  // Cancel while DB is down, persist survivors, and retain the exact remaining send order.
  pending=queue.cancel(pending,'lobby',cancelled);pending=JSON.parse(JSON.stringify(pending))
  assert.deepEqual(pending.map(p=>p.body),['message 1','message 3','message 4'])
  assert.equal(queue.cancel(pending,'lobby',cancelled),pending)
  assert.equal(queue.cancel(pending,'design',pending[0].requestId),pending)
  const sent=[];let head=queue.next(pending,'lobby');sent.push(head.requestId)
  assert.equal(queue.canCancel(head),false);assert.equal(queue.cancel(pending,'lobby',head.requestId),pending)
  queue.reset();pending=JSON.parse(JSON.stringify(pending))
  assert.equal(queue.canCancel(pending[0]),false) // ACK may be lost; reload does not authorize cancellation.
  head=queue.next(pending,'lobby');pending=queue.acknowledge(pending,head)
  while(pending.length){head=queue.next(pending,'lobby');sent.push(head.requestId);pending=queue.acknowledge(pending,head)}
  assert.deepEqual(sent,[1,3,4].map(n=>mode+'-cancel-'+n));assert.ok(!sent.includes(cancelled))
}
{
  const queue=new ChatSendQueue();let pending=[{roomId:'lobby',requestId:'confirmed-failure',body:'failure',cancelable:true}]
  queue.next(pending,'lobby');queue.failed(pending,'unrelated');assert.equal(queue.canCancel(pending[0]),false)
  queue.failed(pending,'confirmed-failure');assert.equal(queue.canCancel(pending[0]),true)
  pending=queue.cancel(pending,'lobby','confirmed-failure');assert.equal(queue.next(pending,'lobby'),null)
  assert.equal(queue.canCancel({roomId:'lobby',requestId:'legacy-unknown'}),false)
}
console.log('Cancellation: middle/head/all requests, confirmed failure, original survivor order, durable exclusion, in-flight/ACK-loss and legacy guards passed.')

for(const mode of ['direct','kafka']){
  const queue=new ChatSendQueue();let pending=[1,2,3].map(n=>({roomId:'lobby',requestId:mode+'-resend-'+n,body:'message '+n,mode,cancelable:true}))
  const canceled=pending[1];pending=queue.cancel(pending,'lobby',canceled.requestId)
  const active=queue.next(pending,'lobby'),before=structuredClone(pending)
  const restored=queue.requeue(pending,{...canceled,cut:true},mode+'-fresh-4')
  assert.deepEqual(pending,before);assert.deepEqual(restored.pending.map(p=>p.requestId),[before[0].requestId,before[1].requestId,mode+'-fresh-4'])
  assert.equal(queue.inFlight.requestId,active.requestId);assert.equal(queue.next(restored.pending,'lobby'),null)
  assert.equal(restored.request.body,canceled.body);assert.equal(restored.request.roomId,canceled.roomId);assert.equal(restored.request.mode,mode)
  assert.equal(restored.request.requeuedFrom,canceled.requestId);assert.equal(restored.request.cancelable,true);assert.equal(restored.request.cut,undefined)
  assert.throws(()=>queue.requeue(restored.pending,canceled,mode+'-duplicate-5'))
  for(const bad of [canceled.requestId,pending[0].requestId,'invalid id',undefined])assert.throws(()=>queue.requeue(pending,canceled,bad))
  canceled.requeuedAs=restored.request.requestId;assert.throws(()=>queue.requeue([],canceled,mode+'-again-5'))
  pending=JSON.parse(JSON.stringify(restored.pending));queue.reset();const sent=[]
  while(pending.length){const request=queue.next(pending,'lobby');sent.push(request.requestId);pending=queue.acknowledge(pending,request)}
  assert.deepEqual(sent,[before[0].requestId,before[1].requestId,mode+'-fresh-4']);assert.ok(!sent.includes(canceled.requestId))
  // A requeued request can itself be canceled and requeued as a distinct new request.
  const second=queue.requeue([],restored.request,mode+'-fresh-6');assert.equal(second.request.requeuedFrom,restored.request.requestId)
}
console.log('Requeue canceled: fresh ID, append-only order, original payload/mode, single use, active-send isolation, reload, ID collision guards and repeated cancellation passed.')

for(const mode of ['direct','kafka']){
  const queue=new ChatSendQueue(),requests=[1,2,3].map(n=>({roomId:'design',requestId:'edit-'+mode+'-'+n,body:'original '+n,mode,cancelable:true}))
  const canceled=requests[1],pending=queue.cancel(requests,'design',canceled.requestId),active=queue.next(pending,'design'),snapshot=JSON.stringify({pending,canceled})
  for(const invalid of ['', ' \n\t ', '가'.repeat(2001), null]){
    assert.throws(()=>queue.requeue(pending,canceled,'edit-'+mode+'-invalid',invalid))
    assert.equal(JSON.stringify({pending,canceled}),snapshot);assert.equal(queue.inFlight.requestId,active.requestId)
  }
  const edited='수정한 메시지\n줄바꿈 유지',restored=queue.requeue(pending,canceled,'edit-'+mode+'-new','  '+edited+'  ')
  assert.equal(JSON.stringify({pending,canceled}),snapshot);assert.equal(canceled.body,'original 2')
  assert.deepEqual(restored.pending.slice(0,-1),pending);assert.equal(restored.request.body,edited);assert.equal(restored.request.mode,mode)
  assert.equal(restored.request.requeuedFrom,canceled.requestId);assert.equal(queue.inFlight.requestId,active.requestId)
  queue.reset();let reloaded=JSON.parse(JSON.stringify(restored.pending));const sent=[]
  while(reloaded.length){const request=queue.next(reloaded,'design');sent.push(request);reloaded=queue.acknowledge(reloaded,request)}
  assert.deepEqual(sent.map(p=>p.requestId),[requests[0].requestId,requests[2].requestId,restored.request.requestId]);assert.deepEqual(sent.map(p=>p.body),['original 1','original 3',edited])
  assert.equal(queue.requeue([],canceled,'edit-'+mode+'-boundary','가'.repeat(2000)).request.body.length,2000)
}
console.log('Edited canceled resend: invalid drafts leave queue/in-flight untouched, multiline body and fresh ID survive reload, and survivors retain their order in both modes.')
