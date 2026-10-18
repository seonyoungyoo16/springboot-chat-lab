// One request per room is in flight. Never advance on broadcast/history, only its matching ACK.
export function prepareRequeueBody(editedBody) {
  if (typeof editedBody !== 'string' || !editedBody.trim() || editedBody.trim().length > 2000) throw new Error('메시지는 1~2000자입니다.');
  return editedBody.trim();
}
export class ChatSendQueue {
  inFlight = null;
  next(pending, room) {
    if (this.inFlight) return null;
    const request = pending.find(p => p.roomId === room);
    if (!request) return null;
    request.cancelable = false; // Persist before transport: ACK loss must never look safe to cancel.
    this.inFlight = {roomId: request.roomId, requestId: request.requestId};
    return request;
  }
  canCancel(request) {
    return request.cancelable === true && !(this.inFlight?.roomId === request.roomId && this.inFlight?.requestId === request.requestId);
  }
  cancel(pending, roomId, requestId) {
    const request = pending.find(p => p.roomId === roomId && p.requestId === requestId);
    if (!request || !this.canCancel(request)) return pending;
    return pending.filter(p => p !== request); // Cancellation preserves the original order of the survivors.
  }
  requeue(pending, canceled, newRequestId, editedBody = canceled.body) {
    if (canceled.requeuedAs || pending.some(p => p.roomId === canceled.roomId && p.requeuedFrom === canceled.requestId)) throw new Error('이미 대기열에 다시 추가한 요청입니다.');
    if (typeof newRequestId !== 'string' || !/^[A-Za-z0-9_-]{1,80}$/.test(newRequestId) || newRequestId === canceled.requestId || pending.some(p => p.requestId === newRequestId)) throw new Error('새 요청 ID가 필요합니다.');
    const body = prepareRequeueBody(editedBody);
    const request = {roomId:canceled.roomId, requestId:newRequestId, body, mode:canceled.mode || 'direct', cancelable:true, requeuedFrom:canceled.requestId};
    return {pending:[...pending,request],request}; // Append a fresh request at the tail; never restore the old position.
  }
  failed(pending, requestId) {
    if (this.inFlight?.requestId !== requestId) return;
    const request = pending.find(p => p.roomId === this.inFlight.roomId && p.requestId === requestId);
    if (request) request.cancelable = true; // Explicit DB rollback response confirms it was not stored.
    this.reset();
  }
  acknowledge(pending, message) {
    if (!this.inFlight || this.inFlight.roomId !== message.roomId || this.inFlight.requestId !== message.requestId) return pending;
    this.inFlight = null;
    return pending.filter(p => p.roomId !== message.roomId || p.requestId !== message.requestId);
  }
  reset() { this.inFlight = null; }
}
