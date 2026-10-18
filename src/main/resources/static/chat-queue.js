// One request per room is in flight. Never advance on broadcast/history, only its matching ACK.
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
