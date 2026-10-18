// One request per room is in flight. Never advance on broadcast/history, only its matching ACK.
export class ChatSendQueue {
  inFlight = null;
  next(pending, room) {
    if (this.inFlight) return null;
    const request = pending.find(p => p.roomId === room);
    if (!request) return null;
    this.inFlight = {roomId: request.roomId, requestId: request.requestId};
    return request;
  }
  acknowledge(pending, message) {
    if (!this.inFlight || this.inFlight.roomId !== message.roomId || this.inFlight.requestId !== message.requestId) return pending;
    this.inFlight = null;
    return pending.filter(p => p.roomId !== message.roomId || p.requestId !== message.requestId);
  }
  reset() { this.inFlight = null; }
}
