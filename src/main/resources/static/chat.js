import {ChatSendQueue,prepareRequeueBody} from './chat-queue.js';
const sendQueue=new ChatSendQueue();
const $=id=>document.getElementById(id);
const clientId=sessionStorage.getItem('chat-client')||crypto.randomUUID();sessionStorage.setItem('chat-client',clientId);
let socket=null,wanted=false,joined=false,retry=null,attempt=0,generation=0,room='lobby',name='참가자',viewOpen=true;
let messages=new Map(),pending=[],reads=new Map(),readRequested=new Map(),cutRequest=null;
try{pending=JSON.parse(sessionStorage.getItem('chat-pending')||'[]')}catch{}
let databaseBlocked=false;const cancellations=[],requeueDrafts=new Map();let canceledRenderKey='';
let typingSent=false,lastTypingAt=0,typingIdle=null;const peerTyping=new Map();
const store=(items=pending)=>{try{sessionStorage.setItem('chat-pending',JSON.stringify(items));return true}catch{$('notice').textContent='대기 상태를 저장하지 못해 전송·취소를 보류합니다.';return false}};
function status(text){$('status').textContent=text;$('body').disabled=!joined;$('send').disabled=!joined;$('fault').disabled=!joined;$('connect').disabled=wanted;$('disconnect').disabled=!wanted;$('name').disabled=wanted;$('closed-state').textContent=joined?'온라인 · 메시지 수신 중':'오프라인 · 서버에 대화 보관 중'}
function render(follow=true){
  const top=$('messages').scrollTop,atBottom=$('messages').scrollHeight-top-$('messages').clientHeight<24;
  const rows=[...messages.values()].sort((a,b)=>a.id-b.id).slice(-100);
  $('closed-count').textContent='받은 메시지 '+rows.length+'건 · 안 읽은 메시지 '+rows.filter(m=>m.clientId!==clientId&&m.id>(reads.get(clientId+':'+(m.deliveryMode||'direct'))?.lastReadId||0)).length+'건';
  const waiting=m=>m.clientId===clientId&&pending.some(p=>p.roomId===m.roomId&&p.requestId===m.requestId);
  const elements=rows.map(m=>bubble(m,waiting(m)));for(const p of pending.filter(p=>p.roomId===room&&!rows.some(m=>m.clientId===clientId&&m.requestId===p.requestId)))elements.push(bubble({clientId,sender:name,body:p.body,requestId:p.requestId},true));
  const queued=pending.filter(p=>p.roomId===room),list=document.createElement('ol');
  for(const [index,p] of queued.entries()){
    const row=document.createElement('li'),title=document.createElement('strong'),body=document.createElement('p'),id=document.createElement('code');
    const active=sendQueue.inFlight?.requestId===p.requestId;
    row.dataset.requestId=p.requestId;row.dataset.queueState=active?'sending':'waiting';
    title.textContent=(index+1)+'. '+(active?'전송 중 · ACK 확인 대기':databaseBlocked?'DB 복구 대기':'순서 대기');body.textContent=p.body;id.textContent=p.requestId;
    const cancel=document.createElement('button');cancel.type='button';cancel.className='text-action cancel-action';
    cancel.textContent=sendQueue.canCancel(p)?'취소':'전송 결과 확인 중';cancel.setAttribute('aria-label','대기 메시지 '+(index+1)+' 취소');cancel.disabled=!sendQueue.canCancel(p);cancel.onclick=()=>cancelPending(p.roomId,p.requestId);
    row.append(title,body,id,cancel);list.append(row);
  }
  $('outbox').replaceChildren();if(queued.length){const heading=document.createElement('strong');heading.textContent='전송 대기 '+queued.length+'건 · ACK 후 다음 요청';$('outbox').append(heading,list)}
  renderCanceled();
  $('messages').replaceChildren(...elements);$('messages').scrollTop=follow&&atBottom?$('messages').scrollHeight:top;requestAnimationFrame(markVisibleRead);
}
function renderCanceled(){
  const items=cancellations.filter(p=>p.roomId===room),key=JSON.stringify([room,items.map(p=>[p.requestId,p.requeuedAs])]);
  if(key===canceledRenderKey)return; // Incoming messages/read receipts must not replace an editor in use.
  canceledRenderKey=key;
  const list=document.createElement('ol');
  for(const [index,p] of items.entries()){
    const row=document.createElement('li'),original=document.createElement('p'),id=document.createElement('code'),label=document.createElement('small'),resend=document.createElement('button');
    original.textContent='취소됨 · '+p.body;label.textContent='이전 요청 ID';id.textContent=p.requestId;id.dataset.requestRole='old';row.append(original,label,id);
    resend.className='text-action requeue-action';resend.setAttribute('aria-label','취소 메시지 '+(index+1)+' 다시 보내기');
    if(p.requeuedAs){
      const bodyLabel=document.createElement('small'),body=document.createElement('p'),nextLabel=document.createElement('small'),nextId=document.createElement('code');
      bodyLabel.textContent='다시 보낸 내용';body.className='requeued-body';body.textContent=p.requeuedBody??p.body;
      nextLabel.textContent='다시 보낸 새 ID';nextId.textContent=p.requeuedAs;nextId.dataset.requestRole='new';
      resend.type='button';resend.textContent='대기열에 다시 추가됨';resend.disabled=true;row.append(bodyLabel,body,nextLabel,nextId,resend);
    }else{
      const form=document.createElement('form'),field=document.createElement('label'),title=document.createElement('span'),input=document.createElement('textarea'),hint=document.createElement('small');
      form.className='requeue-editor';title.textContent='다시 보낼 내용';input.value=requeueDrafts.get(p.requestId)??p.body;input.maxLength=2000;input.rows=2;
      input.setAttribute('aria-label','취소 메시지 '+(index+1)+' 다시 보낼 내용');hint.id='requeue-hint-'+p.requestId;input.setAttribute('aria-describedby',hint.id);
      const update=()=>{const valid=!!input.value.trim()&&input.value.trim().length<=2000;resend.disabled=!valid;input.setAttribute('aria-invalid',String(!valid));hint.textContent=(valid?'수정한 내용은 새 ID로 맨 뒤에 추가합니다.':'메시지는 1~2000자입니다.')+' '+input.value.length.toLocaleString()+' / 2,000자'};
      input.oninput=()=>{requeueDrafts.set(p.requestId,input.value);update()};
      resend.type='submit';resend.textContent='다시 보내기';form.onsubmit=event=>{event.preventDefault();if(!resend.disabled)requeueCanceled(p.roomId,p.requestId,input.value)};
      update();field.append(title,input);form.append(field,hint,resend);row.append(form);
    }
    list.append(row);
  }
  $('cancelled').replaceChildren();if(items.length){const title=document.createElement('strong');title.textContent='취소한 요청 '+items.length+'건 · 이전 ID 재전송 제외';$('cancelled').append(title,list)}
}
function bubble(m,waiting){const div=document.createElement('article');div.className='bubble'+(m.clientId===clientId?' mine':'')+(waiting?' pending':'');const small=document.createElement('small');small.textContent=m.sender+' · '+(waiting?(m.id?'#'+m.id+' · ACK 확인 대기':'서버 확인 대기'):'#'+m.id+' · '+new Date(m.createdAt).toLocaleTimeString('ko-KR',{hour:'2-digit',minute:'2-digit'}));const p=document.createElement('p');p.textContent=m.body;div.append(small,p);if(!waiting&&m.clientId===clientId){const receipt=document.createElement('span');receipt.className='read-state';receipt.textContent=[...reads.values()].some(r=>r.clientId!==clientId&&r.roomId===room&&(r.deliveryMode||'direct')===(m.deliveryMode||'direct')&&r.lastReadId>=m.id)?'읽음':'안 읽음';div.append(receipt)}return div}
function markVisibleRead(){
  if(databaseBlocked||document.hidden||!viewOpen||!joined||socket?.readyState!==WebSocket.OPEN)return;
  const area=$('messages');if(area.scrollHeight-area.scrollTop-area.clientHeight>24)return;
  const end=area.lastElementChild?.getBoundingClientRect();if(!end||end.bottom<=0||end.bottom>innerHeight+2)return;
  for(const mode of ['direct','kafka']){const latest=Math.max(0,...[...messages.values()].filter(m=>m.clientId!==clientId&&m.roomId===room&&(m.deliveryMode||'direct')===mode).map(m=>m.id));
  if(latest<=Math.max(readRequested.get(mode)||0,reads.get(clientId+':'+mode)?.lastReadId||0))continue;
  readRequested.set(mode,latest);socket.send(JSON.stringify({type:'read',upToId:latest}));}
}
function sendTyping(active){
  if(!active){clearTimeout(typingIdle);if(!typingSent)return}
  if(!joined||socket?.readyState!==WebSocket.OPEN){typingSent=false;return}
  if(active&&(!viewOpen||document.hidden))return;
  const now=Date.now();if(active&&typingSent&&now-lastTypingAt<900)return;
  typingSent=active;lastTypingAt=now;socket.send(JSON.stringify({type:'typing',active}));
}
function renderTyping(){
  const now=Date.now();for(const [id,entry] of peerTyping)if(entry.expiresAt<=now)peerTyping.delete(id);
  $('typing-status').textContent=[...peerTyping.values()].map(entry=>entry.name+' 입력 중…').join(' · ');
}
function receiveTyping(event){
  if(event.roomId!==room||event.clientId===clientId)return;
  if(event.active){peerTyping.set(event.clientId,{name:event.name,expiresAt:Date.now()+4000});setTimeout(renderTyping,4010)}else peerTyping.delete(event.clientId);
  renderTyping();
}
function inputTyping(){clearTimeout(typingIdle);if(!$('body').value.trim()){sendTyping(false);return}sendTyping(true);typingIdle=setTimeout(()=>sendTyping(false),2000)}
function cancelPending(roomId,requestId){
  const request=pending.find(p=>p.roomId===roomId&&p.requestId===requestId),remaining=sendQueue.cancel(pending,roomId,requestId);
  if(remaining===pending||!store(remaining))return;
  pending=remaining; // Cancel before transport; this request is never sent to the server.
  cancellations.push(request);if(cancellations.length>20)cancellations.shift();if(cutRequest===requestId)cutRequest=null;
  render(false);$('notice').textContent='전송 대기 취소 · '+requestId+' · 나머지는 원래 순서대로 처리합니다.';flush();
}
function requeueCanceled(roomId,canceledId,editedBody){
  const canceled=cancellations.find(p=>p.roomId===roomId&&p.requestId===canceledId);if(!canceled||canceled.requeuedAs)return;
  try{
    const body=prepareRequeueBody(editedBody); // Validate the edited body before allocating a fresh identity.
    const requestId=crypto.randomUUID(); // New identity for a canceled message.
    const restored=sendQueue.requeue(pending,canceled,requestId,body);if(!store(restored.pending))return;
    pending=restored.pending; // Append at tail before dispatch; existing request order is unchanged.
    canceled.requeuedAs=requestId;canceled.requeuedBody=body;requeueDrafts.delete(canceled.requestId);render(false);$('notice').textContent='새 요청 ID '+requestId+' · 수정한 내용을 대기열 맨 뒤에 추가했습니다. 기존 내용·ID·순서는 유지합니다.';flush();
  }catch(error){$('notice').textContent=error.message}
}
function flush(){
  if(databaseBlocked||!joined||socket?.readyState!==WebSocket.OPEN)return;
  const p=sendQueue.next(pending,room);if(!p)return;
  if(!store()){sendQueue.reset();return;} // Persist non-cancelable state before the first network write.
  try{socket.send(JSON.stringify({type:'send',requestId:p.requestId,body:p.body,mode:p.mode||'direct'}));render(false)}catch{sendQueue.reset();socket?.close();}
}
function receiveAck(event){
  if(event.requestId!==event.message.requestId)return;
  const remaining=sendQueue.acknowledge(pending,event.message);if(remaining===pending)return;
  messages.set(event.message.id,event.message);
  pending=remaining; // Matching ACK removes only this request; preserve the tail in original order.
  store();render();flush();$('notice').textContent=event.duplicate?'재전송 ACK · 기존 메시지 #'+event.message.id+' · 추가 저장/방송 없음 · '+event.requestId:'서버 저장 확인 · #'+event.message.id;
}
function interruptBeforeAck(message){
  // Deterministic demo: receive the committed broadcast, then ignore queued ACKs from this socket.
  cutRequest=null;disconnect();
  $('notice').textContent='서버 저장 #'+message.id+' 후 ACK 수신 전에 연결을 끊었습니다. 연결하기를 누르면 같은 requestId로 자동 재전송합니다.';
}
function connect(){
  clearTimeout(retry);sendQueue.reset();const version=++generation;joined=false;readRequested=new Map();status('연결 중…');
  const ws=new WebSocket((location.protocol==='https:'?'wss:':'ws:')+'//'+location.host+'/ws/chat');socket=ws;
  ws.onopen=()=>{if(version!==generation)return;ws.send(JSON.stringify({type:'join',roomId:room,clientId,name}))};
  ws.onmessage=e=>{if(version!==generation)return;const event=JSON.parse(e.data);
    if(event.type==='joined'){reads=new Map((event.reads||[]).map(r=>[r.clientId+':'+(r.deliveryMode||'direct'),r]));joined=true;attempt=0;for(const m of event.messages)messages.set(m.id,m);status('● 연결됨 · '+room);$('notice').textContent='연결되었습니다. 최근 대화를 불러왔습니다.';render();flush()}
    else if(event.type==='message'){messages.set(event.message.id,event.message);render();if(event.message.clientId===clientId&&event.message.requestId===cutRequest)interruptBeforeAck(event.message)}
    else if(event.type==='ack'){if(event.requestId===cutRequest)interruptBeforeAck(event.message);else receiveAck(event)}
    else if(event.type==='typing')receiveTyping(event);
    else if(event.type==='read'){if(event.receipt.roomId===room){const key=event.receipt.clientId+':'+(event.receipt.deliveryMode||'direct'),previous=reads.get(key);if(!previous||previous.lastReadId<event.receipt.lastReadId)reads.set(key,event.receipt);render(false)}}
    else if(event.type==='presence')$('presence').textContent=event.names.join(', ')||'—';
    else if(event.type==='error'){
      if(event.code==='DB_UNAVAILABLE'&&event.retryable){
        databaseBlocked=true;if(event.operation==='send')sendQueue.failed(pending,event.requestId);else sendQueue.reset();store();readRequested=new Map();render();
        $('notice').textContent='DB 처리 실패 · ACK 없음 · 전송 대기 유지 · '+event.requestId;
        // Keep the joined socket open; retry this exact request only after DB recovery.
        if(event.operation==='join'){ws.close();}
      }else{$('notice').textContent=event.detail;wanted=false;joined=false;generation++;ws.close();status('입력값 확인 후 다시 연결하세요.')}
    }
  };
  ws.onclose=()=>{if(version!==generation)return;joined=false;sendQueue.reset();typingSent=false;clearTimeout(typingIdle);peerTyping.clear();renderTyping();$('presence').textContent='—';if(wanted){const delay=Math.min(10000,1000*2**attempt++);status('연결 끊김 · '+delay/1000+'초 후 재접속');retry=setTimeout(connect,delay)}else status('연결 종료')};
  ws.onerror=()=>{if(version===generation)$('notice').textContent='연결을 확인하고 있습니다.'};
}
function disconnect(){sendTyping(false);sendQueue.reset();peerTyping.clear();renderTyping();wanted=false;joined=false;generation++;clearTimeout(retry);socket?.close();socket=null;status('연결 종료');$('presence').textContent='—'}
$('connect').onclick=()=>{name=$('name').value.trim();if(!name||name.length>20){$('notice').textContent='이름은 1~20자입니다.';return}wanted=true;connect()};$('disconnect').onclick=disconnect;
$('room').onchange=()=>{const again=wanted;disconnect();room=$('room').value;messages=new Map();reads=new Map();readRequested=new Map();$('room-name').textContent=$('room').selectedOptions[0].textContent;$('notice').textContent='';render();if(again){wanted=true;connect()}};
$('compose').onsubmit=e=>{e.preventDefault();const body=$('body').value.trim();if(!joined||!body)return;sendTyping(false);const requestId=crypto.randomUUID();pending.push({roomId:room,requestId,body,mode:$('mode').value,cancelable:true});if($('fault').checked){cutRequest=requestId;$('fault').checked=false}if(!store()){pending.pop();return}$('body').value='';render();flush()};
$('body').onkeydown=e=>{if(e.key==='Enter'&&!e.shiftKey&&!e.isComposing){e.preventDefault();$('compose').requestSubmit()}};
window.addEventListener('pagehide',()=>{clearTimeout(retry);socket?.close()});status('연결 전');render();

$('messages').addEventListener('scroll',markVisibleRead);document.addEventListener('visibilitychange',markVisibleRead);window.addEventListener('focus',markVisibleRead);
window.addEventListener('scroll',markVisibleRead,{passive:true});new IntersectionObserver(markVisibleRead,{threshold:[0,.1,1]}).observe($('messages'));

$('view-toggle').onclick=()=>{if(viewOpen)sendTyping(false);viewOpen=!viewOpen;$('view-toggle').textContent=viewOpen?'대화창 닫기':'대화창 열기';$('view-toggle').setAttribute('aria-expanded',String(viewOpen));$('conversation-view').hidden=!viewOpen;$('conversation-closed').hidden=viewOpen;if(viewOpen){$('messages').scrollTop=$('messages').scrollHeight;requestAnimationFrame(markVisibleRead)}};

$('body').addEventListener('input',inputTyping);$('body').addEventListener('blur',()=>sendTyping(false));document.addEventListener('visibilitychange',()=>{if(document.hidden)sendTyping(false)});

let deliveryState={enabled:false,running:false};
async function refreshDelivery(){try{const response=await fetch('/api/demo/delivery');if(!response.ok)return;const wasBlocked=databaseBlocked;deliveryState=await response.json();
  databaseBlocked=deliveryState.databaseRunning===false;
  $('database-toggle').disabled=!deliveryState.controls;$('database-toggle').textContent=databaseBlocked?'DB 복구':'DB 중단';
  $('database-state').textContent=databaseBlocked?'DB 중단 · 저장/조회 불가 · PC에서 전송 대기':'DB 정상 · 저장 가능';
  if(wasBlocked&&!databaseBlocked){readRequested=new Map();flush();render();requestAnimationFrame(markVisibleRead)}
  if(wasBlocked!==databaseBlocked)render(false);
  $('broker-toggle').disabled=!deliveryState.enabled||!deliveryState.controls;$('broker-toggle').textContent=deliveryState.brokerRunning===false?'Kafka 복구':'Kafka 중단';
  $('delivery-state').textContent=deliveryState.enabled?(deliveryState.brokerRunning===false?'Kafka 중단 · ':'Kafka 연결 허용 · ')+(deliveryState.running?'전달 서버 실행 중':'전달 서버 중단')+(deliveryState.metricsStale?' · DB 지표는 마지막 확인값':'')+' · Outbox '+deliveryState.outboxPending+' · Kafka 대기 '+deliveryState.queued+(deliveryState.publishError?' · 발행 보류: '+deliveryState.publishError:''):'Kafka 비활성 · KAFKA_ENABLED=true와 브로커가 필요합니다.';
  $('worker-toggle').disabled=!deliveryState.enabled||!deliveryState.controls;$('worker-toggle').textContent=deliveryState.running?'전달 서버 중단':'전달 서버 복구';
  $('kafka-option').disabled=!deliveryState.enabled;
}catch{}}
$('database-toggle').onclick=async()=>{await fetch('/api/demo/delivery',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({databaseRunning:deliveryState.databaseRunning===false})});await refreshDelivery()};
$('broker-toggle').onclick=async()=>{await fetch('/api/demo/delivery',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({brokerRunning:deliveryState.brokerRunning===false})});await refreshDelivery()};
$('worker-toggle').onclick=async()=>{await fetch('/api/demo/delivery',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({running:!deliveryState.running})});await refreshDelivery()};
$('mode').onchange=()=>{$('notice').textContent=$('mode').value==='kafka'?'Kafka 경유 · 저장 ACK 후 비동기 전달':'현재 방식 · 저장 후 직접 방송'};
setInterval(refreshDelivery,1000);refreshDelivery();
