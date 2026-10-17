const $=id=>document.getElementById(id);
const clientId=sessionStorage.getItem('chat-client')||crypto.randomUUID();sessionStorage.setItem('chat-client',clientId);
let socket=null,wanted=false,joined=false,retry=null,attempt=0,generation=0,room='lobby',name='참가자',viewOpen=true;
let messages=new Map(),pending=[],reads=new Map(),readRequested=0;
try{pending=JSON.parse(sessionStorage.getItem('chat-pending')||'[]')}catch{}
let typingSent=false,lastTypingAt=0,typingIdle=null;const peerTyping=new Map();
const store=()=>{try{sessionStorage.setItem('chat-pending',JSON.stringify(pending))}catch{}};
function status(text){$('status').textContent=text;$('body').disabled=!joined;$('send').disabled=!joined;$('connect').disabled=wanted;$('disconnect').disabled=!wanted;$('name').disabled=wanted;$('closed-state').textContent=joined?'온라인 · 메시지 수신 중':'오프라인 · 서버에 대화 보관 중'}
function render(follow=true){
  const top=$('messages').scrollTop,atBottom=$('messages').scrollHeight-top-$('messages').clientHeight<24;
  const rows=[...messages.values()].sort((a,b)=>a.id-b.id).slice(-100);
  $('closed-count').textContent='받은 메시지 '+rows.length+'건 · 안 읽은 메시지 '+rows.filter(m=>m.clientId!==clientId&&m.id>(reads.get(clientId)?.lastReadId||0)).length+'건';
  const elements=rows.map(m=>bubble(m,false));for(const p of pending.filter(p=>p.roomId===room))elements.push(bubble({clientId,sender:name,body:p.body,requestId:p.requestId},true));
  $('messages').replaceChildren(...elements);$('messages').scrollTop=follow&&atBottom?$('messages').scrollHeight:top;requestAnimationFrame(markVisibleRead);
}
function bubble(m,waiting){const div=document.createElement('article');div.className='bubble'+(m.clientId===clientId?' mine':'')+(waiting?' pending':'');const small=document.createElement('small');small.textContent=m.sender+' · '+(waiting?'서버 확인 대기':'#'+m.id+' · '+new Date(m.createdAt).toLocaleTimeString('ko-KR',{hour:'2-digit',minute:'2-digit'}));const p=document.createElement('p');p.textContent=m.body;div.append(small,p);if(!waiting&&m.clientId===clientId){const receipt=document.createElement('span');receipt.className='read-state';receipt.textContent=[...reads.values()].some(r=>r.clientId!==clientId&&r.roomId===room&&r.lastReadId>=m.id)?'읽음':'안 읽음';div.append(receipt)}return div}
function markVisibleRead(){
  if(document.hidden||!viewOpen||!joined||socket?.readyState!==WebSocket.OPEN)return;
  const area=$('messages');if(area.scrollHeight-area.scrollTop-area.clientHeight>24)return;
  const end=area.lastElementChild?.getBoundingClientRect();if(!end||end.bottom<=0||end.bottom>innerHeight+2)return;
  const latest=Math.max(0,...[...messages.values()].filter(m=>m.clientId!==clientId&&m.roomId===room).map(m=>m.id));
  if(latest<=Math.max(readRequested,reads.get(clientId)?.lastReadId||0))return;
  readRequested=latest;socket.send(JSON.stringify({type:'read',upToId:latest}));
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
function flush(){if(!joined||socket?.readyState!==WebSocket.OPEN)return;for(const p of pending.filter(p=>p.roomId===room))socket.send(JSON.stringify({type:'send',requestId:p.requestId,body:p.body}))}
function connect(){
  clearTimeout(retry);const version=++generation;joined=false;readRequested=0;status('연결 중…');
  const ws=new WebSocket((location.protocol==='https:'?'wss:':'ws:')+'//'+location.host+'/ws/chat');socket=ws;
  ws.onopen=()=>{if(version!==generation)return;ws.send(JSON.stringify({type:'join',roomId:room,clientId,name}))};
  ws.onmessage=e=>{if(version!==generation)return;const event=JSON.parse(e.data);
    if(event.type==='joined'){reads=new Map((event.reads||[]).map(r=>[r.clientId,r]));joined=true;attempt=0;for(const m of event.messages)messages.set(m.id,m);status('● 연결됨 · '+room);$('notice').textContent='연결되었습니다. 최근 대화를 불러왔습니다.';render();flush()}
    else if(event.type==='message'){messages.set(event.message.id,event.message);pending=pending.filter(p=>p.requestId!==event.message.requestId||event.message.clientId!==clientId);store();render()}
    else if(event.type==='ack'){messages.set(event.message.id,event.message);pending=pending.filter(p=>p.requestId!==event.requestId);store();render();$('notice').textContent=event.duplicate?'중복 요청 · 기존 메시지로 확인했습니다.':'서버 저장 확인 · #'+event.message.id}
    else if(event.type==='typing')receiveTyping(event);
    else if(event.type==='read'){if(event.receipt.roomId===room){const previous=reads.get(event.receipt.clientId);if(!previous||previous.lastReadId<event.receipt.lastReadId)reads.set(event.receipt.clientId,event.receipt);render(false)}}
    else if(event.type==='presence')$('presence').textContent=event.names.join(', ')||'—';
    else if(event.type==='error'){$('notice').textContent=event.detail;wanted=false;joined=false;generation++;ws.close();status('입력값 확인 후 다시 연결하세요.')}
  };
  ws.onclose=()=>{if(version!==generation)return;joined=false;typingSent=false;clearTimeout(typingIdle);peerTyping.clear();renderTyping();$('presence').textContent='—';if(wanted){const delay=Math.min(10000,1000*2**attempt++);status('연결 끊김 · '+delay/1000+'초 후 재접속');retry=setTimeout(connect,delay)}else status('연결 종료')};
  ws.onerror=()=>{if(version===generation)$('notice').textContent='연결을 확인하고 있습니다.'};
}
function disconnect(){sendTyping(false);peerTyping.clear();renderTyping();wanted=false;joined=false;generation++;clearTimeout(retry);socket?.close();socket=null;status('연결 종료');$('presence').textContent='—'}
$('connect').onclick=()=>{name=$('name').value.trim();if(!name||name.length>20){$('notice').textContent='이름은 1~20자입니다.';return}wanted=true;connect()};$('disconnect').onclick=disconnect;
$('room').onchange=()=>{const again=wanted;disconnect();room=$('room').value;messages=new Map();reads=new Map();readRequested=0;$('room-name').textContent=$('room').selectedOptions[0].textContent;$('notice').textContent='';render();if(again){wanted=true;connect()}};
$('compose').onsubmit=e=>{e.preventDefault();const body=$('body').value.trim();if(!joined||!body)return;sendTyping(false);pending.push({roomId:room,requestId:crypto.randomUUID(),body});store();$('body').value='';render();flush()};
$('body').onkeydown=e=>{if(e.key==='Enter'&&!e.shiftKey&&!e.isComposing){e.preventDefault();$('compose').requestSubmit()}};
window.addEventListener('pagehide',()=>{clearTimeout(retry);socket?.close()});status('연결 전');render();

$('messages').addEventListener('scroll',markVisibleRead);document.addEventListener('visibilitychange',markVisibleRead);window.addEventListener('focus',markVisibleRead);
window.addEventListener('scroll',markVisibleRead,{passive:true});new IntersectionObserver(markVisibleRead,{threshold:[0,.1,1]}).observe($('messages'));

$('view-toggle').onclick=()=>{if(viewOpen)sendTyping(false);viewOpen=!viewOpen;$('view-toggle').textContent=viewOpen?'대화창 닫기':'대화창 열기';$('view-toggle').setAttribute('aria-expanded',String(viewOpen));$('conversation-view').hidden=!viewOpen;$('conversation-closed').hidden=viewOpen;if(viewOpen){$('messages').scrollTop=$('messages').scrollHeight;requestAnimationFrame(markVisibleRead)}};

$('body').addEventListener('input',inputTyping);$('body').addEventListener('blur',()=>sendTyping(false));document.addEventListener('visibilitychange',()=>{if(document.hidden)sendTyping(false)});
