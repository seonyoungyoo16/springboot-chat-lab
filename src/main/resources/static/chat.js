const $=id=>document.getElementById(id);
const clientId=sessionStorage.getItem('chat-client')||crypto.randomUUID();sessionStorage.setItem('chat-client',clientId);
let socket=null,wanted=false,joined=false,retry=null,attempt=0,generation=0,room='lobby',name='참가자';
let messages=new Map(),pending=[];
try{pending=JSON.parse(sessionStorage.getItem('chat-pending')||'[]')}catch{}
const store=()=>{try{sessionStorage.setItem('chat-pending',JSON.stringify(pending))}catch{}};
function status(text){$('status').textContent=text;$('body').disabled=!joined;$('send').disabled=!joined;$('connect').disabled=wanted;$('disconnect').disabled=!wanted;$('name').disabled=wanted}
function render(){
  const rows=[...messages.values()].sort((a,b)=>a.id-b.id).slice(-100);
  const elements=rows.map(m=>bubble(m,false));for(const p of pending.filter(p=>p.roomId===room))elements.push(bubble({clientId,sender:name,body:p.body,requestId:p.requestId},true));
  $('messages').replaceChildren(...elements);$('messages').scrollTop=$('messages').scrollHeight;
}
function bubble(m,waiting){const div=document.createElement('article');div.className='bubble'+(m.clientId===clientId?' mine':'')+(waiting?' pending':'');const small=document.createElement('small');small.textContent=m.sender+' · '+(waiting?'서버 확인 대기':'#'+m.id+' · '+new Date(m.createdAt).toLocaleTimeString('ko-KR',{hour:'2-digit',minute:'2-digit'}));const p=document.createElement('p');p.textContent=m.body;div.append(small,p);return div}
function flush(){if(!joined||socket?.readyState!==WebSocket.OPEN)return;for(const p of pending.filter(p=>p.roomId===room))socket.send(JSON.stringify({type:'send',requestId:p.requestId,body:p.body}))}
function connect(){
  clearTimeout(retry);const version=++generation;joined=false;status('연결 중…');
  const ws=new WebSocket((location.protocol==='https:'?'wss:':'ws:')+'//'+location.host+'/ws/chat');socket=ws;
  ws.onopen=()=>{if(version!==generation)return;ws.send(JSON.stringify({type:'join',roomId:room,clientId,name}))};
  ws.onmessage=e=>{if(version!==generation)return;const event=JSON.parse(e.data);
    if(event.type==='joined'){joined=true;attempt=0;for(const m of event.messages)messages.set(m.id,m);status('● 연결됨 · '+room);render();flush()}
    else if(event.type==='message'){messages.set(event.message.id,event.message);pending=pending.filter(p=>p.requestId!==event.message.requestId||event.message.clientId!==clientId);store();render()}
    else if(event.type==='ack'){messages.set(event.message.id,event.message);pending=pending.filter(p=>p.requestId!==event.requestId);store();render();$('notice').textContent=event.duplicate?'중복 요청 · 기존 메시지로 확인했습니다.':'서버 저장 확인 · #'+event.message.id}
    else if(event.type==='presence')$('presence').textContent=event.names.join(', ')||'—';
    else if(event.type==='error'){$('notice').textContent=event.detail;wanted=false;joined=false;generation++;ws.close();status('입력값 확인 후 다시 연결하세요.')}
  };
  ws.onclose=()=>{if(version!==generation)return;joined=false;$('presence').textContent='—';if(wanted){const delay=Math.min(10000,1000*2**attempt++);status('연결 끊김 · '+delay/1000+'초 후 재접속');retry=setTimeout(connect,delay)}else status('연결 종료')};
  ws.onerror=()=>{if(version===generation)$('notice').textContent='연결을 확인하고 있습니다.'};
}
function disconnect(){wanted=false;joined=false;generation++;clearTimeout(retry);socket?.close();socket=null;status('연결 종료');$('presence').textContent='—'}
$('connect').onclick=()=>{name=$('name').value.trim();if(!name||name.length>20){$('notice').textContent='이름은 1~20자입니다.';return}wanted=true;connect()};$('disconnect').onclick=disconnect;
$('room').onchange=()=>{const again=wanted;disconnect();room=$('room').value;messages=new Map();$('room-name').textContent=$('room').selectedOptions[0].textContent;$('notice').textContent='';render();if(again){wanted=true;connect()}};
$('compose').onsubmit=e=>{e.preventDefault();const body=$('body').value.trim();if(!joined||!body)return;pending.push({roomId:room,requestId:crypto.randomUUID(),body});store();$('body').value='';render();flush()};
$('body').onkeydown=e=>{if(e.key==='Enter'&&!e.shiftKey&&!e.isComposing){e.preventDefault();$('compose').requestSubmit()}};
window.addEventListener('pagehide',()=>{clearTimeout(retry);socket?.close()});status('연결 전');render();
