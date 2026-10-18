import fs from 'node:fs'
import path from 'node:path'
import crypto from 'node:crypto'
import {execFileSync} from 'node:child_process'
const root=path.resolve(process.argv[2]||'.'),destination=path.resolve(process.argv[3]||'docs/chat-source.json')
const revision=execFileSync('git',['rev-parse','HEAD'],{cwd:root,encoding:'utf8'}).trim()
const read=file=>execFileSync('git',['show',revision+':'+file],{cwd:root,encoding:'utf8'}).replace(/\r\n/g,'\n')
const java='src/main/java/dev/portfolio/chat/',socket=java+'ChatSocket.java',service=java+'ChatService.java',client='src/main/resources/static/chat.js',delivery=java+'KafkaDelivery.java'
const availability=java+'DatabaseAvailability.java'
const files=[availability,delivery,socket,service,java+'MessageRepository.java',java+'SocketConfig.java',java+'ChatMessage.java','src/main/resources/schema.sql',client]
const sources=Object.fromEntries(files.map(file=>[file,read(file)]))
function step(label,file,highlight,node){const lines=sources[file].split('\n'),index=lines.findIndex(line=>line.includes(highlight));if(index<0)throw Error('Missing evidence '+highlight);const start=index;return {label,node,highlight,file,line:start+1,lines:lines.slice(start,index+3)}}
const join=[step('WebSocket 연결 생성',client,'const ws=new WebSocket','sender'),step('소켓 OPEN · 양방향 연결 완료',client,'ws.onopen=','server'),step('JOIN 프레임 전송',client,"ws.send(JSON.stringify({type:'join'",'sender'),step('입장 요청 검증',socket,'service.validateRoom(roomId)','server'),step('최근 메시지 SELECT',service,'messages.findTop100ByRoomIdOrderByIdDesc','database'),step('저장소 → 서버 · 최근 대화 반환',service,'messages.findTop100ByRoomIdOrderByIdDesc','database-return'),step('서버 → PC · 보관된 대화 전달',socket,'"type", "joined"','recipient'),step('접속자 목록 방송',socket,'"type", "presence"','recipient')]
const send=[step('JSON 프레임 전송',client,"socket.send(JSON.stringify({type:'send'",'sender'),step('서버에서 프레임 해석',socket,'json.readTree(frame.getPayload())','server'),step('본문과 식별자 검증',service,'body = text(body, 2000','server'),step('중복 requestId 조회',service,'messages.findByClientIdAndRequestId','database'),step('메시지 INSERT · 트랜잭션 commit',service,'messages.saveAndFlush','database'),step('같은 방에 저장된 메시지 방송',socket,'if (!saved.duplicate()&&saved.message().deliveryMode().equals("direct")) broadcast','recipient'),step('발신자에게 ACK 응답',socket,'"type", "ack"','sender')]
const duplicate=[send[0],send[1],send[3],step('기존 메시지 반환 · INSERT 생략',service,'return new Saved(view(previous.get()), true)','database'),step('중복이면 방송 생략',socket,'if (!saved.duplicate()&&saved.message().deliveryMode().equals("direct")) broadcast','server'),send[6]]
const interrupted=[...send.slice(0,6),step('ACK 수신 전 연결 끊김 · 요청 보관',client,'function interruptBeforeAck','sender')]
const retryFlow=[...duplicate,step('같은 요청의 ACK 수신 · 대기 목록 제거',client,'pending=pending.filter','sender')]
const disconnect=[step('클라이언트가 소켓 CLOSE',client,'socket?.close();socket=null','sender'),step('연결 종료 감지',socket,'afterConnectionClosed(WebSocketSession','server'),step('세션 제거와 접속자 목록 갱신',socket,'members.remove(socket.getId())','recipient')]
const readFlow=[step('대화 화면에 메시지 표시',client,'if(document.hidden||!viewOpen||!joined','sender'),step('READ 프레임 · 마지막 읽은 ID 전송',client,"socket.send(JSON.stringify({type:'read'",'sender'),step('읽음 위치 저장 · 이전보다 앞으로만',service,'UPDATE chat_mode_reads SET last_read_id','database'),step('같은 방에 읽음 이벤트 전달',socket,'"type", "read", "receipt"','recipient'),step('발신자 화면을 읽음으로 갱신',client,'receipt.textContent=','recipient')]
const typingFlow=[step('입력 상태 프레임 전송',client,"socket.send(JSON.stringify({type:'typing',active}))",'sender'),step('입장 세션과 입력 상태 검증',socket,'if (!input.path("active").isBoolean())','server'),step('같은 방의 상대에게 중계 · DB 저장 없음',socket,'if (!peer.roomId().equals(source.roomId()) ||','server'),step('상대의 입력 중 표시 갱신',client,"$('typing-status').textContent=",'recipient')]
const kafkaSend=[send[0],send[2],send[3],step('대화 + Outbox INSERT · 함께 commit',service,'INSERT INTO chat_outbox','database'),send[6],step('Outbox → Kafka · 같은 방 파티션 발행',delivery,'kafka.send(TOPIC','broker'),step('Kafka 기록 완료 · 전달 서버 처리 대기',delivery,'UPDATE chat_outbox SET published','broker')]
const kafkaUnavailable=[...kafkaSend.slice(0,5),step('Kafka 연결 불가 · 발행 보류',delivery,'if(!brokerRunning){','broker-unavailable'),step('Outbox 미발행 보관 · 복구 후 재시도',delivery,'Keep unpublished Outbox rows','outbox')]
const kafkaUnavailableInterrupted=kafkaUnavailable.map((item,index)=>index===4?interrupted.at(-1):item)
const kafkaPublish=kafkaSend.slice(5)
const kafkaInterrupted=kafkaSend.map((item,index)=>index===4?interrupted.at(-1):item)
const kafkaDeliver=[step('Kafka Consumer · 보관된 이벤트 수신',delivery,'public void consume(ConsumerRecord','consumer'),step('처리한 이벤트 ID인지 확인',delivery,'SELECT COUNT(*) FROM chat_deliveries','consumer'),step('같은 방 참가자에게 WebSocket 전달',socket,'public synchronized void deliver(KafkaDelivery.Ready','recipient'),step('처리한 이벤트 기록 · DB commit',delivery,'INSERT INTO chat_deliveries','consumer'),step('Consumer 처리 완료 · offset commit',delivery,'Container commits its Kafka offset','offset')]
const databaseUnavailable=[send[0],step('DB 접근 실패 · 저장하지 못함',availability,'if (!running) throw','database-unavailable'),step('실패 응답 · ACK와 상대 전달 없음',socket,'"code", "DB_UNAVAILABLE"','server'),step('PC에서 원래 요청 보관 · 복구 대기',client,'Keep the joined socket open','sender')]
const databaseRetry=send.map((item,index)=>index===0?step('원래 요청 ID로 재전송',client,"requestId:p.requestId,body:p.body",'sender'):item)
const databaseRetryKafka=kafkaSend.map((item,index)=>index===0?databaseRetry[0]:item)
const databaseRetryOutbox=kafkaUnavailable.map((item,index)=>index===0?databaseRetry[0]:item)
const bodyMax=Number(sources[service].match(/body = text\(body, (\d+),/)?.[1]);if(bodyMax!==2000)throw Error('Review changed message rule')
const model={repo:'https://github.com/seonyoungyoo16/springboot-chat-lab',revision,bodyMax,historyMax:100,rooms:[{id:'lobby',name:'라운지'},{id:'design',name:'설계 이야기'}],flows:{join,send,duplicate,disconnect,read:readFlow,typing:typingFlow,interrupted,retry:retryFlow,kafkaSend,kafkaInterrupted,kafkaDeliver,kafkaPublish,kafkaUnavailable,kafkaUnavailableInterrupted,databaseUnavailable,databaseRetry,databaseRetryKafka,databaseRetryOutbox},sources:files.map(file=>({file,sha256:crypto.createHash('sha256').update(sources[file]).digest('hex')}))}
fs.mkdirSync(path.dirname(destination),{recursive:true});fs.writeFileSync(destination,JSON.stringify(model,null,2)+'\n');console.log(JSON.stringify({revision,destination,flows:Object.keys(model.flows)}))
