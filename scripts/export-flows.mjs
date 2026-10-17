import fs from 'node:fs'
import path from 'node:path'
import crypto from 'node:crypto'
import {execFileSync} from 'node:child_process'
const root=path.resolve(process.argv[2]||'.'),destination=path.resolve(process.argv[3]||'docs/chat-source.json')
const revision=execFileSync('git',['rev-parse','HEAD'],{cwd:root,encoding:'utf8'}).trim()
const read=file=>execFileSync('git',['show',revision+':'+file],{cwd:root,encoding:'utf8'}).replace(/\r\n/g,'\n')
const java='src/main/java/dev/portfolio/chat/',socket=java+'ChatSocket.java',service=java+'ChatService.java',client='src/main/resources/static/chat.js'
const files=[socket,service,java+'MessageRepository.java',java+'SocketConfig.java',java+'ChatMessage.java','src/main/resources/schema.sql',client]
const sources=Object.fromEntries(files.map(file=>[file,read(file)]))
function step(label,file,highlight,node){const lines=sources[file].split('\n'),index=lines.findIndex(line=>line.includes(highlight));if(index<0)throw Error('Missing evidence '+highlight);const start=index;return {label,node,highlight,file,line:start+1,lines:lines.slice(start,index+3)}}
const join=[step('WebSocket 연결 생성',client,'const ws=new WebSocket','sender'),step('입장 요청 검증',socket,'service.validateRoom(roomId)','server'),step('최근 메시지 SELECT',service,'messages.findTop100ByRoomIdOrderByIdDesc','database'),step('입장 결과와 대화 복구',socket,'"type", "joined"','recipient'),step('접속자 목록 방송',socket,'"type", "presence"','recipient')]
const send=[step('JSON 프레임 전송',client,"socket.send(JSON.stringify({type:'send'",'sender'),step('서버에서 프레임 해석',socket,'json.readTree(frame.getPayload())','server'),step('본문과 식별자 검증',service,'body = text(body, 2000','server'),step('중복 requestId 조회',service,'messages.findByClientIdAndRequestId','database'),step('메시지 INSERT · 트랜잭션 commit',service,'messages.saveAndFlush','database'),step('같은 방에 저장된 메시지 방송',socket,'if (!saved.duplicate()) broadcast','recipient'),step('발신자에게 ACK 응답',socket,'"type", "ack"','sender')]
const duplicate=[send[0],send[1],send[3],step('기존 메시지 반환 · INSERT 생략',service,'return new Saved(view(previous.get()), true)','database'),step('중복이면 방송 생략',socket,'if (!saved.duplicate()) broadcast','server'),send[6]]
const disconnect=[step('연결 종료 감지',socket,'afterConnectionClosed(WebSocketSession','server'),step('세션 제거와 접속자 목록 갱신',socket,'members.remove(socket.getId())','recipient')]
const bodyMax=Number(sources[service].match(/body = text\(body, (\d+),/)?.[1]);if(bodyMax!==2000)throw Error('Review changed message rule')
const model={repo:'https://github.com/seonyoungyoo16/springboot-chat-lab',revision,bodyMax,historyMax:100,rooms:[{id:'lobby',name:'라운지'},{id:'design',name:'설계 이야기'}],flows:{join,send,duplicate,disconnect},sources:files.map(file=>({file,sha256:crypto.createHash('sha256').update(sources[file]).digest('hex')}))}
fs.mkdirSync(path.dirname(destination),{recursive:true});fs.writeFileSync(destination,JSON.stringify(model,null,2)+'\n');console.log(JSON.stringify({revision,destination,flows:Object.keys(model.flows)}))
