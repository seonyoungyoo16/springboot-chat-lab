# Spring Boot Chat Lab

Java 21 · Spring Boot 3.5.16 · Native WebSocket · JPA · H2로 만든 실시간 채팅입니다.

## 실행
```powershell
.\scripts\maven.ps1 verify
.\scripts\maven.ps1 spring-boot:run
```
Maven이 설치되어 있으면 `mvn verify`, `mvn spring-boot:run`을 사용합니다.

http://localhost:8089 를 두 브라우저 창에서 엽니다. 서로 다른 이름으로 같은 방에 연결한 뒤 전송하면 상대 창에 즉시 표시됩니다. 이름은 로그인 인증이 아닌 데모 식별자입니다. 기본 화면은 자동 전송 없이 연결 버튼을 눌러 시작합니다.

## 기능과 처리 순서
- 방: 라운지(lobby), 설계 이야기(design). 접속자 목록과 접속 종료 반영.
- 메시지: 이름 20자, 본문 2000자까지. 서버에서 입력 검증.
- `WebSocket frame → ChatSocket → ChatService → H2 저장/commit → 같은 방 broadcast → 발신자 ACK`.
- `(client_id, request_id)` UNIQUE. 같은 요청은 저장/방송을 반복하지 않고 원래 메시지 ID로 ACK합니다.
- 재접속: 1, 2, 4, 8, 10초 간격으로 재시도하고 같은 요청 ID의 미확인 메시지를 재전송합니다.
- 입장/재접속 시 최근 100개 메시지를 ID 오름차순으로 받습니다. 클라이언트는 메시지 ID로 중복을 합칩니다. 긴 오프라인 기간의 전체 기록 복구를 보장하는 프로토콜은 아닙니다.
- H2 파일은 `data/chat.mv.db`에 저장합니다. 서버를 재시작해도 메시지는 유지됩니다.

## 프로토콜
WebSocket endpoint: `/ws/chat` (같은 origin의 브라우저 연결).

```json
{"type":"join","roomId":"lobby","clientId":"browser-uuid","name":"참가자 A"}
{"type":"send","requestId":"message-uuid","body":"안녕하세요"}
```

서버 이벤트: `joined` (messages 배열), `presence` (names 배열), `message` (저장된 message 객체), `ack` (requestId/message/duplicate), `error` (code/detail).
메시지 객체: `id, roomId, clientId, requestId, sender, body, createdAt`.

REST: `GET /api/rooms`, `GET /api/rooms/{roomId}/messages` (최근 100개).

## 검증
`ChatSocketTest`는 임의 포트에서 실제 Tomcat을 띄우고 Java HTTP WebSocket 클라이언트 여러 개를 연결합니다. 양방향 수신, 방 격리, 재접속 기록, 중복 요청, 입장 전 전송/없는 방/JSON 오류/길이 제한을 확인합니다.

## 배포
```sh
docker build -t chat-lab .
docker run --rm -p 8089:8089 -v chat-data:/app/data chat-lab
```
`PORT`, `DB_URL`, `DB_USER`, `DB_PASSWORD` 환경변수를 지원합니다. GitHub 저장소 게시와 Java 서버의 상시 호스팅은 별개입니다. 이 저장소는 실제 서버 실행용이고, GitHub Pages 포트폴리오는 별도 브라우저 시뮬레이션입니다.

이 버전은 단일 서버 프로세스용 데모입니다. Handler의 직렬 처리는 메시지 저장/방송 순서를 보존합니다. 여러 서버 인스턴스로 확장하려면 공유 브로커와 세션 라우팅, 인증 및 사용자별 권한 등을 별도로 구현해야 합니다. 서로 다른 이름을 입력하는 것만으로 사용자 인증이 되지는 않습니다.
