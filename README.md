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

서버 이벤트: `joined` (messages 배열과 reads 배열), `presence` (names 배열), `message` (저장된 message 객체), `ack` (requestId/message/duplicate), `read` (receipt: roomId/clientId/lastReadId), `error` (code/detail).
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
# 읽음 확인

`대화창 열기/닫기`는 소켓 연결과 별도의 화면 상태입니다. 닫아도 메시지는 계속 수신하며, `viewOpen`이 false인 동안 READ 프레임을 보내지 않습니다. 다시 열고 대화 맨 아래를 확인하면 읽음 신호를 보냅니다. 서버는 대화창 열림 상태를 추측하지 않고 클라이언트의 명시적인 READ 요청을 처리합니다.

메시지 저장 ACK와 읽음 확인은 별개입니다. 대화 화면이 보이고 맨 아래까지 열렸을 때 클라이언트가 `{ "type": "read", "upToId": 42 }`를 전송합니다. 서버는 소켓에 입장한 참가자와 방을 기준으로 메시지를 검증하고 `chat_reads`의 마지막 읽은 ID를 앞으로만 갱신한 뒤 같은 방에 `read` 이벤트를 보냅니다.

재접속 `joined` 응답에는 대화 기록과 `reads`가 함께 포함됩니다. 단순 입장이나 메시지 수신만으로 서버가 읽음을 생성하지 않습니다. UI의 읽음 표시는 상대 클라이언트가 보고한 읽음 위치이며 사람의 실제 독해를 측정하지 않습니다. 독립 UI는 다른 참가자 한 명 이상이 확인한 내 메시지를 읽음으로 표시합니다. 포트폴리오의 A·B 체험은 상대 한 명의 읽음 위치를 사용합니다.

실제 WebSocket 테스트는 오프라인 메시지 보관 후 명시적 읽음, 방 격리, 세션 기반 신원, 잘못된 ID 거부, 이전 ID 재전송 시 읽음 위치 유지 및 재접속 시 복구를 검증합니다.

## 입력 중 신호

클라이언트는 `{ "type": "typing", "active": true }`를 최대 약 0.9초 간격으로 보내고, 2초간 입력이 없거나 전송·대화창 닫기·포커스 이동 시 false를 보냅니다. 서버는 입장한 소켓의 방·이름·clientId로 신호를 구성해 같은 방의 다른 참가자에게만 중계합니다. 본문, 메시지 기록, 읽음 위치, DB 저장 및 ACK를 만들지 않습니다. 상대 UI는 신호가 갱신되지 않으면 4초 후 표시를 만료시킵니다. 소켓 종료 시 서버도 해제 신호를 보냅니다.

## ACK 수신 전 연결 끊김 체험

`다음 전송: 서버 저장 후 ACK 수신 전 연결 끊기`를 체크하고 보냅니다. 클라이언트가 저장된 메시지 방송을 받는 순간 소켓을 닫고, 해당 연결의 뒤늦은 ACK를 무시하는 의도적인 장애 체험입니다. 실제 네트워크 장애를 발생시키는 기능은 아닙니다. 수신 방송이나 재접속 기록만으로 대기 요청을 지우지 않고 ACK를 받아야 지웁니다. 다시 연결하면 sessionStorage에 보관한 같은 requestId·본문을 자동 재전송합니다. 서버는 기존 메시지 ID로 ACK하고 INSERT와 두 번째 방송은 생략합니다. 수동 장애 체험은 연결 버튼으로 복귀하며, 일반적인 예기치 않은 끊김은 기존 재접속 타이머를 사용합니다.

## 현재 방식 / Kafka 방식 선택

같은 WebSocket `send` 요청에 `mode: "direct"` 또는 `mode: "kafka"`를 지정합니다. 생략하면 기존 direct입니다. 서버 UI에서도 전송 방식을 선택할 수 있고, 대기 요청에는 선택한 mode를 함께 보관하므로 재전송 도중 경로가 바뀌지 않습니다.

- Direct: 대화 저장/commit → 같은 방에 직접 방송 → 저장 ACK.
- Kafka: 대화와 `chat_outbox`를 같은 DB 트랜잭션으로 저장 → 저장 ACK. 별도 릴레이가 미발행 Outbox를 Kafka에 발행하고, Consumer가 같은 방에 WebSocket으로 전달한 뒤 처리 기록과 offset을 갱신합니다. ACK는 Kafka 소비 완료나 상대 읽음을 의미하지 않습니다.
- `chat-delivery` 토픽 2개 파티션: lobby=0, design=1. 이 단일 릴레이는 메시지 ID 순으로 발행합니다. Consumer를 중단해도 Outbox 발행은 계속됩니다.
- DB 저장 후 발행 전 장애는 Outbox 재시도로 복구합니다. 발행 후 Outbox 갱신 전 장애로 같은 이벤트가 다시 발행될 수 있어 `chat_deliveries(message_id)`와 화면 메시지 ID로 중복을 처리합니다. DB와 WebSocket을 포함한 exactly-once 전달을 보장하지 않습니다.
- 두 경로를 섞으면 Kafka 메시지가 늦게 도착할 수 있습니다. 읽음 위치는 `chat_mode_reads`에 방·참가자·전송 방식별로 저장해, 나중에 보낸 Direct 메시지를 읽은 것으로 앞서 대기 중이던 Kafka 메시지가 읽음 처리되지 않게 합니다. 기존 `chat_reads`는 삭제하지 않고 Direct 읽음으로 이관합니다.

### 실제 브로커와 실행

Java 21과 Maven만으로 실제 단일 노드 Kafka를 띄우는 로컬 데모입니다. 이 런처의 Kafka와 H2 기록은 임시이며 종료 시 초기화됩니다.

```powershell
.\scripts\maven.ps1 test-compile exec:java '-Dexec.mainClass=dev.portfolio.chat.KafkaDemoLauncher' '-Dexec.classpathScope=test'
```

http://localhost:8089 를 두 창에서 열어 같은 방에 연결합니다. Kafka 방식 선택 → 전달 서버 중단 → 메시지 여러 개 전송 → Kafka 대기 수 확인 → 전달 서버 복구 순서로 체험합니다. Consumer가 멈춰 있어도 Direct 경로는 즉시 전달됩니다.

기존 브로커를 사용할 때는 `KAFKA_ENABLED=true`, `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`로 설정하고 Java 앱을 실행합니다. 데모 중단/복구 API를 쓰려면 `DEMO_CONTROLS_ENABLED=true`도 설정합니다. 기본값은 Kafka와 변경 API 모두 비활성입니다.

`GET /api/demo/delivery`: Consumer 상태, Outbox 미발행 수, 발행 후 미처리 이벤트 수와 목록. 대기 수는 애플리케이션 Outbox/처리 테이블에서 계산하며 Kafka 관리 API의 consumer lag 지표는 아닙니다.
`PUT /api/demo/delivery` + `{ "running": false }` / true: Consumer pause/resume. 진행 중인 한 건은 완료될 수 있습니다.

이 데모에서 수신 서버와 전달 서버는 같은 JVM 안의 서로 다른 역할입니다. 중단 버튼은 Consumer 역할만 멈추며 프로세스·WebSocket·DB는 계속 동작합니다. 여러 WebSocket 서버로 확장하려면 접속 위치 라우팅과 공유 DB, 인증을 추가해야 합니다. 같은 consumer group은 작업을 분담하므로 모든 서버에 자동 방송되지 않습니다.

`KafkaDeliveryTest`는 실제 KRaft 브로커를 띄워 중단 중 누적, 저장 ACK, Direct 우회 전달, 복구 후 같은 방 순서, 재전송/이벤트 재처리 중복 방지와 경로별 읽음을 확인합니다. GitHub Pages 체험은 이 구현 규칙을 보여주는 브라우저 시뮬레이션입니다.

## Kafka 연결 중단 / Outbox 복구 체험

Kafka 중단과 전달 서버 중단을 독립적으로 선택합니다. `PUT /api/demo/delivery`의 선택 필드는 `brokerRunning`과 `running`이며 지정한 필드만 변경합니다. 기존 `{ "running": false }` 요청도 그대로 지원합니다.

- Kafka 중단: Outbox 발행을 보류하고 Consumer를 일시 중지합니다. WebSocket, DB 저장과 저장 ACK는 유지합니다. 새 메시지는 `published=FALSE`, partition/offset 미지정 상태로 Outbox에 남습니다.
- Kafka 복구: Outbox를 원래 메시지 ID와 방 파티션으로 다시 발행합니다. 전달 서버가 실행 중이면 이어서 수신합니다. 전달 서버를 따로 중단했다면 Kafka에 발행된 채로 대기하며 자동으로 서버를 켜지 않습니다.
- 이미 Kafka에 발행된 기록은 중단/복구 시 지우지 않습니다. 진행 중인 한 번의 발행이나 전달은 완료될 수 있습니다. 재발행 가능성은 기존 이벤트 ID/메시지 ID 중복 처리로 다룹니다.
- UI의 Kafka 중단 버튼은 **연결 불가 상태를 주입하는 장애 체험**입니다. 외부 브로커 프로세스를 종료하거나 Kafka 로그를 삭제하지 않습니다. `brokerRunning`은 이 체험의 연결 허용 상태이며 실제 브로커 health 검사 결과가 아닙니다.
- 실제 `KafkaTemplate.send`가 실패해도 Outbox를 발행 완료로 갱신하지 않고 다음 스케줄에 재시도합니다. `publishError`에는 예외 종류만 표시합니다. 테스트는 전송 실패를 주입한 뒤 실제 임베디드 Kafka로 복구 발행하는 경우도 별도로 확인합니다.

체험: Kafka 방식 선택 → Kafka 중단 → 메시지 여러 개 전송 → Outbox 미발행 수 증가/상대 미수신 확인 → Kafka 복구 → 발행 및 전달 확인. 두 역할을 모두 중단했다면 Kafka 복구 후 대기가 Kafka로 옮겨가는 것과 전달 서버 복구 후 수신되는 것을 차례로 확인합니다.
