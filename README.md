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


### DB 중단과 같은 요청 ID 재시도

데모 제어를 켜면 `PUT /api/demo/delivery`의 `{"databaseRunning":false}`로 DB 접근 실패를 재현합니다. 실제 H2 프로세스나 파일은 종료·삭제하지 않습니다. 현재 방식과 Kafka 방식에 모두 적용하며 메시지 저장/조회, 읽음 저장, Outbox 발행과 Consumer 처리를 멈춥니다. 제어 상태 조회와 DB 복구 API는 DB 없이 동작합니다. 중단 중 지표는 마지막 확인값입니다.

저장 트랜잭션 실패 시 WebSocket은 `error / DB_UNAVAILABLE / retryable:true / requestId`를 돌려줍니다. 저장 ACK와 상대 전달은 하지 않습니다. 브라우저는 원래 요청 ID·본문·방·전송 방식을 sessionStorage에 보관하고, DB 복구를 확인하면 같은 요청을 자동 전송합니다. 이미 저장된 요청은 `(client_id, request_id)` 조회로 기존 메시지 ACK만 반환합니다. Kafka에서는 메시지와 Outbox가 함께 commit된 후 발행됩니다. Consumer의 DB 트랜잭션 실패는 offset을 commit하지 않고 재시도하며, 상대 전달은 DB commit 후에만 발생합니다.

`KafkaDeliveryTest`는 두 전송 방식의 중단/복구/중복 재시도와 실제 H2 Outbox INSERT 예외를 검증합니다. INSERT 예외 시 먼저 실행된 메시지 INSERT도 rollback되고, 재시도 시 메시지와 Outbox가 한 건만 남는지 확인합니다. Pages 화면은 이 동작을 재현하는 브라우저 시뮬레이션입니다.


### 여러 전송 대기 요청의 순차 복구

DB 중단 중에도 여러 메시지를 입력할 수 있습니다. PC 대기열은 본문·방·mode·원래 requestId를 배열 순서로 sessionStorage에 보관합니다. DB 복구 또는 소켓 재접속 후 현재 방의 맨 앞 요청 한 건만 전송하고, 해당 요청의 ACK를 확인한 뒤 다음 요청을 전송합니다. 방송이나 기록 조회, 다른 요청의 ACK로 대기열을 넘기지 않습니다. 재실패/연결 끊김 시 미확인 요청을 맨 앞에 유지합니다. 이미 commit된 요청의 재시도는 기존 ID의 ACK만 반환합니다.

현재 방식은 저장 후 순서대로 직접 방송하고, Kafka 방식은 방별 같은 파티션에 순서대로 발행해 전달합니다. Kafka 경유 요청의 저장 ACK와 Consumer 전달은 별도이며 전달 서버 중단 시 저장 ACK 후 Kafka에 대기합니다. 서로 다른 방/전송 경로의 전역 도착 순서를 보장하는 분산 시스템은 아닙니다.

`node scripts/test-client-queue.mjs`는 실제 클라이언트에서 사용하는 단일 전송 상태 모듈을 검사합니다. 통합 테스트는 두 전송 방식에서 여러 원래 요청 ID, 두 번째 DB 장애, commit된 요청 재시도, 저장 및 수신 순서와 중복 방지를 확인합니다.


### 전송 전 대기 요청 취소

클라이언트 대기열의 `취소`로 요청을 제거할 수 있습니다. 제거된 대기열을 sessionStorage에 저장한 후 화면을 갱신하며, DB 복구 시 남은 원래 요청 ID만 순서대로 보냅니다. 전송 직전에 `cancelable=false`를 저장해 ACK 유실/새로고침을 전송 전 상태로 오인하지 않습니다. 실제 저장 실패 응답이 확인되면 다시 취소 가능 상태로 바꿉니다. 저장 상태 기록에 실패하면 전송과 취소를 보류합니다.

이 취소는 PC 대기열에서 처리하며 서버 취소 API를 호출하지 않습니다. 이미 서버로 전송한 요청은 ACK가 없어도 취소할 수 없으며 같은 ID로 저장 결과를 확인합니다. 기존 메시지 삭제/Outbox 삭제 기능은 포함하지 않습니다. `node scripts/test-client-queue.mjs`에서 중간/맨 앞/전체 요청 취소, 다른 방/알 수 없는 상태 보호, ACK 유실 후 취소 거부, 저장 실패 확인 후 취소, 남은 ID의 원래 전송 순서를 검증합니다.


### 취소 메시지 다시 보내기

취소 목록의 `다시 보내기`는 crypto.randomUUID()로 새 요청 ID를 발급하고 입력란의 본문·원래 채팅방·mode를 가진 요청을 대기열 맨 뒤에 추가합니다. 입력란에는 원문을 미리 채우며, 보내기 전에 수정할 수 있습니다. 공백만 있는 내용이나 2000자를 넘는 내용은 추가하지 않습니다. 이전 ID는 취소 상태로 남기고 새 ID를 연결해 표시합니다. 대기열 저장에 성공한 뒤 완료 표시를 갱신하며, 같은 취소 항목은 한 번만 다시 추가할 수 있습니다. 이미 전송 중인 요청과 나머지 대기 요청의 ID·순서는 바꾸지 않습니다. 새로고침 후에는 새 ID 그대로 대기열을 복구합니다.

새 요청이 다시 취소되면 그 항목에서 또 다른 새 ID로 다시 보낼 수 있습니다. 원래 취소 ID를 사용한 자동 재전송(ACK 유실 복구)과 구분하며, 연결 끊김 체험 플래그는 새 요청으로 복사하지 않습니다. 저장소 단위 테스트는 ID 재사용/충돌 거부, 중복 클릭 방지, 전송 중 요청 보호와 남은 순서, 새 요청 재취소까지 확인합니다.

수정 후 다시 보내도 취소 기록의 원문과 이전 ID는 그대로 남습니다. 새 요청에 수정한 본문을 저장하고, 기록에는 다시 보낸 내용과 새 ID를 함께 표시합니다. 입력만 하는 동안 요청 ID를 만들거나 대기열/DB를 수정하지 않습니다. 수신·읽음 이벤트가 도착해도 편집 중인 입력란을 교체하지 않습니다. `node scripts/test-client-queue.mjs`에서 잘못된 입력의 원자성, 여러 줄 본문, 진행 중인 요청 보호, 새로고침 이후 ID/내용/순서 유지를 검사합니다.
