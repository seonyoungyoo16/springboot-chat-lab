package dev.portfolio.chat;

import com.fasterxml.jackson.databind.*;
import java.net.URI;
import java.net.http.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import java.time.Duration;

@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.RANDOM_PORT,properties={"spring.datasource.url=jdbc:h2:mem:kafka-test;DB_CLOSE_DELAY=-1","chat.kafka.enabled=true","chat.demo.controls-enabled=true","chat.kafka.publish-delay=100","logging.level.org.apache.kafka=ERROR","logging.level.kafka=ERROR"})
@EmbeddedKafka(partitions=2,topics="chat-delivery",kraft=true)
@DirtiesContext
class KafkaDeliveryTest {
    @LocalServerPort int port;
    @Autowired ObjectMapper json;
    @Autowired KafkaDelivery delivery;
    @Autowired JdbcTemplate jdbc;
    @Autowired KafkaTemplate<String,String> kafka;
    @Autowired org.springframework.kafka.config.KafkaListenerEndpointRegistry registry;
    @Autowired ChatService service;
    @Autowired org.springframework.context.ApplicationEventPublisher events;
    class Peer implements WebSocket.Listener,AutoCloseable {
        final BlockingQueue<JsonNode> events=new LinkedBlockingQueue<>();final StringBuilder buffer=new StringBuilder();WebSocket socket;
        Peer(String client,String room)throws Exception{socket=HttpClient.newHttpClient().newWebSocketBuilder().buildAsync(URI.create("ws://localhost:"+port+"/ws/chat"),this).join();send(Map.of("type","join","clientId",client,"roomId",room,"name",client));take("joined");}
        @Override public void onOpen(WebSocket socket){socket.request(1);}
        @Override public CompletionStage<?> onText(WebSocket ws,CharSequence text,boolean last){buffer.append(text);if(last){try{events.add(json.readTree(buffer.toString()));}catch(Exception e){throw new RuntimeException(e);}buffer.setLength(0);}ws.request(1);return null;}
        void send(Object frame)throws Exception{socket.sendText(json.writeValueAsString(frame),true).join();}
        JsonNode take(String type)throws Exception{long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);while(System.nanoTime()<until){var event=events.poll(100,TimeUnit.MILLISECONDS);if(event!=null&&event.path("type").asText().equals(type))return event;}throw new AssertionError("No event "+type);}
        boolean messageWithin300ms()throws Exception{long until=System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(300);while(System.nanoTime()<until){var e=events.poll(20,TimeUnit.MILLISECONDS);if(e!=null&&e.path("type").asText().equals("message"))return true;}return false;}
        @Override public void close(){socket.sendClose(1000,"done").join();}
    }
    @Test void selectableRoutesPauseAccumulateResumeAndDeduplicate()throws Exception{
        long beforeOutbox=jdbc.queryForObject("SELECT COUNT(*) FROM chat_outbox",Long.class),beforeProcessed=jdbc.queryForObject("SELECT COUNT(*) FROM chat_deliveries",Long.class);
        delivery.setRunning(false);
        await().atMost(Duration.ofSeconds(10)).until(()->Boolean.TRUE.equals(delivery.state().get("paused")));
        try(var a=new Peer("kafka-a","lobby");var b=new Peer("kafka-b","lobby");var outside=new Peer("kafka-out","design")){
            var ids=new ArrayList<Long>();
            for(int i=0;i<3;i++){
                a.send(Map.of("type","send","mode","kafka","requestId","kafka-"+i,"body","대기 "+i));
                var ack=a.take("ack");ids.add(ack.path("message").path("id").asLong());
                assertThat(ack.path("message").path("deliveryMode").asText()).isEqualTo("kafka");
            }
            await().atMost(Duration.ofSeconds(15)).until(()->((Number)delivery.state().get("queued")).longValue()==3);
            assertThat(b.messageWithin300ms()).isFalse();
            assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM chat_outbox",Long.class)).isEqualTo(beforeOutbox+3);
            a.send(Map.of("type","send","mode","kafka","requestId","kafka-0","body","대기 0"));
            assertThat(a.take("ack").path("duplicate").asBoolean()).isTrue();
            assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM chat_outbox",Long.class)).isEqualTo(beforeOutbox+3);
            // Direct delivery bypasses the stopped Kafka consumer.
            a.send(Map.of("type","send","mode","direct","requestId","direct-1","body","직접 전달"));
            var direct=b.take("message").path("message");assertThat(direct.path("body").asText()).isEqualTo("직접 전달");a.take("ack");
            b.send(Map.of("type","read","upToId",direct.path("id").asLong()));
            assertThat(a.take("read").path("receipt").path("deliveryMode").asText()).isEqualTo("direct");
            assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM chat_mode_reads WHERE delivery_mode='kafka'",Long.class)).isZero();
            delivery.setRunning(true);
            var received=new ArrayList<Long>();for(int i=0;i<3;i++)received.add(b.take("message").path("message").path("id").asLong());
            assertThat(received).containsExactlyElementsOf(ids);
            b.send(Map.of("type","read","upToId",ids.getLast()));
            var kafkaRead=a.take("read").path("receipt");assertThat(kafkaRead.path("deliveryMode").asText()).isEqualTo("kafka");
            assertThat(kafkaRead.path("lastReadId").asLong()).isLessThan(direct.path("id").asLong());
            await().atMost(Duration.ofSeconds(10)).until(()->((Number)delivery.state().get("queued")).longValue()==0);
            assertThat(outside.messageWithin300ms()).isFalse();
            // Relay retries can append the same business event again: consumer dedup still applies.
            kafka.send(KafkaDelivery.TOPIC,0,"lobby",String.valueOf(ids.getFirst())).get(5,TimeUnit.SECONDS);
            assertThat(b.messageWithin300ms()).isFalse();
            assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM chat_deliveries",Long.class)).isEqualTo(beforeProcessed+3);
        }
    }
    void control(Map<String,Boolean> body)throws Exception{
        var request=HttpRequest.newBuilder(URI.create("http://localhost:"+port+"/api/demo/delivery")).header("Content-Type","application/json").PUT(HttpRequest.BodyPublishers.ofString(json.writeValueAsString(body))).build();
        assertThat(HttpClient.newHttpClient().send(request,HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(200);
    }
    @Test void brokerOutageKeepsOutboxThenRecoveryHonorsStoppedWorker()throws Exception{
        control(Map.of("brokerRunning",false));delivery.setRunning(false);
        await().atMost(Duration.ofSeconds(10)).until(()->Boolean.TRUE.equals(delivery.state().get("paused")));
        try(var a=new Peer("outage-a","design");var b=new Peer("outage-b","design")){
            var ids=new ArrayList<Long>();
            for(int i=0;i<2;i++){
                a.send(Map.of("type","send","mode","kafka","requestId","outage-"+i,"body","Outbox 보관 "+i));
                ids.add(a.take("ack").path("message").path("id").asLong());
            }
            delivery.publishPending();
            assertThat(delivery.state().get("outboxPending")).isEqualTo(2L);
            assertThat(delivery.state().get("queued")).isEqualTo(0L);
            assertThat(b.messageWithin300ms()).isFalse();
            a.send(Map.of("type","send","mode","kafka","requestId","outage-0","body","Outbox 보관 0"));assertThat(a.take("ack").path("duplicate").asBoolean()).isTrue();
            assertThat(delivery.state().get("outboxPending")).isEqualTo(2L);
            control(Map.of("brokerRunning",true));
            await().atMost(Duration.ofSeconds(15)).until(()->((Number)delivery.state().get("outboxPending")).longValue()==0);
            assertThat(delivery.state().get("running")).isEqualTo(false);
            assertThat(delivery.state().get("queued")).isEqualTo(2L);
            assertThat(b.messageWithin300ms()).isFalse();
            delivery.setRunning(true);
            assertThat(b.take("message").path("message").path("id").asLong()).isEqualTo(ids.getFirst());
            assertThat(b.take("message").path("message").path("id").asLong()).isEqualTo(ids.getLast());
            await().atMost(Duration.ofSeconds(10)).until(()->((Number)delivery.state().get("queued")).longValue()==0);
            assertThat(b.messageWithin300ms()).isFalse();
        }finally{delivery.setBrokerRunning(true);delivery.setRunning(true);}
    }
    @Test void producerSendFailureRetainsUnpublishedRowAndRetriesAfterRecovery()throws Exception{
        delivery.setRunning(false);delivery.setBrokerRunning(false);
        // Inject a send failure; recovery uses the real Kafka producer and embedded broker.
        KafkaTemplate<String,String> failing=mock(KafkaTemplate.class);
        var failedPublisher=new KafkaDelivery(jdbc,failing,registry,service,events);
        doThrow(new org.springframework.kafka.KafkaException("injected outage")).when(failing).send(eq(KafkaDelivery.TOPIC),anyInt(),anyString(),anyString());
        try(var a=new Peer("failed-a","design");var b=new Peer("failed-b","design")){
            a.send(Map.of("type","send","mode","kafka","requestId","send-failure","body","발행 실패 후 복구"));long id=a.take("ack").path("message").path("id").asLong();
            failedPublisher.publishPending();assertThat(failedPublisher.state().get("publishError")).isEqualTo("KafkaException");
            assertThat(jdbc.queryForObject("SELECT published FROM chat_outbox WHERE message_id=?",Boolean.class,id)).isFalse();
            assertThat(b.messageWithin300ms()).isFalse();
            delivery.setBrokerRunning(true);
            await().atMost(Duration.ofSeconds(15)).until(()->jdbc.queryForObject("SELECT published FROM chat_outbox WHERE message_id=?",Boolean.class,id));
            delivery.setRunning(true);assertThat(b.take("message").path("message").path("id").asLong()).isEqualTo(id);
            await().atMost(Duration.ofSeconds(10)).until(()->((Number)delivery.state().get("queued")).longValue()==0);
        }finally{delivery.setBrokerRunning(true);delivery.setRunning(true);}
    }
    @Test void databaseOutageKeepsRequestWithoutAckThenStoresAndDeliversOnceInBothModes()throws Exception{
        try(var a=new Peer("db-a","design");var b=new Peer("db-b","design")){
            for(String mode:List.of("direct","kafka")){
                String requestId="db-retry-"+mode;
                var request=Map.of("type","send","mode",mode,"requestId",requestId,"body","DB 복구 "+mode);
                control(Map.of("databaseRunning",false));
                a.send(request);
                var failure=a.take("error");
                assertThat(failure.path("code").asText()).isEqualTo("DB_UNAVAILABLE");
                assertThat(failure.path("retryable").asBoolean()).isTrue();
                assertThat(failure.path("requestId").asText()).isEqualTo(requestId);
                assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM chat_messages WHERE client_id='db-a' AND request_id=?",Long.class,requestId)).isZero();
                assertThat(b.messageWithin300ms()).isFalse();
                assertThat(a.events.stream().noneMatch(e->e.path("type").asText().equals("ack"))).isTrue();
                // The control endpoint remains usable without reading the unavailable DB.
                var state=HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create("http://localhost:"+port+"/api/demo/delivery")).GET().build(),HttpResponse.BodyHandlers.ofString());
                assertThat(state.statusCode()).isEqualTo(200);assertThat(json.readTree(state.body()).path("databaseRunning").asBoolean()).isFalse();
                control(Map.of("databaseRunning",true));
                a.send(request);var ack=a.take("ack");long id=ack.path("message").path("id").asLong();
                assertThat(ack.path("duplicate").asBoolean()).isFalse();
                assertThat(ack.path("message").path("requestId").asText()).isEqualTo(requestId);
                assertThat(b.take("message").path("message").path("id").asLong()).isEqualTo(id);
                a.send(request);assertThat(a.take("ack").path("duplicate").asBoolean()).isTrue();
                assertThat(b.messageWithin300ms()).isFalse();
                assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM chat_messages WHERE client_id='db-a' AND request_id=?",Long.class,requestId)).isEqualTo(1L);
                if(mode.equals("kafka"))await().atMost(Duration.ofSeconds(10)).until(()->jdbc.queryForObject("SELECT COUNT(*) FROM chat_deliveries WHERE message_id=?",Long.class,id)==1L);
            }
        }finally{control(Map.of("databaseRunning",true));}
    }
    public static class FailOutboxOnce implements org.h2.api.Trigger {
        static final java.util.concurrent.atomic.AtomicBoolean fail=new java.util.concurrent.atomic.AtomicBoolean();
        @Override public void fire(java.sql.Connection connection,Object[] oldRow,Object[] newRow)throws java.sql.SQLException{
            if(fail.getAndSet(false))throw new java.sql.SQLNonTransientConnectionException("injected write failure","08006");
        }
    }
    @Test void actualOutboxWriteExceptionRollsBackMessageAndRetryUsesOriginalId()throws Exception{
        jdbc.execute("CREATE TRIGGER fail_outbox BEFORE INSERT ON chat_outbox FOR EACH ROW CALL 'dev.portfolio.chat.KafkaDeliveryTest$FailOutboxOnce'");
        try(var a=new Peer("db-tx-a","design");var b=new Peer("db-tx-b","design")){
            FailOutboxOnce.fail.set(true);
            var request=Map.of("type","send","mode","kafka","requestId","db-tx-retry","body","트랜잭션 롤백 후 재시도");
            a.send(request);assertThat(a.take("error").path("code").asText()).isEqualTo("DB_UNAVAILABLE");
            assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM chat_messages WHERE client_id='db-tx-a'",Long.class)).isZero();
            assertThat(b.messageWithin300ms()).isFalse();
            a.send(request);long id=a.take("ack").path("message").path("id").asLong();
            assertThat(b.take("message").path("message").path("id").asLong()).isEqualTo(id);
            a.send(request);assertThat(a.take("ack").path("duplicate").asBoolean()).isTrue();
            assertThat(b.messageWithin300ms()).isFalse();
            assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM chat_messages WHERE client_id='db-tx-a'",Long.class)).isEqualTo(1L);
            assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM chat_outbox WHERE message_id=?",Long.class,id)).isEqualTo(1L);
            await().atMost(Duration.ofSeconds(10)).until(()->jdbc.queryForObject("SELECT COUNT(*) FROM chat_deliveries WHERE message_id=?",Long.class,id)==1L);
        }finally{jdbc.execute("DROP TRIGGER fail_outbox");FailOutboxOnce.fail.set(false);}
    }
    @Test void invalidModeRollsBackBeforeAnyMessageOrOutboxWrite()throws Exception{
        long before=jdbc.queryForObject("SELECT COUNT(*) FROM chat_messages",Long.class);
        try(var a=new Peer("invalid-kafka","design")){
            a.send(Map.of("type","send","mode","unknown","requestId","bad-mode","body","내용"));assertThat(a.take("error").path("code").asText()).isEqualTo("INVALID_INPUT");
        }
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM chat_messages",Long.class)).isEqualTo(before);
    }
}
