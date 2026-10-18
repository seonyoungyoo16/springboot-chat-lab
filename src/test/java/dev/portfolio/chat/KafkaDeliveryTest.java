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
            assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM chat_outbox",Long.class)).isEqualTo(3);
            a.send(Map.of("type","send","mode","kafka","requestId","kafka-0","body","대기 0"));
            assertThat(a.take("ack").path("duplicate").asBoolean()).isTrue();
            assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM chat_outbox",Long.class)).isEqualTo(3);
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
            assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM chat_deliveries",Long.class)).isEqualTo(3);
        }
    }
    @Test void invalidModeRollsBackBeforeAnyMessageOrOutboxWrite()throws Exception{
        long before=jdbc.queryForObject("SELECT COUNT(*) FROM chat_messages",Long.class);
        try(var a=new Peer("invalid-kafka","design")){
            a.send(Map.of("type","send","mode","unknown","requestId","bad-mode","body","내용"));assertThat(a.take("error").path("code").asText()).isEqualTo("INVALID_INPUT");
        }
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM chat_messages",Long.class)).isEqualTo(before);
    }
}
