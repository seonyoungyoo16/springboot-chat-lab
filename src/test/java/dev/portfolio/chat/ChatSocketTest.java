package dev.portfolio.chat;
import com.fasterxml.jackson.databind.*;
import java.net.*;
import java.net.http.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "spring.datasource.url=jdbc:h2:mem:chat-test;DB_CLOSE_DELAY=-1")
class ChatSocketTest {
    @LocalServerPort int port;
    @Autowired ObjectMapper json;
    @Autowired MessageRepository repository;
    class Peer implements WebSocket.Listener, AutoCloseable {
        WebSocket socket; final BlockingQueue<JsonNode> events = new LinkedBlockingQueue<>(); final StringBuilder buffer = new StringBuilder();
        Peer() { socket = HttpClient.newHttpClient().newWebSocketBuilder().buildAsync(URI.create("ws://localhost:" + port + "/ws/chat"), this).join(); }
        @Override public void onOpen(WebSocket socket) { socket.request(1); }
        @Override public CompletionStage<?> onText(WebSocket socket, CharSequence data, boolean last) { buffer.append(data); if(last){try{events.add(json.readTree(buffer.toString()));}catch(Exception e){throw new RuntimeException(e);}buffer.setLength(0);}socket.request(1);return null; }
        void raw(String text) { socket.sendText(text, true).join(); }
        void send(Object data) throws Exception { raw(json.writeValueAsString(data)); }
        JsonNode await(String type) throws Exception { long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);while(System.nanoTime()<until){var e=events.poll(100,TimeUnit.MILLISECONDS);if(e!=null&&e.path("type").asText().equals(type))return e;}throw new AssertionError("No event: "+type); }
        JsonNode join(String room, String client) throws Exception { send(Map.of("type","join","roomId",room,"clientId",client,"name",client));return await("joined"); }
        boolean getsMessage() throws Exception { long until=System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(300);while(System.nanoTime()<until){var e=events.poll(30,TimeUnit.MILLISECONDS);if(e!=null&&e.path("type").asText().equals("message"))return true;}return false; }
        @Override public void close() { socket.sendClose(WebSocket.NORMAL_CLOSURE,"test complete").join(); }
    }
    @Test void twoClientsRoomIsolationDuplicateAndReconnect() throws Exception {
        String client="a"+System.nanoTime(), request=UUID.randomUUID().toString();long id;
        try(var a=new Peer();var b=new Peer();var outsider=new Peer()){
            a.join("lobby",client);b.join("lobby","b"+System.nanoTime());outsider.join("design","c"+System.nanoTime());
            var event=Map.of("type","send","requestId",request,"body","서로 다른 연결의 실시간 메시지");a.send(event);
            var received=b.await("message").get("message");id=received.get("id").asLong();assertThat(received.get("body").asText()).contains("실시간");
            assertThat(a.await("message").get("message").get("id").asLong()).isEqualTo(id);assertThat(a.await("ack").get("duplicate").asBoolean()).isFalse();assertThat(outsider.getsMessage()).isFalse();
            a.send(event);assertThat(a.await("ack").get("duplicate").asBoolean()).isTrue();assertThat(b.getsMessage()).isFalse();
            assertThat(repository.findByClientIdAndRequestId(client,request)).isPresent();
        }
        try(var reconnect=new Peer()){
            var history=reconnect.join("lobby",client).get("messages");assertThat(history.findValuesAsText("id")).contains(String.valueOf(id));
            reconnect.send(Map.of("type","send","requestId",request,"body","재전송"));assertThat(reconnect.await("ack").get("message").get("id").asLong()).isEqualTo(id);
        }
    }
    @Test void invalidInputNeverWritesAndConnectionStaysUsable() throws Exception {
        long before=repository.count();
        try(var p=new Peer()){
            p.raw("not json");assertThat(p.await("error").get("code").asText()).isEqualTo("INVALID_INPUT");
            p.send(Map.of("type","send","requestId","early","body","입장 전"));p.await("error");
            p.send(Map.of("type","join","roomId","missing","clientId","bad","name","사람"));p.await("error");
            p.join("design","valid"+System.nanoTime());
            for(String body:List.of("   ","a".repeat(2001))){p.send(Map.of("type","send","requestId",UUID.randomUUID().toString(),"body",body));p.await("error");}
            p.send(Map.of("type","send","requestId","not valid!","body","내용"));p.await("error");assertThat(repository.count()).isEqualTo(before);
            p.send(Map.of("type","send","requestId",UUID.randomUUID().toString(),"body","정상 전송"));assertThat(p.await("message").get("message").get("body").asText()).isEqualTo("정상 전송");
        }
    }
    @Test void restHistoryAndRooms() throws Exception {
        var http=HttpClient.newHttpClient();var rooms=http.send(HttpRequest.newBuilder(URI.create("http://localhost:"+port+"/api/rooms")).build(),HttpResponse.BodyHandlers.ofString());
        assertThat(rooms.statusCode()).isEqualTo(200);assertThat(json.readTree(rooms.body()).size()).isEqualTo(2);
        var invalid=http.send(HttpRequest.newBuilder(URI.create("http://localhost:"+port+"/api/rooms/missing/messages")).build(),HttpResponse.BodyHandlers.ofString());assertThat(invalid.statusCode()).isEqualTo(400);
    }
    @Test void readReceiptsRequireExplicitReadAndSurviveReconnect() throws Exception {
        String sender="s"+System.nanoTime(),reader="r"+System.nanoTime();long lastId;
        try(var a=new Peer();var b=new Peer();var other=new Peer()){
            b.send(Map.of("type","read","upToId",1));b.await("error");
            a.join("lobby",sender);other.join("design","o"+System.nanoTime());
            a.send(Map.of("type","send","requestId",UUID.randomUUID().toString(),"body","아직 읽지 않은 첫 메시지"));
            long firstId=a.await("message").path("message").path("id").asLong();a.await("ack");
            a.send(Map.of("type","send","requestId",UUID.randomUUID().toString(),"body","오프라인 중 두 번째 메시지"));
            lastId=a.await("message").path("message").path("id").asLong();a.await("ack");
            var joined=b.join("lobby",reader);
            assertThat(joined.path("reads").findValuesAsText("clientId")).doesNotContain(reader);
            long count=repository.count();
            // Identity comes from the joined session, never from a supplied clientId.
            b.send(Map.of("type","read","upToId",lastId,"clientId",sender));
            var receipt=a.await("read").path("receipt");assertThat(receipt.path("clientId").asText()).isEqualTo(reader);assertThat(receipt.path("lastReadId").asLong()).isEqualTo(lastId);
            b.await("read");b.send(Map.of("type","read","upToId",firstId));
            assertThat(a.await("read").path("receipt").path("lastReadId").asLong()).isEqualTo(lastId);b.await("read");
            other.send(Map.of("type","send","requestId",UUID.randomUUID().toString(),"body","다른 방"));long foreignId=other.await("message").path("message").path("id").asLong();other.await("ack");
            for(Object invalid:List.of(Long.MAX_VALUE,-1L,foreignId,1.5)){b.send(Map.of("type","read","upToId",invalid));b.await("error");}
            assertThat(repository.count()).isEqualTo(count+1);
            assertThat(other.events.stream().noneMatch(e->e.path("type").asText().equals("read"))).isTrue();
        }
        try(var reconnect=new Peer()){
            var reads=reconnect.join("lobby",sender).path("reads");
            JsonNode stored=null;for(var receipt:reads)if(receipt.path("clientId").asText().equals(reader))stored=receipt;
            assertThat(stored).isNotNull();assertThat(stored.path("lastReadId").asLong()).isEqualTo(lastId);
        }
    }
}
