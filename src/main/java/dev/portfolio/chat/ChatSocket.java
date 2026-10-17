package dev.portfolio.chat;
import com.fasterxml.jackson.databind.*;
import java.io.IOException;
import java.util.*;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.*;
import org.springframework.web.socket.handler.TextWebSocketHandler;

@Component
public class ChatSocket extends TextWebSocketHandler {
    private final ChatService service;
    private final ObjectMapper json;
    private final Map<String, Member> members = new HashMap<>();
    private record Member(WebSocketSession socket, String roomId, String clientId, String name) {}
    ChatSocket(ChatService service, ObjectMapper json) { this.service = service; this.json = json; }
    @Override public void afterConnectionEstablished(WebSocketSession socket) { socket.setTextMessageSizeLimit(16384); }
    // Single-process serialization keeps history -> live ordering and duplicate checks deterministic.
    @Override protected synchronized void handleTextMessage(WebSocketSession socket, TextMessage frame) throws Exception {
        try {
            JsonNode input = json.readTree(frame.getPayload());
            if (input == null || !input.isObject()) throw new IllegalArgumentException("JSON 객체가 필요합니다.");
            switch (input.path("type").asText()) {
                case "join" -> join(socket, input);
                case "send" -> send(socket, input);
                case "read" -> read(socket, input);
                default -> throw new IllegalArgumentException("지원하지 않는 메시지 유형입니다.");
            }
        } catch (com.fasterxml.jackson.core.JsonProcessingException | IllegalArgumentException error) {
            emit(socket, Map.of("type", "error", "code", "INVALID_INPUT", "detail", error instanceof IllegalArgumentException ? error.getMessage() : "올바른 JSON이 필요합니다."));
        }
    }
    private void join(WebSocketSession socket, JsonNode input) throws IOException {
        if (members.containsKey(socket.getId())) throw new IllegalArgumentException("이미 입장했습니다. 방 변경 시 다시 연결하세요.");
        String roomId = input.path("roomId").asText(), clientId = service.identifier(input.path("clientId").asText());
        String name = service.text(input.path("name").asText(), 20, "이름"); service.validateRoom(roomId);
        var history = service.history(roomId);
        members.put(socket.getId(), new Member(socket, roomId, clientId, name));
        emit(socket, Map.of("type", "joined", "roomId", roomId, "clientId", clientId, "messages", history, "reads", service.reads(roomId)));
        presence(roomId);
    }
    private void send(WebSocketSession socket, JsonNode input) throws IOException {
        var member = members.get(socket.getId());
        if (member == null) throw new IllegalArgumentException("먼저 채팅방에 입장해 주세요.");
        String requestId = input.path("requestId").asText();
        var saved = service.save(member.roomId(), member.clientId(), requestId, member.name(), input.path("body").asText());
        if (!saved.duplicate()) broadcast(member.roomId(), Map.of("type", "message", "message", saved.message()));
        emit(socket, Map.of("type", "ack", "requestId", requestId, "message", saved.message(), "duplicate", saved.duplicate()));
    }
    private void read(WebSocketSession socket, JsonNode input) {
        var member = members.get(socket.getId());
        if (member == null) throw new IllegalArgumentException("먼저 채팅방에 입장해 주세요.");
        if (!input.path("upToId").isIntegralNumber() || !input.path("upToId").canConvertToLong()) throw new IllegalArgumentException("메시지 ID가 필요합니다.");
        var receipt = service.markRead(member.roomId(), member.clientId(), input.path("upToId").asLong());
        broadcast(member.roomId(), Map.of("type", "read", "receipt", receipt));
    }
    private void emit(WebSocketSession socket, Object event) throws IOException {
        if (!socket.isOpen()) return;
        try { socket.sendMessage(new TextMessage(json.writeValueAsString(event))); }
        catch (IllegalStateException closedDuringSend) { if (socket.isOpen()) throw closedDuringSend; }
    }
    private void broadcast(String roomId, Object event) {
        for (var member : List.copyOf(members.values())) {
            if (!member.roomId().equals(roomId)) continue;
            try { emit(member.socket(), event); } catch (IOException | IllegalStateException error) { try { member.socket().close(CloseStatus.SERVER_ERROR); } catch (IOException ignored) {} }
        }
    }
    private void presence(String roomId) {
        var names = members.values().stream().filter(m -> m.roomId().equals(roomId)).map(Member::name).sorted().toList();
        broadcast(roomId, Map.of("type", "presence", "roomId", roomId, "names", names));
    }
    @Override public synchronized void afterConnectionClosed(WebSocketSession socket, CloseStatus status) {
        var member = members.remove(socket.getId()); if (member != null) presence(member.roomId());
    }
}
