package dev.portfolio.chat;
import java.time.Instant;
import java.util.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.jdbc.core.JdbcTemplate;
@Service
public class ChatService {
    private final MessageRepository messages;
    private final JdbcTemplate jdbc;
    ChatService(MessageRepository messages, JdbcTemplate jdbc) { this.messages = messages; this.jdbc = jdbc; }
    public record ReadView(String roomId, String clientId, long lastReadId) {}
    @Transactional(readOnly = true)
    public List<ReadView> reads(String roomId) {
        validateRoom(roomId);
        return jdbc.query("SELECT client_id, last_read_id FROM chat_reads WHERE room_id = ?", (row, index) -> new ReadView(roomId, row.getString(1), row.getLong(2)), roomId);
    }
    @Transactional
    public ReadView markRead(String roomId, String clientId, long upToId) {
        validateRoom(roomId); identifier(clientId);
        messages.findById(upToId).filter(message -> message.roomId.equals(roomId)).orElseThrow(() -> new IllegalArgumentException("현재 방의 저장된 메시지만 읽음 처리할 수 있습니다."));
        // ChatSocket serializes commands in this single-process demo; cursors never decrease.
        int updated = jdbc.update("UPDATE chat_reads SET last_read_id = GREATEST(last_read_id, ?) WHERE room_id = ? AND client_id = ?", upToId, roomId, clientId);
        if (updated == 0) jdbc.update("INSERT INTO chat_reads(room_id, client_id, last_read_id) VALUES (?, ?, ?)", roomId, clientId, upToId);
        long saved = jdbc.queryForObject("SELECT last_read_id FROM chat_reads WHERE room_id = ? AND client_id = ?", Long.class, roomId, clientId);
        return new ReadView(roomId, clientId, saved);
    }
    public record MessageView(Long id, String roomId, String clientId, String requestId, String sender, String body, Instant createdAt) {}
    public record Saved(MessageView message, boolean duplicate) {}
    public void validateRoom(String roomId) {
        if (!List.of("lobby", "design").contains(roomId)) throw new IllegalArgumentException("존재하지 않는 채팅방입니다.");
    }
    public String text(String value, int max, String label) {
        if (value == null || value.isBlank() || value.length() > max) throw new IllegalArgumentException(label + "은 1~" + max + "자입니다.");
        return value.trim();
    }
    public String identifier(String value) {
        if (value == null || !value.matches("[A-Za-z0-9_-]{1,80}")) throw new IllegalArgumentException("요청 식별자가 올바르지 않습니다.");
        return value;
    }
    private MessageView view(ChatMessage m) { return new MessageView(m.id, m.roomId, m.clientId, m.requestId, m.sender, m.body, m.createdAt); }
    @Transactional(readOnly = true)
    public List<MessageView> history(String roomId) {
        validateRoom(roomId);
        return messages.findTop100ByRoomIdOrderByIdDesc(roomId).stream().sorted(Comparator.comparing(m -> m.id)).map(this::view).toList();
    }
    @Transactional
    public Saved save(String roomId, String clientId, String requestId, String sender, String body) {
        validateRoom(roomId); identifier(clientId); identifier(requestId); text(sender, 20, "이름"); body = text(body, 2000, "메시지");
        var previous = messages.findByClientIdAndRequestId(clientId, requestId);
        if (previous.isPresent()) {
            if (!previous.get().roomId.equals(roomId)) throw new IllegalArgumentException("다른 방에서 사용한 요청 ID입니다.");
            return new Saved(view(previous.get()), true);
        }
        var message = messages.saveAndFlush(new ChatMessage(roomId, clientId, requestId, sender, body));
        return new Saved(view(message), false);
    }
}
