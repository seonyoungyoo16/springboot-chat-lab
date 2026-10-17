package dev.portfolio.chat;
import jakarta.persistence.*;
import java.time.Instant;
@Entity
@Table(name = "chat_messages", uniqueConstraints = @UniqueConstraint(name = "uq_chat_request", columnNames = {"client_id", "request_id"}))
public class ChatMessage {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY) Long id;
    @Column(name = "room_id", nullable = false, length = 30) String roomId;
    @Column(name = "client_id", nullable = false, length = 80) String clientId;
    @Column(name = "request_id", nullable = false, length = 80) String requestId;
    @Column(nullable = false, length = 20) String sender;
    @Column(nullable = false, length = 2000) String body;
    @Column(name = "created_at", nullable = false) Instant createdAt = Instant.now();
    protected ChatMessage() {}
    ChatMessage(String roomId, String clientId, String requestId, String sender, String body) {
        this.roomId = roomId; this.clientId = clientId; this.requestId = requestId; this.sender = sender; this.body = body;
    }
}
