package dev.portfolio.chat;
import java.util.*;
import org.springframework.data.jpa.repository.JpaRepository;
public interface MessageRepository extends JpaRepository<ChatMessage, Long> {
    List<ChatMessage> findTop100ByRoomIdOrderByIdDesc(String roomId);
    Optional<ChatMessage> findByClientIdAndRequestId(String clientId, String requestId);
}
