package dev.portfolio.chat;
import java.util.*;
import org.springframework.web.bind.annotation.*;
@RestController
public class ChatController {
    private final ChatService service;
    ChatController(ChatService service) { this.service = service; }
    @GetMapping("/api/rooms") public List<Map<String, String>> rooms() { return List.of(Map.of("id", "lobby", "name", "라운지"), Map.of("id", "design", "name", "설계 이야기")); }
    @GetMapping("/api/rooms/{roomId}/messages") public List<ChatService.MessageView> history(@PathVariable String roomId) { return service.history(roomId); }
    @ExceptionHandler(IllegalArgumentException.class) @ResponseStatus(org.springframework.http.HttpStatus.BAD_REQUEST)
    public Map<String, String> invalid(IllegalArgumentException error) { return Map.of("detail", error.getMessage()); }
}
