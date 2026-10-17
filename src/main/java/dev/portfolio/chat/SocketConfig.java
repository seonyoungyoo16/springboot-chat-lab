package dev.portfolio.chat;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.*;
@Configuration
@EnableWebSocket
public class SocketConfig implements WebSocketConfigurer {
    private final ChatSocket socket;
    SocketConfig(ChatSocket socket) { this.socket = socket; }
    @Override public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(socket, "/ws/chat"); // Same-origin browser clients only.
    }
}
