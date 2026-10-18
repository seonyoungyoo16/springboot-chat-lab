package dev.portfolio.chat;

import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/** Single-node demo: the consumer is a separately stoppable delivery role in this JVM. */
@Component
@ConditionalOnProperty(name="chat.kafka.enabled", havingValue="true")
public class KafkaDelivery {
    public static final String TOPIC="chat-delivery", GROUP="chat-delivery-worker";
    public record Ready(ChatService.MessageView message) {}
    private final JdbcTemplate jdbc;
    private final KafkaTemplate<String,String> kafka;
    private final KafkaListenerEndpointRegistry registry;
    private final ChatService service;
    private final ApplicationEventPublisher events;
    KafkaDelivery(JdbcTemplate jdbc, KafkaTemplate<String,String> kafka, KafkaListenerEndpointRegistry registry, ChatService service, ApplicationEventPublisher events) {
        this.jdbc=jdbc;this.kafka=kafka;this.registry=registry;this.service=service;this.events=events;
    }
    @Bean NewTopic chatDeliveryTopic(){return TopicBuilder.name(TOPIC).partitions(2).replicas(1).build();}
    @Scheduled(fixedDelayString="${chat.kafka.publish-delay:500}")
    public void publishPending(){
        var rows=jdbc.queryForList("SELECT o.message_id, m.room_id FROM chat_outbox o JOIN chat_messages m ON m.id=o.message_id WHERE o.published=FALSE ORDER BY o.message_id LIMIT 100");
        for(var row:rows){
            long id=((Number)row.get("message_id")).longValue();String room=(String)row.get("room_id");
            try {
                // Stable room partition and ordered relay preserve this demo's per-room append order.
                var result=kafka.send(TOPIC, room.equals("lobby")?0:1, room, String.valueOf(id)).get(5,TimeUnit.SECONDS);
                jdbc.update("UPDATE chat_outbox SET published=TRUE, kafka_partition=?, kafka_offset=? WHERE message_id=?",result.getRecordMetadata().partition(),result.getRecordMetadata().offset(),id);
            } catch(Exception unavailable){if(unavailable instanceof InterruptedException)Thread.currentThread().interrupt();return;}
        }
    }
    @KafkaListener(id=GROUP, topics=TOPIC, groupId=GROUP)
    @Transactional
    public void consume(ConsumerRecord<String,String> record){
        long id=Long.parseLong(record.value());
        if(jdbc.queryForObject("SELECT COUNT(*) FROM chat_deliveries WHERE message_id=?",Long.class,id)>0)return;
        var message=service.message(id);
        events.publishEvent(new Ready(message));
        jdbc.update("INSERT INTO chat_deliveries(message_id, kafka_partition, kafka_offset) VALUES (?,?,?)",id,record.partition(),record.offset());
        // Container commits its Kafka offset after this listener and its DB transaction succeed.
    }
    public void setRunning(boolean running){
        var consumer=registry.getListenerContainer(GROUP);
        if(running)consumer.resume();else consumer.pause();
    }
    public Map<String,Object> state(){
        var consumer=registry.getListenerContainer(GROUP);
        long unpublished=jdbc.queryForObject("SELECT COUNT(*) FROM chat_outbox WHERE published=FALSE",Long.class);
        long queued=jdbc.queryForObject("SELECT COUNT(*) FROM chat_outbox o WHERE published=TRUE AND NOT EXISTS (SELECT 1 FROM chat_deliveries d WHERE d.message_id=o.message_id)",Long.class);
        var pending=jdbc.queryForList("SELECT m.id, m.room_id, m.body, o.published, o.kafka_partition, o.kafka_offset FROM chat_outbox o JOIN chat_messages m ON m.id=o.message_id WHERE NOT EXISTS (SELECT 1 FROM chat_deliveries d WHERE d.message_id=o.message_id) ORDER BY m.id LIMIT 100");
        return Map.of("enabled",true,"running",!consumer.isPauseRequested(),"paused",consumer.isContainerPaused(),"outboxPending",unpublished,"queued",queued,"pending",pending,"topic",TOPIC);
    }
}
