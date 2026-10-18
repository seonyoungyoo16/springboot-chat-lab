package dev.portfolio.chat;

import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
/** Local demo runner with an actual single-node Kafka broker, no Docker required. */
public class KafkaDemoLauncher {
    public static void main(String[] args){
        var broker=new EmbeddedKafkaKraftBroker(1,2,"chat-delivery");broker.afterPropertiesSet();
        Runtime.getRuntime().addShutdownHook(new Thread(broker::destroy));
        System.setProperty("spring.kafka.bootstrap-servers",broker.getBrokersAsString());
        System.setProperty("spring.datasource.url","jdbc:h2:mem:kafka-demo;DB_CLOSE_DELAY=-1");
        System.setProperty("chat.kafka.enabled","true");System.setProperty("chat.demo.controls-enabled","true");
        ChatApplication.main(args);
    }
}
