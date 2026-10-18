package dev.portfolio.chat;

import java.util.Map;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/demo/delivery")
class DeliveryController {
    private final ObjectProvider<KafkaDelivery> delivery;
    private final boolean controls;
    DeliveryController(ObjectProvider<KafkaDelivery> delivery,@Value("${chat.demo.controls-enabled:false}") boolean controls){this.delivery=delivery;this.controls=controls;}
    @GetMapping Map<String,Object> state(){var worker=delivery.getIfAvailable();return worker==null?Map.of("enabled",false,"running",false,"queued",0,"outboxPending",0,"controls",false):withControls(worker.state());}
    private Map<String,Object> withControls(Map<String,Object> state){var result=new java.util.HashMap<>(state);result.put("controls",controls);return result;}
    record Switch(Boolean running,Boolean brokerRunning) {}
    @PutMapping Map<String,Object> toggle(@RequestBody Switch request){
        if(!controls)throw new ResponseStatusException(HttpStatus.FORBIDDEN,"Demo controls disabled");
        var worker=delivery.getIfAvailable();if(worker==null)throw new ResponseStatusException(HttpStatus.CONFLICT,"Kafka mode disabled");
        if(request.running()==null&&request.brokerRunning()==null)throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Specify running or brokerRunning");
        if(request.running()!=null)worker.setRunning(request.running());
        if(request.brokerRunning()!=null)worker.setBrokerRunning(request.brokerRunning());
        return withControls(worker.state());
    }
}
