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
    private final DatabaseAvailability database;
    DeliveryController(ObjectProvider<KafkaDelivery> delivery,DatabaseAvailability database,@Value("${chat.demo.controls-enabled:false}") boolean controls){this.delivery=delivery;this.controls=controls;this.database=database;}
    @GetMapping Map<String,Object> state(){var worker=delivery.getIfAvailable();return withControls(worker==null?Map.of("enabled",false,"running",false,"queued",0,"outboxPending",0):worker.state());}
    private Map<String,Object> withControls(Map<String,Object> state){var result=new java.util.HashMap<>(state);result.put("controls",controls);result.put("databaseRunning",database.isRunning());result.put("databaseFaultSimulation",true);return result;}
    record Switch(Boolean running,Boolean brokerRunning,Boolean databaseRunning) {}
    @PutMapping Map<String,Object> toggle(@RequestBody Switch request){
        if(!controls)throw new ResponseStatusException(HttpStatus.FORBIDDEN,"Demo controls disabled");
        var worker=delivery.getIfAvailable();
        if(request.running()==null&&request.brokerRunning()==null&&request.databaseRunning()==null)throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Specify an availability field");
        if(worker==null&&(request.running()!=null||request.brokerRunning()!=null))throw new ResponseStatusException(HttpStatus.CONFLICT,"Kafka mode disabled");
        if(request.databaseRunning()!=null){database.setRunning(request.databaseRunning());if(worker!=null)worker.applyAvailability();}
        if(request.running()!=null)worker.setRunning(request.running());
        if(request.brokerRunning()!=null)worker.setBrokerRunning(request.brokerRunning());
        return state();
    }
}
