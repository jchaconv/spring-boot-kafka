package dev.vilelo.dispatch.handler;


import dev.vilelo.dispatch.service.DispatchService;
import dev.vilelo.dispatch.message.OrderCreated;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class OrderCreatedHandler {

    private final DispatchService dispatchService;


    @KafkaListener(
            id = "orderConsumerClient",
            topics = "order.created.topic",
            groupId = "dispatch.order.created.consumer",
            containerFactory = "kafkaListenerContainerFactory"  // from Spring Bean Config onwards
    )
    public void listen(@Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition,
                       @Header(KafkaHeaders.RECEIVED_KEY) String key,
                       @Payload OrderCreated payload) {
        log.info("Received message:: partition => {}", partition);
        log.info("message key: {} - payload: {}", key, payload);
        try {
            dispatchService.process(key, payload);
        } catch (Exception e) {
            log.info("Processing failure", e);
        }
    }



    /*
    @KafkaListener(
            id = "orderConsumerClient2",
            topics = "order.observed.topic",
            groupId = "dispatch.order.observed.consumer",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listenTrackedOrder(OrderCreated payload) {
        log.info("Received observed order: " + payload);
        try {
            dispatchService.processTrackedOrder(payload);
        } catch (Exception e) {
            log.info("Processing failure", e);
        }
    }
    */


    // This code should be useful to test String Deserialization
    /*
    @KafkaListener(
            id = "orderConsumerClient",
            topics = "order.created.topic",
            groupId = "dispatch.order.created.consumer"
    )
    public void listen(String payload) {
        log.info("Received payload: " + payload);
        dispatchService.process(payload);
    }
    */



}
