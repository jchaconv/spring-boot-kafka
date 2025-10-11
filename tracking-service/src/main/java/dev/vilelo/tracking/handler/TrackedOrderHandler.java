package dev.vilelo.tracking.handler;


import dev.vilelo.data_models.message.OrderTracked;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class TrackedOrderHandler {


    @KafkaListener(
            id = "trackedOrderConsumerClient",
            topics = "dispatched.tracking.topic",
            groupId = "tracking.order.observer.consumer",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(OrderTracked payload) {
        log.info("Received payload: {}", payload);
    }




}
