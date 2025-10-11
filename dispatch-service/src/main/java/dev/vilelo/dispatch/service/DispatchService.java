package dev.vilelo.dispatch.service;

import dev.vilelo.data_models.enums.OrderStatus;
import dev.vilelo.data_models.message.OrderTracked;
import dev.vilelo.dispatch.message.OrderCreated;
import dev.vilelo.dispatch.message.OrderDispatched;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.UUID;

import static java.util.UUID.randomUUID;


@Slf4j
@RequiredArgsConstructor
@Service
public class DispatchService {

    private final KafkaTemplate<String, Object> kafkaProducer;
    private static final String ORDER_DISPATCHED_TOPIC = "order.dispatched.topic";
    private static final String ORDER_TRACKING_TOPIC = "dispatched.tracking.topic";

    private static final UUID APPLICATION_ID = randomUUID();


    public void process(OrderCreated orderCreated) throws Exception {

        OrderDispatched orderDispatched = OrderDispatched.builder()
                .orderId(orderCreated.getOrderId())
                .processedById(APPLICATION_ID)
                .notes("Dispatched-" + orderCreated.getItem())
                .build();

        //making sync this process with get()
        kafkaProducer.send(ORDER_DISPATCHED_TOPIC, orderDispatched).get();

        log.info("Sent messages: orderId: {} - processedById: {}", orderCreated.getOrderId(), APPLICATION_ID);

    }

    public void processTrackedOrder(OrderCreated orderCreated) throws Exception {

        OrderTracked orderTracked = OrderTracked.builder()
                .orderId(orderCreated.getOrderId())
                .item(orderCreated.getItem())
                .orderStatus(orderCreated.getItem().contains("D-") ? OrderStatus.DISPATCHED : OrderStatus.CANCELED)
                .creationDate(new Date())
                .build();

        kafkaProducer.send(ORDER_TRACKING_TOPIC, orderTracked).get();

    }

    // This code should be useful to test String Deserialization
    /*
    public void process(String payload) {
        //no-op
    }
    */
}
