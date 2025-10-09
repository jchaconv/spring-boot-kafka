package dev.vilelo.dispatch.service;

import dev.vilelo.dispatch.enums.OrderStatus;
import dev.vilelo.dispatch.message.OrderCreated;
import dev.vilelo.dispatch.message.OrderDispatched;
import dev.vilelo.dispatch.message.OrderTracked;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Date;


@Service
@RequiredArgsConstructor
public class DispatchService {

    private final KafkaTemplate<String, Object> kafkaProducer;
    private static final String ORDER_DISPATCHED_TOPIC = "order.dispatched.topic";
    private static final String ORDER_TRACKING_TOPIC = "dispatched.tracking.topic";


    public void process(OrderCreated orderCreated) throws Exception {

        OrderDispatched orderDispatched = OrderDispatched.builder()
                .orderId(orderCreated.getOrderId())
                .build();

        //making sync this process with get()
        kafkaProducer.send(ORDER_DISPATCHED_TOPIC, orderDispatched).get();

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
