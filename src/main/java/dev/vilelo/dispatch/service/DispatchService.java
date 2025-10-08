package dev.vilelo.dispatch.service;

import dev.vilelo.dispatch.message.OrderCreated;
import dev.vilelo.dispatch.message.OrderDispatched;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;


@Service
@RequiredArgsConstructor
public class DispatchService {

    private final KafkaTemplate<String, Object> kafkaProducer;
    private static final String ORDER_DISPATCHED_TOPIC = "order.dispatched.topic";


    public void process(OrderCreated orderCreated) throws Exception {

        OrderDispatched orderDispatched = OrderDispatched.builder()
                .orderId(orderCreated.getOrderId())
                .build();

        //making sync this process with get()
        kafkaProducer.send(ORDER_DISPATCHED_TOPIC, orderDispatched).get();

    }

    // This code should be useful to test String Deserialization
    /*
    public void process(String payload) {
        //no-op
    }
    */
}
