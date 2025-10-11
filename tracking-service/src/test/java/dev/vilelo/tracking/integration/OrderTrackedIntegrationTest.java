package dev.vilelo.tracking.integration;


import dev.vilelo.data_models.enums.OrderStatus;
import dev.vilelo.data_models.message.OrderTracked;
import dev.vilelo.tracking.TrackingConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.UUID.randomUUID;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

@Slf4j
@DirtiesContext
@ActiveProfiles("test")
@EmbeddedKafka(controlledShutdown = true)
@SpringBootTest(classes = {TrackingConfiguration.class})
public class OrderTrackedIntegrationTest {


    @Autowired
    private KafkaTemplate kafkaTemplate;


    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;


    @Autowired
    private KafkaListenerEndpointRegistry registry;


    @Autowired
    private KafkaTestListener testListener;


    private static final String DISPATCH_TRACKING_TOPIC = "dispatched.tracking.topic";


    @Configuration
    static class TestConfig {


        @Bean
        public KafkaTestListener testListener() {
            return new KafkaTestListener();
        }


    }


    public static class KafkaTestListener {

        AtomicInteger dispatchedOrderCounter = new AtomicInteger(0);

        @KafkaListener(groupId = "kafkaIntegrationTest", topics = DISPATCH_TRACKING_TOPIC)
        void receivedDispatchedOrder(@Payload OrderTracked orderTracked) {
            log.debug("Received DispatchedOrder: {}", orderTracked);
            dispatchedOrderCounter.incrementAndGet();
        }


    }


    @BeforeEach
    void setUp() {

        testListener.dispatchedOrderCounter.set(0);

        registry.getListenerContainers().forEach(container ->
                ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic())
        );


    }


    @Test
    void testTrackedOrderFlow() throws Exception {

        OrderTracked orderTracked = OrderTracked.builder()
                .orderId(randomUUID())
                .orderStatus(OrderStatus.DISPATCHED)
                .item("Laptop AllienWare")
                .creationDate(new Date())
                .build();

        sendMessage(DISPATCH_TRACKING_TOPIC, orderTracked);


        await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testListener.dispatchedOrderCounter::get, equalTo(1));

    }


    private void sendMessage(String topic, Object data) throws Exception {
        kafkaTemplate.send(MessageBuilder
                .withPayload(data)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .build()).get();
    }












}
