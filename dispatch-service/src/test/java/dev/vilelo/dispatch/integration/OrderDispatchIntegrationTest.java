package dev.vilelo.dispatch.integration;


import dev.vilelo.data_models.message.OrderTracked;
import dev.vilelo.dispatch.DispatchConfiguration;
import dev.vilelo.dispatch.message.OrderCreated;
import dev.vilelo.dispatch.message.OrderDispatched;
import dev.vilelo.dispatch.util.TestEventData;
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
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.UUID.randomUUID;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;


@Slf4j
@DirtiesContext
@ActiveProfiles("test")
@EmbeddedKafka(controlledShutdown = true)
@SpringBootTest(classes = {DispatchConfiguration.class})
class OrderDispatchIntegrationTest {

    private static final String ORDER_CREATED_TOPIC = "order.created.topic";
    private static final String ORDER_DISPATCHED_TOPIC = "order.dispatched.topic";
    private static final String DISPATCH_TRACKING_TOPIC = "dispatched.tracking.topic";


    @Autowired
    private KafkaTemplate kafkaTemplate;


    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;


    @Autowired
    private KafkaListenerEndpointRegistry registry;



    @Autowired
    private KafkaTestListener testListener;


    @Configuration
    static class TestConfig {


        @Bean
        public KafkaTestListener testListener() {
            return new KafkaTestListener();
        }


    }



    public static class KafkaTestListener {

        AtomicInteger dispatchPreparingCounter = new AtomicInteger(0);

        AtomicInteger orderDispatchedCounter = new AtomicInteger(0);

        @KafkaListener(groupId = "kafkaIntegrationTest", topics = DISPATCH_TRACKING_TOPIC)
        void receivedDispatchPreparing(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload OrderTracked orderTracked) {
            log.debug("Received DispatchPreparing - key: {} - payload; {}", key, orderTracked);
            assertThat(key, notNullValue());
            assertThat(orderTracked, notNullValue());
            dispatchPreparingCounter.incrementAndGet();
        }

        @KafkaListener(groupId = "kafkaIntegrationTest", topics = ORDER_DISPATCHED_TOPIC)
        void receivedDispatchPreparing(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload OrderDispatched orderDispatched) {
            log.debug("Received OrderDispatched - key: {} - payload: {}", key, orderDispatched);
            assertThat(key, notNullValue());
            assertThat(orderDispatched, notNullValue());
            orderDispatchedCounter.incrementAndGet();
        }


    }

    @BeforeEach
    void setUp() {

        testListener.dispatchPreparingCounter.set(0);
        testListener.orderDispatchedCounter.set(0);

        registry.getListenerContainers().forEach(container ->
                ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic())
        );


    }


    @Test
    void testOrderDispatchFlow() throws Exception {

        OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(randomUUID(), "my-test-item");

        String key = randomUUID().toString();

        sendMessage(ORDER_CREATED_TOPIC, key, orderCreated);

        /*
        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testListener.dispatchPreparingCounter::get, equalTo(1));
        */

        await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testListener.orderDispatchedCounter::get, equalTo(1));

    }



    private void sendMessage(String topic, String key, Object data) throws Exception {
        kafkaTemplate.send(MessageBuilder
                .withPayload(data)
                .setHeader(KafkaHeaders.KEY, key)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .build()).get();
    }





}
