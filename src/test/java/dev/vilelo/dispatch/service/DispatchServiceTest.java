package dev.vilelo.dispatch.service;

import dev.vilelo.dispatch.message.OrderCreated;
import dev.vilelo.dispatch.message.OrderDispatched;
import dev.vilelo.dispatch.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;



class DispatchServiceTest {

    private DispatchService service;
    private KafkaTemplate kafkaProducer;

    @BeforeEach
    void setUp() {
        kafkaProducer = mock(KafkaTemplate.class);
        service = new DispatchService(kafkaProducer);
    }



    @Test
    void process_Success() throws Exception {

        when(kafkaProducer.send(anyString(), any(OrderDispatched.class))).thenReturn(mock(CompletableFuture.class));
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        service.process(testEvent);
        verify(kafkaProducer, times(1)).send(eq("order.dispatched.topic"), any(OrderDispatched.class));
    }



    @Test
    void process_ProducerThrowsException() throws Exception {

        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());

        doThrow(new RuntimeException("Procedure failure")).when(kafkaProducer).send(eq("order.dispatched.topic"), any(OrderDispatched.class));

        Exception exception = assertThrows(RuntimeException.class, () -> service.process(testEvent));

        verify(kafkaProducer, times(1)).send(eq("order.dispatched.topic"), any(OrderDispatched.class));
        assertThat(exception.getMessage(), equalTo("Procedure failure"));
    }



    // This code should be useful to test String Deserialization
    /*
    @Test
    void process() {
        service.process("payload");
    }
    */



}