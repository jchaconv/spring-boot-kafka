package dev.vilelo.dispatch.service;

import dev.vilelo.dispatch.message.OrderCreated;
import dev.vilelo.dispatch.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.util.UUID.randomUUID;

class DispatchServiceTest {

    private DispatchService service;

    @BeforeEach
    void setUp() {
        service = new DispatchService();
    }



    @Test
    void process() {
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        service.process(testEvent);
    }



    // This code should be useful to test String Deserialization
    /*
    @Test
    void process() {
        service.process("payload");
    }
    */



}