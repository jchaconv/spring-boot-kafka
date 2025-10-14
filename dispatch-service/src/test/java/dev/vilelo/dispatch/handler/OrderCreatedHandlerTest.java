package dev.vilelo.dispatch.handler;

import dev.vilelo.dispatch.message.OrderCreated;
import dev.vilelo.dispatch.service.DispatchService;
import dev.vilelo.dispatch.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.util.UUID.randomUUID;
import static org.mockito.Mockito.*;

class OrderCreatedHandlerTest {

    private OrderCreatedHandler handler;

    private DispatchService dispatchService;

    @BeforeEach
    void setUp() {
        dispatchService = mock(DispatchService.class);
        handler = new OrderCreatedHandler(dispatchService);
    }


    @Test
    void listen_Success() throws Exception {
        String key = randomUUID().toString();
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        handler.listen(0, key, testEvent);
        verify(dispatchService, atLeastOnce()).process(key, testEvent);
    }



    @Test
    void listen_ServiceThrowsException() throws Exception {
        String key = randomUUID().toString();
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        doThrow(new RuntimeException("Service failure")).when(dispatchService).process(key, testEvent);
        handler.listen(0, key, testEvent);
        verify(dispatchService, atLeastOnce()).process(key, testEvent);
    }



    // This code should be useful to test String Deserialization
    /*
    @Test
    void listen() {
        handler.listen("payload");
        verify(dispatchService, atLeastOnce()).process(anyString());
    }
    */



}