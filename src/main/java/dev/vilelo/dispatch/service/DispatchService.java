package dev.vilelo.dispatch.service;

import dev.vilelo.dispatch.message.OrderCreated;
import org.springframework.stereotype.Service;

@Service
public class DispatchService {


    public void process(OrderCreated payload) {

    }

    // This code should be useful to test String Deserialization
    /*
    public void process(String payload) {
        //no-op
    }
    */
}
