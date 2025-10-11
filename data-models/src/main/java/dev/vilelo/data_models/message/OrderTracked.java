package dev.vilelo.data_models.message;

import dev.vilelo.data_models.enums.OrderStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderTracked {

    private UUID orderId;
    private Date creationDate;
    private OrderStatus orderStatus;
    private String item;

}

