package dev.nioflow.springbootwithnioflow.service;

import dev.nioflow.springbootwithnioflow.model.Order;
import org.springframework.stereotype.Service;

@Service
public class ShippingService {

    public Order express(Order order) {
        return order.withShipping("EXPRESS", 0);
    }

    public Order standard(Order order) {
        return order.withShipping("STANDARD", 9.99);
    }

    public Order economy(Order order) {
        return order.withShipping("ECONOMY", 4.99);
    }
}
