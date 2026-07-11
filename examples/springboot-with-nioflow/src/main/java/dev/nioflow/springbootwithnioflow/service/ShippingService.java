package dev.nioflow.springbootwithnioflow.service;

import dev.nioflow.springbootwithnioflow.model.Order;
import org.springframework.stereotype.Service;

@Service
public class ShippingService {

    private static final String HOME_COUNTRY = "US";
    private static final double CUSTOMS_FEE = 25.0;
    private static final double EXPRESS_THRESHOLD = 300;
    private static final double STANDARD_THRESHOLD = 100;

    public boolean isInternational(Order order) {
        return !HOME_COUNTRY.equals(order.country());
    }

    public Order addCustoms(Order order) {
        return order.withCustomsFee(CUSTOMS_FEE);
    }

    public Order international(Order order) {
        return order.withShipping("INTERNATIONAL", 49.99);
    }

    public boolean qualifiesForExpress(Order order) {
        return order.subtotal() >= EXPRESS_THRESHOLD;
    }

    public Order express(Order order) {
        return order.withShipping("EXPRESS", 0);
    }

    public boolean qualifiesForStandard(Order order) {
        return order.subtotal() >= STANDARD_THRESHOLD;
    }

    public Order standard(Order order) {
        return order.withShipping("STANDARD", 9.99);
    }

    public Order economy(Order order) {
        return order.withShipping("ECONOMY", 4.99);
    }
}
