package dev.nioflow.springbootwithnioflow.service;

import dev.nioflow.springbootwithnioflow.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class NotificationService {

    // Background links: fire-and-forget, they never block nor fail the flow.

    public void notifyCustomer(Order order) {
        log.info("notify: email to {} — order {} via {} for {}",
                order.customer(), order.status(), order.shipping(), order.total());
    }

    public void notifyWarehouse(Order order) {
        log.info("notify: warehouse picking list for {} ({} items)", order.customer(), order.items());
    }
}
