package dev.nioflow.springbootwithnioflow.service;

import dev.nioflow.springbootwithnioflow.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class InventoryService {

    private static final int MAX_STOCK_PER_ORDER = 100;

    public boolean hasStock(Order order) {
        return order.items() <= MAX_STOCK_PER_ORDER;
    }

    public Order reserve(Order order) {
        log.info("inventory: reserved {} items for {}", order.items(), order.customer());
        return order.withStatus("RESERVED");
    }
}
