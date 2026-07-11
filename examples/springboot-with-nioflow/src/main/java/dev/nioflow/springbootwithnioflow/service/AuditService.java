package dev.nioflow.springbootwithnioflow.service;

import dev.nioflow.springbootwithnioflow.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class AuditService {

    private final AtomicLong audited = new AtomicLong();

    // Runs as a background link: fire-and-forget, never blocks nor fails the flow.
    public void record(Order order) {
        audited.incrementAndGet();
        log.info("audit: order for {} status {} total {}", order.customer(), order.status(), order.total());
    }

    public long count() {
        return audited.get();
    }
}
