package dev.nioflow.unit;

import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.infrastructure.trace.LoggingTracer;
import dev.nioflow.unit.utils.CapturingLogger;
import org.junit.jupiter.api.Test;

import java.lang.System.Logger.Level;

import static org.junit.jupiter.api.Assertions.assertTrue;

class LoggingTracerTest {

    @Test
    void transitionsEndUpInTheLogger() {
        CapturingLogger logger = new CapturingLogger();

        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.trace(LoggingTracer.to(logger, Level.INFO))
                    .handle("double", x -> x * 2);

            defaultNioFlow.just(21);
            defaultNioFlow.join();
        }

        assertTrue(logger.lines.stream().anyMatch(line -> line.contains("value 0 injected: 21")));
        assertTrue(logger.lines.stream().anyMatch(line -> line.contains("handle[double]: 21 -> 42")));
        assertTrue(logger.lines.stream().anyMatch(line -> line.contains("value 0 completed: 42")));
    }
}
