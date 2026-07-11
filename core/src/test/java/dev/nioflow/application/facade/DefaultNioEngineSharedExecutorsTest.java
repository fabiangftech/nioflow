package dev.nioflow.application.facade;

import dev.nioflow.core.model.Decision;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultNioEngineSharedExecutorsTest extends EngineTestSupport {

    @Test
    void defaultEnginesShareTheBossPoolAndSurviveEachOthersShutdown() {
        var other = new DefaultNioEngine();
        var bossOfThis = new AtomicReference<String>();
        var bossOfOther = new AtomicReference<String>();
        engine.append(new Decision(value -> {
            bossOfThis.set(Thread.currentThread().getName());
            return true;
        }, engine.nextDecision(), List.of()));
        other.append(new Decision(value -> {
            bossOfOther.set(Thread.currentThread().getName());
            return true;
        }, other.nextDecision(), List.of()));

        engine.call("x", new ConcurrentHashMap<>()).join();
        // Shutting one engine down must never starve the others: executors are shared.
        engine.shutdown(Duration.ofMillis(100));
        assertEquals("y", other.call("y", new ConcurrentHashMap<>()).join());

        assertTrue(bossOfThis.get().startsWith("nio-flow-boss-"));
        assertTrue(bossOfOther.get().startsWith("nio-flow-boss-"));
    }
}
