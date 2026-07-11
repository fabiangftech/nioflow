package dev.nioflow.application.facade;

import dev.nioflow.core.model.OverflowPolicy;
import dev.nioflow.core.model.Stage;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultNioEngineBackpressureTest {

    private static Stage identity() {
        return new Stage("identity", Function.identity(), false, null, null, List.of());
    }

    @Test
    void unboundedByDefaultAdmitsEverything() {
        var engine = new DefaultNioEngine();
        engine.append(identity());

        for (int i = 0; i < 100; i++) {
            engine.inject(i);
        }
        for (int i = 0; i < 100; i++) {
            assertEquals(i, engine.await());
        }
        engine.shutdown(Duration.ofMillis(100));
    }

    @Test
    void dropPolicyDiscardsBeyondCapacityAndReportsIt() {
        var engine = new DefaultNioEngine(2, OverflowPolicy.DROP);
        var dropped = new AtomicReference<Throwable>();
        engine.addErrorHandler(dropped::set);
        engine.append(identity());

        engine.inject("a");
        engine.inject("b");
        engine.inject("c"); // capacity 2: never runs, reported instead

        assertInstanceOf(RejectedExecutionException.class, dropped.get());
        assertEquals("a", engine.await());
        assertEquals("b", engine.await());
        // Only two results exist: the third value was rejected before running.
        assertThrows(IllegalStateException.class, () -> engine.await(Duration.ofMillis(100)));
        engine.shutdown(Duration.ofMillis(100));
    }

    @Test
    void failPolicyThrowsToTheProducer() {
        var engine = new DefaultNioEngine(1, OverflowPolicy.FAIL);
        engine.append(identity());

        engine.inject("a");

        assertThrows(RejectedExecutionException.class, () -> engine.inject("b"));
        assertEquals("a", engine.await());
        engine.shutdown(Duration.ofMillis(100));
    }

    @Test
    void blockPolicyParksTheProducerUntilAwaitFreesASlot() throws Exception {
        var engine = new DefaultNioEngine(1, OverflowPolicy.BLOCK);
        engine.append(identity());

        engine.inject("a"); // occupies the single slot until awaited

        var secondInjected = new AtomicBoolean(false);
        var producer = Thread.ofVirtual().start(() -> {
            engine.inject("b"); // must park: no slot available
            secondInjected.set(true);
        });

        Thread.sleep(150);
        assertFalse(secondInjected.get(), "producer should be parked while the slot is taken");

        assertEquals("a", engine.await()); // frees the slot: the producer resumes
        producer.join();
        assertEquals("b", engine.await());
        engine.shutdown(Duration.ofMillis(100));
    }
}
