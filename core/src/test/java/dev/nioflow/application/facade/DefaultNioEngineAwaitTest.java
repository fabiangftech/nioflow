package dev.nioflow.application.facade;

import dev.nioflow.core.model.Filter;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * inject/await is the fire-and-forget pair: results queue up in order and the
 * bounded await() converts "nothing arrived in time" into a failure instead of
 * blocking forever.
 */
class DefaultNioEngineAwaitTest extends EngineTestSupport {

    @Test
    void boundedAwaitCollectsResultsInSubmissionOrder() {
        engine.append(stage("double", value -> (Integer) value * 2));

        engine.inject(1);
        engine.inject(2);

        assertEquals(2, engine.await(Duration.ofSeconds(2)));
        assertEquals(4, engine.await(Duration.ofSeconds(2)));
    }

    @Test
    void boundedAwaitFailsWhenNoResultArrivesInTime() {
        IllegalStateException empty = assertThrows(IllegalStateException.class,
                () -> engine.await(Duration.ofMillis(50)));

        assertTrue(empty.getMessage().contains("No result available"), empty::getMessage);
    }

    @Test
    void boundedAwaitFailsWhenTheStageItselfIsSlowerThanTheTimeout() {
        engine.append(stage("slow", value -> {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return value;
        }));

        engine.inject(1);

        assertThrows(IllegalStateException.class, () -> engine.await(Duration.ofMillis(100)));
    }

    @Test
    void boundedAwaitRethrowsTheExecutionFailure() {
        engine.append(stage("boom", value -> {
            throw new IllegalArgumentException("boom");
        }));

        engine.inject(1);

        CompletionException failure = assertThrows(CompletionException.class,
                () -> engine.await(Duration.ofSeconds(2)));
        assertEquals("boom", failure.getCause().getMessage());
    }

    @Test
    void aFilteredExecutionIsCollectedAsNullByBothAwaits() {
        engine.append(new Filter(value -> false, List.of()));

        engine.inject(1);
        assertNull(engine.await());

        engine.inject(2);
        assertNull(engine.await(Duration.ofSeconds(2)));
    }
}
