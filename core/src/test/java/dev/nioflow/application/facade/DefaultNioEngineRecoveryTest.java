package dev.nioflow.application.facade;

import dev.nioflow.core.model.Decision;
import dev.nioflow.core.model.Recovery;
import dev.nioflow.core.model.Stage;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultNioEngineRecoveryTest extends EngineTestSupport {

    @Test
    void recoveryHandlesStageFailure() {
        engine.append(stage("boom", value -> {
            throw new IllegalStateException("boom");
        }));
        engine.append(new Recovery("recovery", error -> "recovered:" + error.getMessage(), List.of()));

        assertEquals("recovered:boom", engine.call("in", new ConcurrentHashMap<>()).join());
    }

    @Test
    void stageTimeoutTriggersRecovery() {
        engine.append(new Stage("sleepy", value -> {
            try {
                Thread.sleep(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return value;
        }, false, Duration.ofMillis(50), null, List.of()));
        engine.append(new Recovery("recovery", error -> "timed-out", List.of()));

        assertEquals("timed-out", engine.call("in", new ConcurrentHashMap<>()).join());
    }

    @Test
    void throwingPredicateCanBeRecovered() {
        engine.append(new Decision(value -> {
            throw new IllegalStateException("boom");
        }, engine.nextDecision(), List.of()));
        engine.append(new Recovery("recovery", error -> "recovered:" + error.getMessage(), List.of()));

        assertEquals("recovered:boom", engine.call("x", new ConcurrentHashMap<>()).join());
    }

    @Test
    void failureWithoutRecoveryReachesErrorHandlers() {
        var seen = new AtomicReference<Throwable>();
        engine.addErrorHandler(seen::set);
        engine.append(stage("boom", value -> {
            throw new IllegalStateException("boom");
        }));

        CompletableFuture<Object> result = engine.call("in", new ConcurrentHashMap<>());

        assertThrows(CompletionException.class, result::join);
        assertInstanceOf(IllegalStateException.class, seen.get());
    }
}
