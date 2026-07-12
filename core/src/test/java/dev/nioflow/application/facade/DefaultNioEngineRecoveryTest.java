package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
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

    /**
     * Recoveries are positional INSIDE a fused run too: when the first one
     * throws while handling the failure, the scan continues forward through
     * the run and the next recovery picks the new failure up.
     */
    @Test
    void aFusedRecoveryThatThrowsHandsTheFailureToTheNextOne() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("boom", value -> {
                    throw new IllegalStateException("boom");
                })
                .recover("broken", error -> {
                    throw new IllegalArgumentException("the recovery is broken too");
                })
                .recover("net", error -> error instanceof IllegalArgumentException ? -1 : -2)
                .handle("tail", value -> value * 10);

        assertEquals(-10, flow.just(1).execute());
    }

    /**
     * With no recovery left in the run, the failure escapes it and the boss
     * keeps scanning the rest of the chain — the segment already searched is
     * not searched twice, but nothing downstream is skipped either.
     */
    @Test
    void aFailureEscapesTheFusedRunAndFindsARecoveryDownstream() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("boom", value -> {
                    throw new IllegalStateException("boom");
                })
                .recover("broken", error -> {
                    throw new IllegalArgumentException("the recovery is broken too");
                })
                .handle("timed", value -> value, Duration.ofSeconds(2))   // breaks the fusion run
                .recover("net", error -> -1);

        assertEquals(-1, flow.just(1).execute());
    }

    /** Same positional scan on the dispatched (non-fused) recovery path. */
    @Test
    void aDispatchedRecoveryThatThrowsHandsTheFailureToTheNextOne() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("boom", value -> {
                    throw new IllegalStateException("boom");
                }, Duration.ofSeconds(2))                                  // timeout stage: dispatched alone
                .recover("broken", error -> {
                    throw new IllegalArgumentException("the recovery is broken too");
                })
                .handle("timed", value -> value, Duration.ofSeconds(2))    // keeps the recoveries unfused
                .recover("net", error -> -1);

        assertEquals(-1, flow.just(1).execute());
    }
}
