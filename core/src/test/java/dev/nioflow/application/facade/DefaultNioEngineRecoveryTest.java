package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.Decision;
import dev.nioflow.core.model.Recovery;
import dev.nioflow.core.model.Retry;
import dev.nioflow.core.model.Stage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
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
     * A stage may fail with a CompletionException wrapping the real cause —
     * a CompletableFuture-shaped failure the engine itself produces (a retried
     * stage that exhausts its attempts rethrows the last one that way), and the
     * shape the reactive facade uses for a Mono's checked failure.
     *
     * <p>recover() must see the CAUSE, not the wrapper — and it must see the
     * same thing whether or not the recovery happened to fuse with the stage.
     * The dispatched path unwraps; the fused one used to hand the wrapper over,
     * so a mono.timeout() reached recover() as a CompletionException while an
     * identical stage timeout reached it as a TimeoutException.
     */
    @Test
    void aFusedRecoverySeesTheCauseNotItsCompletionExceptionWrapper() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        var seen = new AtomicReference<Throwable>();
        flow.handle("boom", value -> {
                    throw new CompletionException(new IOException("connection reset"));
                })                                     // no timeout: fuses with the recovery
                .recover("net", error -> {
                    seen.set(error);
                    return -1;
                });

        assertEquals(-1, flow.just(1).execute());
        assertInstanceOf(IOException.class, seen.get());
    }

    @Test
    void aDispatchedRecoverySeesTheSameCause() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        var seen = new AtomicReference<Throwable>();
        flow.handle("boom", value -> {
                    throw new CompletionException(new IOException("connection reset"));
                }, Duration.ofSeconds(2))              // timeout stage: dispatched alone, recovery unfused
                .recover("net", error -> {
                    seen.set(error);
                    return -1;
                });

        assertEquals(-1, flow.just(1).execute());
        assertInstanceOf(IOException.class, seen.get());
    }

    /**
     * Exhausting the retries rewraps the last failure, so a wrapped cause would
     * come out doubly wrapped: unwrapping only one layer still left recover()
     * holding a CompletionException.
     */
    @Test
    void aRetriedStageUnwrapsTheCauseItRewrapped() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        var seen = new AtomicReference<Throwable>();
        flow.handle("boom", value -> {
                    throw new CompletionException(new IOException("connection reset"));
                }, Retry.of(2, Duration.ofMillis(5)))
                .recover("net", error -> {
                    seen.set(error);
                    return -1;
                });

        assertEquals(-1, flow.just(1).execute());
        assertInstanceOf(IOException.class, seen.get());
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

    @Test
    void aRecoveryAfterExhaustedRetriesSeesTheStagesOwnException() {
        // No Reactor anywhere: a plain retried stage that runs out of attempts.
        // applyStage rethrows the last failure inside a CompletionException, so a
        // FUSED recovery used to get the wrapper instead of the IllegalStateException.
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        var seen = new AtomicReference<Throwable>();
        flow.handle("flaky", value -> {
                    throw new IllegalStateException("downstream is down");
                }, Retry.of(2, Duration.ofMillis(5)))
                .recover("net", error -> {
                    seen.set(error);
                    return -1;
                });

        assertEquals(-1, flow.just(1).execute());
        assertInstanceOf(IllegalStateException.class, seen.get());
        assertEquals("downstream is down", seen.get().getMessage());
    }
}
