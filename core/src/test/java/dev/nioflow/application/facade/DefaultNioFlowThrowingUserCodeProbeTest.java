package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * RFC 0020 — adversarial bug-hunt on user code that throws where the happy-path
 * tests never look: a {@code recover} that itself throws, and a boss-side
 * predicate ({@code when}/{@code match}/{@code filter}) that throws. The
 * contract: a throwing recover is a fresh failure caught by the NEXT positional
 * recovery (never by itself), and a throwing boss predicate fails the value
 * through the recovery path — never the boss task (a dead boss would hang every
 * execution pinned to it). The potentially-hanging probes use {@code orTimeout}
 * so a boss-death bug surfaces as a failure, not an infinite hang.
 */
class DefaultNioFlowThrowingUserCodeProbeTest {

    @Test
    void aThrowingRecoverIsCaughtByTheNextRecoverNotItself() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        Integer result = flow.just(5)
                .handle("boom", value -> {
                    throw new IllegalStateException("first");
                })
                .recover("throws", error -> {
                    throw new IllegalArgumentException("in-recover");
                })
                .recover("net", error -> -1)
                .handle("tail", value -> value * 10)
                .execute();

        assertEquals(-10, result); // the net recover catches the first recover's throw
    }

    @Test
    void aThrowingRecoverWithNoNetFailsTheFuture() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        var pipeline = flow.just(5)
                .handle("boom", value -> {
                    throw new IllegalStateException("first");
                })
                .recover("throws", error -> {
                    throw new IllegalArgumentException("in-recover");
                })
                .handle("tail", value -> value);

        CompletionException failure = assertThrows(CompletionException.class, pipeline::execute);
        assertEquals("in-recover", failure.getCause().getMessage());
    }

    @Test
    void aThrowingRecoverInsideAFusedRunIsCaughtByTheNextRecover() {
        // No timeouts anywhere: boom, both recoveries and the tail fuse into one
        // worker run, so the throwing-recover scan happens INSIDE applyRun.
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        Integer result = flow.just(7)
                .handle("pre", value -> value + 1)
                .handle("boom", value -> {
                    throw new IllegalStateException("boom");
                })
                .recover("throws", error -> {
                    throw new IllegalArgumentException("in-recover");
                })
                .recover("net", error -> 42)
                .handle("tail", value -> value + 1)
                .execute();

        assertEquals(43, result);
    }

    @Test
    void aThrowingWhenPredicateRoutesToRecoverAndKeepsTheBossAlive() throws Exception {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);
        flow.when(value -> {
                    throw new IllegalStateException("predicate boom");
                })
                .then(lane -> lane.handle(value -> value * 2))
                .otherwise(lane -> lane.handle(value -> value - 1))
                .recover("net", error -> -1);

        // The failing predicate must reach recover, not hang the boss...
        assertEquals(-1, flow.just(5).executeAsync().orTimeout(5, TimeUnit.SECONDS).get());
        // ...and the boss must still be alive for the next execution.
        assertEquals(-1, flow.just(9).executeAsync().orTimeout(5, TimeUnit.SECONDS).get());
    }

    @Test
    void aThrowingMatchPredicateRoutesToRecoverAndKeepsTheBossAlive() throws Exception {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);
        flow.match()
                .is(value -> {
                    throw new IllegalStateException("case boom");
                }, lane -> lane.handle(value -> value * 2))
                .otherwise(lane -> lane.handle(value -> value + 1))
                .recover("net", error -> -1);

        assertEquals(-1, flow.just(5).executeAsync().orTimeout(5, TimeUnit.SECONDS).get());
        assertEquals(-1, flow.just(6).executeAsync().orTimeout(5, TimeUnit.SECONDS).get());
    }

    @Test
    void aThrowingFilterPredicateRoutesToRecover() throws Exception {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);
        flow.handle("pre", value -> value + 1)
                .filter(value -> {
                    throw new IllegalStateException("filter boom");
                })
                .recover("net", error -> -1)
                .handle("tail", value -> value * 10);

        assertEquals(-10, flow.just(5).executeAsync().orTimeout(5, TimeUnit.SECONDS).get());
        assertEquals(-10, flow.just(6).executeAsync().orTimeout(5, TimeUnit.SECONDS).get());
    }

    @Test
    void aThrowingUnrecoveredWhenPredicateFailsTheFutureWithoutHanging() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);
        flow.when(value -> {
                    throw new IllegalStateException("predicate boom");
                })
                .then(lane -> lane.handle(value -> value * 2))
                .otherwise(lane -> lane.handle(value -> value - 1));

        var future = flow.just(5).executeAsync().orTimeout(5, TimeUnit.SECONDS);
        CompletionException failure = assertThrows(CompletionException.class, future::join);
        assertEquals("predicate boom", rootCause(failure).getMessage());
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable root = failure;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root;
    }
}
