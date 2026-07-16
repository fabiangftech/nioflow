package dev.nioflow.infrastructure.reactive;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * RFC 0030 — the reactive mirror's BEHAVIOUR must be identical in every position
 * a reactive step can be declared, not just present (that is
 * {@link ReactiveMirrorTest}).
 *
 * <p>A reactive step lives in four places, each a separate hand-written copy:
 * the main line ({@code DefaultReactiveFlow}), a {@code just()} pipeline
 * ({@code DefaultReactiveStep}), a {@code when()} lane and a {@code fork()} body
 * (both {@code DefaultReactiveLane}). This runs the SAME probe through all four
 * and asserts the load-bearing properties agree — so a fix that lands in one copy
 * but not another (the "fixed in two of three places" bug this RFC exists to
 * prevent) fails the build instead of shipping. The oracle is agreement; no
 * hand-computed expected value.
 */
class ReactiveParityTest {

    private static final Duration BLOCK = Duration.ofSeconds(5);

    // ── the properties, each asserted across all four positions × both paths ──

    @Test
    void aDefaultBudgetBoundsAHungMonoInEveryPosition() {
        // RFC 0028's leak, guarded everywhere at once: a hung Mono under a declared
        // defaultBudget times out — on the block path and the preferAsync path, on
        // the main line, in a pipeline, in a lane and in a fork.
        for (boolean preferAsync : new boolean[]{false, true}) {
            Function<Integer, Mono<Integer>> hung = value -> Mono.never();
            Duration budget = Duration.ofMillis(50);
            assertTimeout(onMainLine(preferAsync, budget, hung), preferAsync, "main line");
            assertTimeout(inJustPipeline(preferAsync, budget, hung), preferAsync, "just() pipeline");
            assertTimeout(inLane(preferAsync, budget, hung), preferAsync, "when() lane");
            assertTimeout(inFork(preferAsync, budget, hung), preferAsync, "fork body");
        }
    }

    @Test
    void anEmptyMonoFailsInEveryPosition() {
        // RFC 0027, guarded everywhere: an empty value-carrying step is an
        // EmptyMonoException, never a silent null — identically in all four
        // positions and on both paths.
        for (boolean preferAsync : new boolean[]{false, true}) {
            Function<Integer, Mono<Integer>> empty = value -> Mono.empty();
            assertEmptyFailure(onMainLine(preferAsync, null, empty), preferAsync, "main line");
            assertEmptyFailure(inJustPipeline(preferAsync, null, empty), preferAsync, "just() pipeline");
            assertEmptyFailure(inLane(preferAsync, null, empty), preferAsync, "when() lane");
            assertEmptyFailure(inFork(preferAsync, null, empty), preferAsync, "fork body");
        }
    }

    private static void assertTimeout(Throwable seen, boolean preferAsync, String where) {
        assertInstanceOf(TimeoutException.class, rootOf(seen),
                () -> where + " (preferAsync=" + preferAsync + ") did not time out: " + seen);
    }

    private static void assertEmptyFailure(Throwable seen, boolean preferAsync, String where) {
        assertInstanceOf(EmptyMonoException.class, rootOf(seen),
                () -> where + " (preferAsync=" + preferAsync + ") did not fail on empty: " + seen);
    }

    // ── the four positions: each declares handleMono("probe", call) with a
    //    capturing recover, and returns what recover() saw ──

    private Throwable onMainLine(boolean preferAsync, Duration budget, Function<Integer, Mono<Integer>> call) {
        DefaultNioEngine engine = new DefaultNioEngine();
        try {
            ReactiveFlow<Integer, Integer> flow = configure(engine, preferAsync, budget);
            AtomicReference<Throwable> seen = new AtomicReference<>();
            flow.handleMono("probe", call).recover(capture(seen));
            flow.just(1).executeMono().block(BLOCK);
            return seen.get();
        } finally {
            engine.shutdown(Duration.ofSeconds(1));
        }
    }

    private Throwable inJustPipeline(boolean preferAsync, Duration budget, Function<Integer, Mono<Integer>> call) {
        DefaultNioEngine engine = new DefaultNioEngine();
        try {
            AtomicReference<Throwable> seen = new AtomicReference<>();
            configure(engine, preferAsync, budget).just(1)
                    .handleMono("probe", call)
                    .recover(capture(seen))
                    .executeMono()
                    .block(BLOCK);
            return seen.get();
        } finally {
            engine.shutdown(Duration.ofSeconds(1));
        }
    }

    private Throwable inLane(boolean preferAsync, Duration budget, Function<Integer, Mono<Integer>> call) {
        DefaultNioEngine engine = new DefaultNioEngine();
        try {
            AtomicReference<Throwable> seen = new AtomicReference<>();
            configure(engine, preferAsync, budget).just(1)
                    .when(value -> true)
                    .then(lane -> Reactive.lane(lane).handleMono("probe", call).recover(capture(seen)))
                    .executeMono()
                    .block(BLOCK);
            return seen.get();
        } finally {
            engine.shutdown(Duration.ofSeconds(1));
        }
    }

    private Throwable inFork(boolean preferAsync, Duration budget, Function<Integer, Mono<Integer>> call) {
        DefaultNioEngine engine = new DefaultNioEngine();
        try {
            AtomicReference<Throwable> seen = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);
            configure(engine, preferAsync, budget).just(1)
                    .fork("probe-fork", lane -> Reactive.lane(lane)
                            .handleMono("probe", call)
                            .recover(error -> {
                                seen.set(error);
                                done.countDown();
                                return -1;
                            }))
                    .executeMono()
                    .block(BLOCK);
            awaitLatch(done);
            return seen.get();
        } finally {
            engine.shutdown(Duration.ofSeconds(2));
        }
    }

    private static ReactiveFlow<Integer, Integer> configure(DefaultNioEngine engine, boolean preferAsync,
                                                            Duration budget) {
        ReactiveFlow<Integer, Integer> flow = Reactive.<Integer, Integer>flow(DefaultNioFlow.from(Integer.class, engine)).allowUnbudgeted();
        if (preferAsync) {
            flow = flow.preferAsync();
        }
        if (budget != null) {
            flow = flow.defaultBudget(budget);
        }
        return flow;
    }

    private static Function<Throwable, Integer> capture(AtomicReference<Throwable> seen) {
        return error -> {
            seen.set(error);
            return -1;
        };
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("the fork's recover never fired");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static Throwable rootOf(Throwable error) {
        Throwable cause = error;
        while (cause != null && cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }
}
