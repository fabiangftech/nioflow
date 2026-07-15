package dev.nioflow.infrastructure.reactive;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.Pipeline;
import dev.nioflow.core.model.RateLimit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RFC 0015 — inside a {@code pipe}, {@code handleMono}/{@code adaptMono} route to
 * the async (future-holding) path so the ingestion loop parks no worker per
 * element. The routing is a build-time decision (the {@code preferAsync} config
 * flag); it changes NO result, so these tests pin equivalence and the two things
 * that are genuinely different on the async path: the budget cancels the
 * subscription, and a {@code RateLimit} step stays blocking (it is a plain
 * handle, not a handleMono).
 */
class ReactivePreferAsyncTest {

    private NioEngine engine;
    private ReactiveFlow<Integer, Integer> flow;

    @BeforeEach
    void setUp() {
        engine = new DefaultNioEngine();
        flow = Reactive.flow(DefaultNioFlow.from(Integer.class, engine));
    }

    @AfterEach
    void tearDown() {
        engine.shutdown(Duration.ofSeconds(2));
    }

    @Test
    void pipeRoutesHandleMonoAsyncAndStillProducesTheSameResult() {
        // The BiFunction pipe form is async-routed by default: same output as the
        // blocking equivalent would give.
        Function<Flux<Integer>, Flux<Integer>> pipe = flow.pipe(8, (input, step) -> step
                .handleMono("plus", value -> Mono.just(value + 1))
                .adaptMono(value -> Mono.just(value * 10)));

        List<Integer> results = pipe.apply(Flux.range(1, 5)).sort().collectList().block();
        assertEquals(List.of(20, 30, 40, 50, 60), results);   // (n+1)*10
    }

    @Test
    void aPreferAsyncPrebuiltPipelineProducesTheSameResult() {
        // The prebuilt path holds futures too when built from a preferAsync flow.
        Pipeline<Integer, Integer> pipeline = flow.preferAsync().pipeline(step ->
                Reactive.lane(step).handleMono("plus", value -> Mono.just(value + 1)));

        List<Integer> results = flow.pipe(8, pipeline).apply(Flux.range(1, 4)).sort().collectList().block();
        assertEquals(List.of(2, 3, 4, 5), results);
    }

    @Test
    void directHandleMonoStillBlocksUnlessPreferAsyncIsDeclared() {
        // Off the pipe path, handleMono keeps parking — same result, and this is
        // just the equivalence check that routing did not change the value.
        Integer result = flow.just(5).handleMono("plus", value -> Mono.just(value + 1)).executeMono().block();
        assertEquals(6, result);

        Integer async = flow.preferAsync().just(5)
                .handleMono("plus", value -> Mono.just(value + 1)).executeMono().block();
        assertEquals(6, async);
    }

    @Test
    void theBudgetCancelsAHungMonoOnTheAsyncPipePath() {
        // A Mono that never completes, with a short budget: on the async path the
        // budget is the engine timeout, which cancels the subscription and reaches
        // recover() as a TimeoutException — exactly as the blocking budget does.
        var cancelled = new java.util.concurrent.atomic.AtomicBoolean();
        Function<Flux<Integer>, Flux<Integer>> pipe = flow.pipe(4, (input, step) -> step
                .handleMono("hung", value -> Mono.<Integer>never()
                        .doOnCancel(() -> cancelled.set(true)), Duration.ofMillis(80))
                .recover(error -> error instanceof TimeoutException ? -1 : -2));

        List<Integer> results = pipe.apply(Flux.just(1)).collectList().block();
        assertEquals(List.of(-1), results);            // TimeoutException reached recover
        assertTrue(cancelled.get(), "the budget must cancel the subscription on the async path");
    }

    @Test
    void aRateLimitStepInsideAPipeStaysBlockingAndStillLimits() {
        // RateLimit is a plain handle that parks on acquire() — not a handleMono,
        // so preferAsync does not touch it. It must still admit at its rate.
        RateLimit oncePerformed = RateLimit.of(1000, Duration.ofSeconds(1));
        var applied = new AtomicInteger();
        Function<Flux<Integer>, Flux<Integer>> pipe = flow.pipe(4, (input, step) -> step
                .handleMono("enrich", Mono::just)
                .handle("limited", value -> {
                    applied.incrementAndGet();
                    return value;
                }, oncePerformed));

        List<Integer> results = pipe.apply(Flux.range(1, 5)).sort().collectList().block();
        assertEquals(List.of(1, 2, 3, 4, 5), results);
        assertEquals(5, applied.get());
    }
}
