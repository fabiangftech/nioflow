package dev.nioflow.infrastructure.reactive;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.Context.Key;
import dev.nioflow.core.facade.FlowResult;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.Segment;
import dev.nioflow.core.model.RateLimit;
import dev.nioflow.core.model.Retry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The reactive facade is a MIRROR: every step delegates to the flow underneath
 * and appends the very same link. {@link ReactiveMirrorTest} proves the mirror
 * is complete (every core method has a covariant override); this one proves it
 * actually WORKS — a delegate wired to the wrong method, or one that forgets to
 * re-wrap its result, is a silent bug the type system cannot catch.
 *
 * <p>So each test walks a real pipeline through the mirrored steps and checks
 * two things at once: the value the engine produced, and that the builder never
 * fell back to the base type on the way (which is what would silently cost you
 * the reactive steps further down the chain).
 */
class ReactiveDelegationTest {

    private static final Key<String> TRACE = Key.of("trace");

    // Typed explicitly on purpose: Java cannot infer List<Function<Integer, R>>
    // from a List.of() of lambdas, so the join would receive Objects.
    private static final List<Function<Integer, Integer>> PAIR =
            List.of(value -> value, value -> 1);
    private static final List<Function<Integer, Mono<Integer>>> MONO_PAIR =
            List.of(Mono::just, value -> Mono.just(1));

    private NioEngine engine;
    private ReactiveFlow<Integer, Integer> flow;

    @BeforeEach
    void setUp() {
        engine = new DefaultNioEngine();
        flow = Reactive.<Integer, Integer>flow(DefaultNioFlow.from(Integer.class, engine)).allowUnbudgeted();
    }

    @AfterEach
    void tearDown() {
        engine.shutdown(Duration.ofSeconds(2));
    }

    @Test
    void wrappingIsIdempotent() {
        // Wrapping something already reactive gives it back, rather than a
        // wrapper of a wrapper — otherwise every re-wrap would add a hop.
        assertSame(flow, Reactive.flow(flow));

        flow.just(1).fork("f", sub -> {
            ReactiveLane<Integer> lane = Reactive.lane(sub);
            assertSame(lane, Reactive.lane(lane));
            return lane.handle("x", value -> value);
        }).execute();
    }

    @Test
    void adaptMonoWithABudgetRetypesAndBoundsTheCall() {
        assertEquals("n=2", flow.just(1)
                .adaptMono(value -> Mono.just("n=" + (value + 1)), Duration.ofSeconds(2))
                .execute());
    }

    /** Every step of the SHARED DEFINITION: the value proves each link landed. */
    @Test
    void everyFlowStepDelegatesAndStaysReactive() {
        ConcurrentLinkedQueue<Integer> effects = new ConcurrentLinkedQueue<>();
        Segment<Integer, Integer> segment = lane -> lane.handle("segment", value -> value + 1);
        Segment<Integer, Integer> region = lane -> lane.handle("region-segment", value -> value + 1);

        ReactiveFlow<Integer, Integer> built = flow
                .handle(value -> value + 1)                                    // 1  -> 2
                .handle("named", value -> value + 1)                           // -> 3
                .handle("timed", value -> value + 1, Duration.ofSeconds(2))    // -> 4
                .handle("retried", value -> value + 1, Retry.of(2, Duration.ofMillis(5)))     // -> 5
                .handle("both", value -> value + 1, Duration.ofSeconds(2), Retry.of(2, Duration.ofMillis(5)))  // -> 6
                .handle("limited", value -> value + 1, RateLimit.perSecond(1_000))            // -> 7
                .handleSync(value -> value + 1)                                // -> 8
                .handleSync("sync-named", value -> value + 1)                  // -> 9
                .handleContextual((value, ctx) -> value + 1)                   // -> 10
                .handleContextual("ctx-named", (value, ctx) -> value + 1)      // -> 11
                .handleMono("mono", value -> Mono.just(value + 1))             // -> 12
                .handleMono("mono-budget", value -> Mono.just(value + 1), Duration.ofSeconds(2))    // -> 13
                .handleMono("mono-retry", value -> Mono.just(value + 1), Retry.of(2, Duration.ofMillis(5)))  // -> 14
                .handleMono("mono-both", value -> Mono.just(value + 1),
                        Duration.ofSeconds(2), Retry.of(2, Duration.ofMillis(5)))                   // -> 15
                .background(effects::add)
                .background("bg-named", effects::add)
                .filter(value -> value > 0)
                .fanOut(PAIR, results -> results.get(0) + results.get(1))  // -> 16
                .fanOut("fanout-named", PAIR, results -> results.get(0) + results.get(1))                                       // -> 17
                .fanOutMono("fanout-mono", MONO_PAIR, results -> results.get(0) + results.get(1))                                       // -> 18
                .batch(1, Duration.ofMillis(50), values -> values.stream().map(value -> value + 1).toList())      // -> 19
                .batch("batch-named", 1, Duration.ofMillis(50),
                        values -> values.stream().map(value -> value + 1).toList())                               // -> 20
                .use(segment)                                                  // -> 21
                .use("region", region)                                         // -> 22
                .fork(sub -> sub.handle("anon-fork", value -> value))
                .fork("named-fork", sub -> sub.handle("in-fork", value -> value))
                .recover(error -> -1)
                .recover("recovery-named", error -> -1)
                .onComplete(effects::add)
                .onError(error -> effects.add(-99));

        // The builder never fell back to NioFlow on the way.
        assertInstanceOf(ReactiveFlow.class, built);
        engine.seal();

        assertEquals(22, flow.just(1).execute());
        assertTrue(effects.contains(15), () -> "the background effects ran: " + effects);
    }

    /** Every step of the PER-REQUEST pipeline, including the ones that re-type. */
    @Test
    @SuppressWarnings("deprecation")   // exercises the uncapped adaptFlux on purpose
    void everyStepDelegatesAndStaysReactive() {
        ConcurrentLinkedQueue<Object> seen = new ConcurrentLinkedQueue<>();
        Segment<Integer, Integer> segment = lane -> lane.handle("segment", value -> value + 1);

        String result = flow.just(1)
                .with(TRACE, "abc")
                .key("k")
                .handle(value -> value + 1)                                   // 1 -> 2
                .handle("named", value -> value + 1)                          // -> 3
                .handle("timed", value -> value + 1, Duration.ofSeconds(2))   // -> 4
                .handle("retried", value -> value + 1, Retry.of(2, Duration.ofMillis(5)))    // -> 5
                .handle("both", value -> value + 1, Duration.ofSeconds(2), Retry.of(2, Duration.ofMillis(5)))  // -> 6
                .handle("limited", value -> value + 1, RateLimit.perSecond(1_000))           // -> 7
                .handleSync(value -> value + 1)                               // -> 8
                .handleSync("sync-named", value -> value + 1)                 // -> 9
                .handleContextual((value, ctx) -> value + 1)                  // -> 10
                .handleContextual("ctx-named", (value, ctx) -> value + ctx.get(TRACE).length() - 2)  // -> 11
                .handleMono("mono", value -> Mono.just(value + 1))            // -> 12
                .handleMono("mono-budget", value -> Mono.just(value + 1), Duration.ofSeconds(2))     // -> 13
                .handleMono("mono-retry", value -> Mono.just(value + 1), Retry.of(2, Duration.ofMillis(5)))  // -> 14
                .handleMono("mono-both", value -> Mono.just(value + 1),
                        Duration.ofSeconds(2), Retry.of(2, Duration.ofMillis(5)))                    // -> 15
                .background(seen::add)
                .background("bg-named", seen::add)
                .filter(value -> value > 0)
                .use(segment)                                                 // -> 16
                .fork(sub -> sub.handle("anon-fork", value -> value))
                .fork("named-fork", sub -> sub.handle("in-fork", value -> value))
                .fanOut(PAIR, results -> results.get(0) + results.get(1))   // -> 17
                .fanOut("fanout-named", PAIR, results -> results.get(0) + results.get(1))                                        // -> 18
                .fanOutMono("fanout-mono", MONO_PAIR, results -> results.get(0) + results.get(1))                                        // -> 19
                .batch(1, Duration.ofMillis(50), values -> values.stream().map(value -> value + 1).toList())       // -> 20
                .batch("batch-named", 1, Duration.ofMillis(50),
                        values -> values.stream().map(value -> value + 1).toList())                                // -> 21
                .recover(error -> -1)
                .recover("recovery-named", error -> -1)
                .adaptMono(value -> Mono.just(value + 1))                     // -> 22
                .adaptFlux(value -> reactor.core.publisher.Flux.range(1, value))   // -> List of 22
                .adapt(List::size)                                            // -> 22
                .adapt(value -> "n=" + value)
                // Declared LAST on purpose: a callback is typed where it is
                // declared, so registering it before an adapt would hand it the
                // old type at runtime.
                .onComplete(seen::add)
                .onError(error -> seen.add("error"))
                .execute();

        assertEquals("n=22", result);
    }

    @Test
    void theThreeTerminalsDelegate() {
        assertEquals(2, flow.just(1).handle("double", value -> value * 2).execute());
        assertEquals(2, flow.just(1).handle("double", value -> value * 2).executeAsync().join());
        assertInstanceOf(FlowResult.Completed.class,
                flow.just(1).handle("double", value -> value * 2).executeResult());
        assertInstanceOf(FlowResult.Filtered.class,
                flow.just(1).filter(value -> value > 100).executeResult());
        assertNull(flow.just(1).filter(value -> value > 100).execute());
    }

    /** when()/match() on the FLOW: ReactiveCondition / ReactiveBranch / ReactiveCases. */
    @Test
    void branchingOnTheFlowKeepsTheChainReactive() {
        ReactiveFlow<Integer, Integer> afterWhen = flow
                .when(value -> value > 10)
                    .then(lane -> Reactive.lane(lane).handleMono("big", value -> Mono.just(value * 2)))
                    .otherwise(lane -> lane.handle("small", value -> value - 1))
                .handleMono("after-when", value -> Mono.just(value + 100));

        assertInstanceOf(ReactiveFlow.class, afterWhen);

        afterWhen.match()
                .is(value -> value > 100, lane -> Reactive.lane(lane)
                        .handleMono("case-1", value -> Mono.just(value * 10)))
                .is(value -> value < 0, lane -> lane.handle("case-2", value -> value))
                .otherwise(lane -> lane.handle("default", value -> value))
                .handleMono("after-match", value -> Mono.just(value + 1));
        engine.seal();

        assertEquals(1841, flow.just(42).execute());   // 42 >10 -> 84 -> +100 = 184 -> >100 -> *10 -> +1
        assertEquals(1021, flow.just(3).execute());    // 3 -> 2 -> +100 = 102 -> also >100 -> *10 -> +1
    }

    /** when()/match() on a STEP: ReactiveStepCondition / Branch / Cases. */
    @Test
    void branchingOnAStepKeepsThePipelineReactive() {
        Function<Integer, Integer> route = input -> flow.just(input)
                .when(value -> value > 10)
                    .then(lane -> Reactive.lane(lane).handleMono("big", value -> Mono.just(value * 2)))
                    .otherwise(lane -> lane.handle("small", value -> value - 1))
                .handleMono("after", value -> Mono.just(value + 1))
                .match()
                    .is(value -> value % 2 == 0, lane -> Reactive.lane(lane)
                            .handleMono("even", value -> Mono.just(value + 1000)))
                    .otherwise(lane -> lane.handle("odd", value -> value - 1000))
                .handleMono("tail", value -> Mono.just(value + 1))
                .execute();

        assertEquals(-914, route.apply(42));   // 84 -> 85, odd -> -915 -> +1
        assertEquals(-996, route.apply(3));    // 2  -> 3,  odd -> -997 -> +1
    }

    /** when()/match() INSIDE a lane: ReactiveLaneCondition / Branch / Cases. */
    @Test
    @SuppressWarnings("deprecation")   // exercises the uncapped adaptFlux on purpose
    void branchingInsideALaneKeepsTheLaneReactive() {
        Function<Integer, Integer> route = input -> flow.just(input)
                .when(value -> value > 0)
                    .then(outer -> Reactive.lane(outer)
                            .handleMono("enter", value -> Mono.just(value + 1))
                            .when(value -> value > 10)
                                .then(inner -> Reactive.lane(inner)
                                        .handleMono("inner-big", value -> Mono.just(value * 2)))
                                .otherwise(inner -> inner.handle("inner-small", value -> value * 3))
                            .match()
                                .is(value -> value > 50, inner -> Reactive.lane(inner)
                                        .handleMono("huge", value -> Mono.just(value + 1)))
                                .otherwise(inner -> inner.handle("rest", value -> value - 1))
                            .handleMono("leave", value -> Mono.just(value + 100)))
                .handle("main", value -> value)
                .execute();

        assertEquals(187, route.apply(42));   // 43 -> *2 = 86 -> >50 so +1 = 87 -> +100
        assertEquals(111, route.apply(3));    // 4 -> *3 = 12 -> not >50 so -1 = 11 -> +100
    }

    /** Lane steps reached through a fork sub-flow (the ReactiveLane mirror). */
    @Test
    @SuppressWarnings("deprecation")   // exercises the uncapped adaptFlux on purpose
    void everyLaneStepDelegates() throws InterruptedException {
        CountDownLatch done = new CountDownLatch(1);
        ConcurrentLinkedQueue<Object> seen = new ConcurrentLinkedQueue<>();
        Segment<Integer, Integer> segment = lane -> lane.handle("segment", value -> value + 1);

        flow.just(1)
                .fork("everything", sub -> Reactive.lane(sub)
                        .handle(value -> value + 1)                                    // 1 -> 2
                        .handle("named", value -> value + 1)                           // -> 3
                        .handle("timed", value -> value + 1, Duration.ofSeconds(2))    // -> 4
                        .handle("retried", value -> value + 1, Retry.of(2, Duration.ofMillis(5)))   // -> 5
                        .handle("both", value -> value + 1, Duration.ofSeconds(2),
                                Retry.of(2, Duration.ofMillis(5)))                     // -> 6
                        .handle("limited", value -> value + 1, RateLimit.perSecond(1_000))          // -> 7
                        .handleSync(value -> value + 1)                                // -> 8
                        .handleSync("sync-named", value -> value + 1)                  // -> 9
                        .handleContextual((value, ctx) -> value + 1)                   // -> 10
                        .handleContextual("ctx-named", (value, ctx) -> value + 1)      // -> 11
                        .handleMono("mono", value -> Mono.just(value + 1))             // -> 12
                        .handleMono("mono-budget", value -> Mono.just(value + 1), Duration.ofSeconds(2))     // -> 13
                        .handleMono("mono-retry", value -> Mono.just(value + 1),
                                Retry.of(2, Duration.ofMillis(5)))                     // -> 14
                        .handleMono("mono-both", value -> Mono.just(value + 1), Duration.ofSeconds(2),
                                Retry.of(2, Duration.ofMillis(5)))                     // -> 15
                        .background(seen::add)
                        .background("bg-named", seen::add)
                        .filter(value -> value > 0)
                        .recover(error -> -1)
                        .recover("recovery-named", error -> -1)
                        .fork(inner -> inner.handle("nested-anon", value -> value))
                        .fork("nested", inner -> inner.handle("nested-named", value -> value))
                        .use(segment)                                                  // -> 16
                        .fanOut(PAIR, results -> results.get(0) + results.get(1))       // -> 17
                        .fanOut("fanout-named", PAIR, results -> results.get(0) + results.get(1))    // -> 18
                        .fanOutMono("fanout-mono", MONO_PAIR, results -> results.get(0) + results.get(1))  // -> 19
                        .batch(1, Duration.ofMillis(50),
                                values -> values.stream().map(value -> value + 1).toList())          // -> 20
                        .batch("batch-named", 1, Duration.ofMillis(50),
                                values -> values.stream().map(value -> value + 1).toList())          // -> 21
                        .adaptMono(value -> Mono.just(value + 1))                      // -> 22
                        .adaptFlux(value -> reactor.core.publisher.Flux.range(1, value))
                        .adapt(List::size)                                             // -> 22
                        .handle("done", value -> {
                            seen.add(value);
                            done.countDown();
                            return value;
                        }))
                .execute();

        assertTrue(done.await(5, TimeUnit.SECONDS), "the fork must run: " + seen);
        assertTrue(seen.contains(22), () -> "the lane walked every step: " + seen);
    }

    /**
     * A named region only lives on the shared definition — it exists to be
     * spliced at runtime, and a fork's sub-chain (or a per-request pipeline) is
     * not the engine's chain. It used to fail with "the segment appended no
     * links", which was a lie: they were appended, just somewhere else.
     */
    @Test
    void aNamedRegionCannotBeDeclaredInsideAFork() {
        Segment<Integer, Integer> segment = lane -> lane.handle("inside", value -> value);

        ReactiveStep<Integer, Integer> step = flow.just(1);
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> step.fork("f", sub -> Reactive.lane(sub).use("nope", segment)));

        assertTrue(failure.getMessage().contains("only live on the shared definition"), failure.getMessage());
    }

    @Test
    void justAllAndCloseDelegate() throws Exception {
        NioEngine own = new DefaultNioEngine();
        DefaultNioFlow<Integer, Integer> root = DefaultNioFlow.from(Integer.class, own);
        ReactiveFlow<Integer, Integer> reactive = Reactive.flow(root);
        reactive.handle("double", value -> value * 2);

        reactive.justAll(List.of(1, 2, 3));
        assertEquals(2 + 4 + 6, (Integer) own.await() + (Integer) own.await() + (Integer) own.await());

        // close() on the mirror shuts the engine the underlying flow owns.
        ((AutoCloseable) reactive).close();
    }
}
