package dev.nioflow.benchmark;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.Context.Key;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.infrastructure.reactive.Reactive;
import dev.nioflow.infrastructure.reactive.ReactiveFlow;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * The trade-off from the RFC, measured: what an ALL-REACTIVE chain costs versus
 * a plain Reactor chain doing the same four calls.
 *
 * <p>Latency is not the trade-off — two thread hops per fused run against remote
 * calls measured in milliseconds is unmeasurable. What the RFC claims, and what
 * these benchmarks have to confirm or refute, is:
 *
 * <ul>
 * <li>{@code executeMono} costs a wrapper, not a thread hop ({@code monoOverhead});</li>
 * <li>{@code handleMono} costs the same as {@code handle} when the Mono is
 *     already resolved — the mirror must not tax a stage ({@code stageOverhead});</li>
 * <li>four sequential reactive stages FUSE, so the chain pays 2 hops, not 8
 *     ({@code fourReactiveStages} vs {@code fourReactiveStagesConcurrent});</li>
 * <li>and the honest one: allocation per request against pure Reactor
 *     ({@code pureReactorChain}) — run with {@code -prof gc}, which is where the
 *     "kilobytes vs bytes" claim lives or dies.</li>
 * </ul>
 *
 * <p>The context bridge is measured the same way. {@code contextPropagated} —
 * keys declared once with {@code propagate}, seeded from the subscriber context
 * on every subscription — against {@code contextSeededByHand}, the
 * deferContextual/with() dance it replaces; and {@code monoOverhead}, whose flow
 * declares no keys at all, is the acceptance bar: <b>propagate() unused must cost
 * exactly zero</b> (one branch on an empty list, no defer, no map).
 *
 * The Monos here resolve immediately: the point is to measure the ENGINE's
 * overhead, not a stubbed network.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class ReactiveBenchmark {

    private static final String TRACE_ID = "traceId";
    private static final Key<String> TRACE = Key.of(TRACE_ID);
    private static final Context TRACE_CONTEXT = Context.of(TRACE_ID, "abc-123");

    ReactiveFlow<Integer, Integer> plainStages;
    ReactiveFlow<Integer, Integer> reactiveStages;
    ReactiveFlow<Integer, Integer> reactiveConcurrent;
    ReactiveFlow<Integer, Integer> asyncStages;
    ReactiveFlow<Integer, Integer> contextual;
    ReactiveFlow<Integer, Integer> propagating;

    @Setup
    public void setUp() {
        NioEngine plainEngine = new DefaultNioEngine();
        plainStages = Reactive.flow(DefaultNioFlow.from(Integer.class, plainEngine));
        plainStages.handle("a", value -> value + 1)
                .handle("b", value -> value * 2)
                .handle("c", value -> value - 3)
                .handle("d", value -> value * 5);
        plainEngine.seal();

        // Four reactive stages in a row: they fuse into ONE worker run, so the
        // four awaits park the same virtual worker one after another.
        NioEngine reactiveEngine = new DefaultNioEngine();
        reactiveStages = Reactive.flow(DefaultNioFlow.from(Integer.class, reactiveEngine));
        reactiveStages.handleMono("a", value -> Mono.just(value + 1))
                .handleMono("b", value -> Mono.just(value * 2))
                .handleMono("c", value -> Mono.just(value - 3))
                .handleMono("d", value -> Mono.just(value * 5));
        reactiveEngine.seal();

        // The Mono.zip equivalent: three calls concurrently, on three workers.
        List<Function<Integer, Mono<Integer>>> branches = List.of(
                value -> Mono.just(value + 1),
                value -> Mono.just(value * 2),
                value -> Mono.just(value - 3));
        NioEngine concurrentEngine = new DefaultNioEngine();
        reactiveConcurrent = Reactive.flow(DefaultNioFlow.from(Integer.class, concurrentEngine));
        reactiveConcurrent.fanOutMono("enrich", branches,
                        results -> results.stream().mapToInt(Integer::intValue).sum())
                .handleMono("d", value -> Mono.just(value * 5));
        concurrentEngine.seal();

        // The cost side of RFC 0006: four async stages are four dispatches,
        // where the four handleMonos above fuse into ONE worker run. Same Monos,
        // resolved immediately: what is measured is the hops, nothing else.
        NioEngine asyncEngine = new DefaultNioEngine();
        asyncStages = Reactive.flow(DefaultNioFlow.from(Integer.class, asyncEngine));
        asyncStages.handleMonoAsync("a", value -> Mono.just(value + 1))
                .handleMonoAsync("b", value -> Mono.just(value * 2))
                .handleMonoAsync("c", value -> Mono.just(value - 3))
                .handleMonoAsync("d", value -> Mono.just(value * 5));
        asyncEngine.seal();

        // The context bridge, against the boilerplate it replaces. Same chain,
        // same stage reading the trace id: one flow declares propagate(TRACE),
        // the other makes every caller write the deferContextual/with() dance.
        NioEngine contextEngine = new DefaultNioEngine();
        contextual = Reactive.flow(DefaultNioFlow.from(Integer.class, contextEngine));
        contextual.handleContextual("read-trace", (value, ctx) -> value + ctx.get(TRACE).length());
        contextEngine.seal();

        NioEngine propagateEngine = new DefaultNioEngine();
        propagating = Reactive.<Integer, Integer>flow(DefaultNioFlow.from(Integer.class, propagateEngine))
                .propagate(TRACE);
        propagating.handleContextual("read-trace", (value, ctx) -> value + ctx.get(TRACE).length());
        propagateEngine.seal();
    }

    /** The baseline: the same chain, no Reactor anywhere. */
    @Benchmark
    public Object plainChain() {
        return plainStages.just(1).execute();
    }

    /** executeMono over a plain chain: the wrapper's cost, and nothing else. */
    @Benchmark
    public Object monoOverhead() {
        return plainStages.just(1).executeMono().block();
    }

    /** Four reactive stages: must fuse, so ~2 hops for the four of them. */
    @Benchmark
    public Object fourReactiveStages() {
        return reactiveStages.just(1).executeMono().block();
    }

    /**
     * Four async stages: 4 dispatches, 0 parked workers. Against
     * fourReactiveStages (2 hops, 4 parked-worker waits) this is the trade the
     * RFC's decision tree is made of — and the heap side of it is not here, it
     * is in ReactiveHeapProbeTest.
     */
    @Benchmark
    public Object fourAsyncReactiveStages() {
        return asyncStages.just(1).executeMono().block();
    }

    /** The Mono.zip shape: concurrent, but three parked workers per request. */
    @Benchmark
    public Object fourReactiveStagesConcurrent() {
        return reactiveConcurrent.just(1).executeMono().block();
    }

    /** The boilerplate propagate() replaces: seed by hand, at every call site. */
    @Benchmark
    public Object contextSeededByHand() {
        return Mono.deferContextual(view -> contextual.just(1)
                        .with(TRACE, view.get(TRACE_ID))
                        .executeMono())
                .contextWrite(TRACE_CONTEXT)
                .block();
    }

    /** The same seeding, declared once on the flow. This is the number to watch. */
    @Benchmark
    public Object contextPropagated() {
        return propagating.just(1)
                .executeMono()
                .contextWrite(TRACE_CONTEXT)
                .block();
    }

    /**
     * The honest comparison. Same four calls, no engine at all — this is what a
     * WebFlux user writes today, and the number to beat (or to lose to
     * gracefully) under -prof gc.
     */
    @Benchmark
    public Object pureReactorChain() {
        return Mono.just(1)
                .flatMap(value -> Mono.just(value + 1))
                .flatMap(value -> Mono.just(value * 2))
                .flatMap(value -> Mono.just(value - 3))
                .flatMap(value -> Mono.just(value * 5))
                .block();
    }
}
