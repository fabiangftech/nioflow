package dev.nioflow.benchmark;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.Pipeline;
import dev.nioflow.infrastructure.reactive.Reactive;
import dev.nioflow.infrastructure.reactive.ReactiveFlow;
import dev.nioflow.infrastructure.reactive.ReactiveStep;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import reactor.core.publisher.Flux;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * RFC 0014 — pipe over a prebuilt Pipeline. Same ingestion loop, two ways:
 *
 * - dynamicPipe: the BiFunction form re-assembles the pipeline on EVERY element
 *   (a fresh ExecutionNioFlow, every step re-wrapped, the RFC 0011 chain copy,
 *   interpreted dispatch).
 * - prebuiltPipe: the Pipeline is recorded/validated/compiled ONCE; each element
 *   only executes it off the plan.
 *
 * Both push a fixed Flux of ELEMENTS through `stages` resolved stages. Prebuilt
 * should win on allocation per element (that is the whole point); throughput
 * follows. Run with -prof gc.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class PipeBenchmark {

    private static final int ELEMENTS = 200;
    private static final int CONCURRENCY = 8;

    @Param({"1", "8"})
    public int stages;

    NioEngine engine;
    Function<Flux<Integer>, Flux<Integer>> dynamicPipe;
    Function<Flux<Integer>, Flux<Integer>> prebuiltPipe;

    @Setup
    public void setUp() {
        engine = new DefaultNioEngine();
        ReactiveFlow<Integer, Integer> flow = Reactive.flow(DefaultNioFlow.from(Integer.class, engine));

        int steps = stages;
        Pipeline<Integer, Integer> pipeline = flow.pipeline(step -> chain(step, steps));
        prebuiltPipe = flow.pipe(CONCURRENCY, pipeline);
        dynamicPipe = flow.pipe(CONCURRENCY, (input, step) -> chain(step, steps));
    }

    private static Lane<Integer> chain(Lane<Integer> lane, int steps) {
        Lane<Integer> built = lane;
        for (int i = 0; i < steps; i++) {
            built = built.handle("s" + i, value -> value + 1);
        }
        return built;
    }

    private static ReactiveStep<Integer, Integer> chain(ReactiveStep<Integer, Integer> step, int steps) {
        ReactiveStep<Integer, Integer> built = step;
        for (int i = 0; i < steps; i++) {
            built = built.handle("s" + i, value -> value + 1);
        }
        return built;
    }

    @Benchmark
    public Object dynamicPipe() {
        return dynamicPipe.apply(Flux.range(1, ELEMENTS)).blockLast();
    }

    @Benchmark
    public Object prebuiltPipe() {
        return prebuiltPipe.apply(Flux.range(1, ELEMENTS)).blockLast();
    }
}
