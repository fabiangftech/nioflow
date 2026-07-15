package dev.nioflow.benchmark;

import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.Pipeline;
import dev.nioflow.core.facade.Segment;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.TimeUnit;

/**
 * RFC 0011 — the per-request plan. Same pipeline (a shared chain of `stages`
 * plus one local step), run three ways:
 *
 * - dynamicBuild: {@code just().handle().execute()} — the documented main path,
 *   which copies the shared chain and interprets. The baseline (== the
 *   {@code perRequestBuilder} in NioFlowBenchmark).
 * - dynamicCached: the same pipeline BUILT ONCE, executed many times — Part B's
 *   cached snapshot: one compile, then plan dispatch (a re-subscribing Mono).
 * - prebuilt: a {@link Pipeline} declared once — Part A: recorded, validated and
 *   compiled at startup, so a request allocates only its run.
 *
 * prebuilt and dynamicCached should both beat dynamicBuild, and the gap should
 * widen with `stages` (the copy grows with chain length).
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class PipelineBenchmark {

    private static final String EXTRA = "extra";

    @Param({"1", "8", "32"})
    public int stages;

    NioFlow<Integer, Integer> flow;
    Segment<Integer, Integer> segment;
    Pipeline<Integer, Integer> prebuilt;
    dev.nioflow.core.facade.NioStep<Integer, Integer> cached;

    @Setup
    public void setUp() {
        flow = DefaultNioFlow.from(Integer.class);
        for (int i = 0; i < stages; i++) {
            flow.handle("stage-" + i, value -> value + 1);
        }
        segment = step -> step.handle(EXTRA, value -> value + 1);
        prebuilt = flow.pipeline(segment);
        cached = flow.just(1).handle(EXTRA, value -> value + 1);
    }

    @Benchmark
    public Object dynamicBuild() {
        return flow.just(1).handle(EXTRA, value -> value + 1).execute();
    }

    @Benchmark
    public Object dynamicCached() {
        return cached.execute();
    }

    @Benchmark
    public Object prebuilt() {
        return prebuilt.just(1).execute();
    }
}
