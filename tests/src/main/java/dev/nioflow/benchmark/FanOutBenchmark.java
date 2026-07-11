package dev.nioflow.benchmark;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlow;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * When fanOut pays off and what it costs:
 *
 * - sequentialWork / fanOutWork:  three independent computations of ~50µs
 *   each, chained vs fanned out — the parallel version should approach 3x.
 * - sequentialTrivial / fanOutTrivial: the same shape with trivial branches —
 *   measures the fan-out overhead (N worker dispatches + allOf + join hop)
 *   when there is no work to parallelize. Use fanOut for real work, not for
 *   cheap transformations.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class FanOutBenchmark {

    private static final long WORK_TOKENS = 50_000; // ~tens of microseconds

    NioFlow<Integer, Integer> sequentialWork;
    NioFlow<Integer, Integer> fanOutWork;
    NioFlow<Integer, Integer> sequentialTrivial;
    NioFlow<Integer, Integer> fanOutTrivial;

    @Setup
    public void setUp() {
        sequentialWork = sequential(true);
        fanOutWork = fanned(true);
        sequentialTrivial = sequential(false);
        fanOutTrivial = fanned(false);
    }

    private static int compute(int value, boolean heavy) {
        if (heavy) {
            Blackhole.consumeCPU(WORK_TOKENS);
        }
        return value + 1;
    }

    private static NioFlow<Integer, Integer> sequential(boolean heavy) {
        var engine = new DefaultNioEngine();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("a", value -> compute(value, heavy))
                .handle("b", value -> compute(value, heavy))
                .handle("c", value -> compute(value, heavy));
        engine.seal();
        return flow;
    }

    private static NioFlow<Integer, Integer> fanned(boolean heavy) {
        List<Function<Integer, Integer>> branches = List.of(
                value -> compute(value, heavy),
                value -> compute(value, heavy),
                value -> compute(value, heavy));
        NioEngine engine = new DefaultNioEngine();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.fanOut("split", branches,
                results -> results.get(0) + results.get(1) + results.get(2));
        engine.seal();
        return flow;
    }

    @Benchmark
    public Object sequentialWork() {
        return sequentialWork.just(1).execute();
    }

    @Benchmark
    public Object fanOutWork() {
        return fanOutWork.just(1).execute();
    }

    @Benchmark
    public Object sequentialTrivial() {
        return sequentialTrivial.just(1).execute();
    }

    @Benchmark
    public Object fanOutTrivial() {
        return fanOutTrivial.just(1).execute();
    }
}
