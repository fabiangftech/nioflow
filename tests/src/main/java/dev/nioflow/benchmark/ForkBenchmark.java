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

import java.util.concurrent.TimeUnit;

/**
 * What a detached fork costs the MAIN LINE — the only cost that matters, since
 * nobody waits for the fork itself.
 *
 * <p>Four chains, same 4 stages on the main line:
 *
 * <ul>
 * <li>{@code plain} — no side work: the fusion baseline (one run, 2 hops).</li>
 * <li>{@code background} — a fire-and-forget lambda in the middle. It breaks
 *     the fused run exactly like a fork does, so this is the honest baseline:
 *     the fork must cost about the same for strictly more capability.</li>
 * <li>{@code fork} — a 3-stage detached sub-flow in the same position.</li>
 * <li>{@code forkHeavy} — the same fork with a much heavier body. The main
 *     line's throughput must NOT move: if it does, something is waiting.</li>
 * </ul>
 *
 * The forks do real (if small) work so the workers actually run them; the
 * scores measure the main line's request rate, not the fork's.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class ForkBenchmark {

    NioFlow<Integer, Integer> plain;
    NioFlow<Integer, Integer> background;
    NioFlow<Integer, Integer> fork;
    NioFlow<Integer, Integer> forkHeavy;

    @Setup
    public void setUp() {
        plain = sealed(flow -> flow
                .handle("a", value -> value + 1)
                .handle("b", value -> value * 2)
                .handle("c", value -> value - 3)
                .handle("d", value -> value * 5));

        background = sealed(flow -> flow
                .handle("a", value -> value + 1)
                .handle("b", value -> value * 2)
                .background("side", ForkBenchmark::sink)
                .handle("c", value -> value - 3)
                .handle("d", value -> value * 5));

        fork = sealed(flow -> flow
                .handle("a", value -> value + 1)
                .handle("b", value -> value * 2)
                .fork("side", sub -> sub
                        .handle("s1", value -> value + 1)
                        .handle("s2", value -> value * 2)
                        .handle("s3", ForkBenchmark::consume))
                .handle("c", value -> value - 3)
                .handle("d", value -> value * 5));

        forkHeavy = sealed(flow -> flow
                .handle("a", value -> value + 1)
                .handle("b", value -> value * 2)
                .fork("side", sub -> sub
                        .handle("s1", value -> value + 1)
                        .when(value -> value % 2 == 0)
                            .then(lane -> lane.handle("even", ForkBenchmark::spin))
                            .otherwise(lane -> lane.handle("odd", ForkBenchmark::spin))
                        .handle("s2", ForkBenchmark::spin)
                        .handle("s3", ForkBenchmark::consume))
                .handle("c", value -> value - 3)
                .handle("d", value -> value * 5));
    }

    @Benchmark
    public Object plainChain() {
        return plain.just(1).execute();
    }

    @Benchmark
    public Object withBackground() {
        return background.just(1).execute();
    }

    @Benchmark
    public Object withFork() {
        return fork.just(1).execute();
    }

    @Benchmark
    public Object withHeavyFork() {
        return forkHeavy.just(1).execute();
    }

    private static NioFlow<Integer, Integer> sealed(java.util.function.Consumer<NioFlow<Integer, Integer>> define) {
        NioEngine engine = new DefaultNioEngine();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        define.accept(flow);
        engine.seal();
        return flow;
    }

    // Blackhole-ish sinks: keep the fork's work from being optimized away
    // without adding cost the main line could be blamed for.
    private static int consumed;

    private static void sink(Integer value) {
        consumed += value;
    }

    private static Integer consume(Integer value) {
        consumed += value;
        return value;
    }

    private static Integer spin(Integer value) {
        int acc = value;
        for (int i = 0; i < 64; i++) {
            acc = acc * 31 + i;
        }
        return acc;
    }
}
