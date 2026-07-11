package dev.nioflow.benchmark;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioFlow;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;

import java.util.concurrent.TimeUnit;

/**
 * Interpreted vs compiled dispatch over identical chains. seal() compiles the
 * plan; unsealed engines interpret every execution.
 *
 * - plain8:  8 unguarded stages — the compiled path does zero scanning and
 *            zero allocation per dispatch (one precollected run).
 * - forked:  match with 3 cases — guarded windows keep per-execution guard
 *            selection, bounded to the precompiled window; expect parity.
 * - contended8: the plain chain under 8 threads — less boss work per request
 *            should help the shared boss pool scale.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class CompiledChainBenchmark {

    NioFlow<Integer, Integer> plainInterpreted;
    NioFlow<Integer, Integer> plainCompiled;
    NioFlow<Integer, Integer> forkedInterpreted;
    NioFlow<Integer, Integer> forkedCompiled;

    @Setup
    public void setUp() {
        plainInterpreted = plain(false);
        plainCompiled = plain(true);
        forkedInterpreted = forked(false);
        forkedCompiled = forked(true);
    }

    private static NioFlow<Integer, Integer> plain(boolean sealed) {
        var engine = new DefaultNioEngine();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        for (int i = 0; i < 8; i++) {
            flow.handle("stage-" + i, value -> value + 1);
        }
        if (sealed) {
            engine.seal();
        }
        return flow;
    }

    private static NioFlow<Integer, Integer> forked(boolean sealed) {
        var engine = new DefaultNioEngine();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.match()
                .is(value -> value % 3 == 0, lane -> lane.handle(value -> value * 2))
                .is(value -> value % 3 == 1, lane -> lane.handle(value -> value * 2))
                .otherwise(lane -> lane.handle(value -> value * 2))
                .handle("main", value -> value + 1);
        if (sealed) {
            engine.seal();
        }
        return flow;
    }

    @Benchmark
    public Object plain8Interpreted() {
        return plainInterpreted.just(1).execute();
    }

    @Benchmark
    public Object plain8Compiled() {
        return plainCompiled.just(1).execute();
    }

    @Benchmark
    public Object forkedInterpreted() {
        return forkedInterpreted.just(7).execute();
    }

    @Benchmark
    public Object forkedCompiled() {
        return forkedCompiled.just(7).execute();
    }

    @Benchmark
    @Threads(8)
    public Object contended8Compiled() {
        return plainCompiled.just(1).execute();
    }

    @Benchmark
    @Threads(8)
    public Object contended8Interpreted() {
        return plainInterpreted.just(1).execute();
    }
}
