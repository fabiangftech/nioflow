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
 * The cost of ROUTING (when/match) — not of fork(), which is a different feature
 * entirely. The same transformation as a
 * flat chain (1 stage) against a 3-case match() (3 Decisions evaluated on the
 * boss, plus the guards checked per link). The gap is what pays for
 * passesGuards() being a plain loop instead of a stream — the stream version
 * cost ~20% here.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class BranchRoutingBenchmark {

    NioEngine plainEngine;
    NioEngine branchedEngine;
    NioFlow<Integer, Integer> plain;
    NioFlow<Integer, Integer> branched;

    @Setup
    public void setUp() {
        plainEngine = new DefaultNioEngine();
        plain = DefaultNioFlow.from(Integer.class, plainEngine);
        plain.handle("only", value -> value * 2);
        plainEngine.seal();

        branchedEngine = new DefaultNioEngine();
        branched = DefaultNioFlow.from(Integer.class, branchedEngine);
        branched.match()
                .is(value -> value % 3 == 0, lane -> lane.handle(value -> value * 2))
                .is(value -> value % 3 == 1, lane -> lane.handle(value -> value * 2))
                .otherwise(lane -> lane.handle(value -> value * 2));
        branchedEngine.seal();
    }

    @Benchmark
    public Object plainChain() {
        return plain.just(7).execute();
    }

    @Benchmark
    public Object matchRouting() {
        return branched.just(7).execute();
    }
}
