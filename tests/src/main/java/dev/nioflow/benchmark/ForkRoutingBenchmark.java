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
 * Overhead del enrutamiento con forks: la misma transformación como chain
 * plana (1 stage) vs match() de 3 casos (3 Decisions evaluados en el boss +
 * guards chequeados por link, con stream().allMatch en el hot path — si la
 * brecha es grande, la mejora es evaluar guards sin streams/alloc).
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class ForkRoutingBenchmark {

    NioEngine plainEngine;
    NioEngine forkedEngine;
    NioFlow<Integer, Integer> plain;
    NioFlow<Integer, Integer> forked;

    @Setup
    public void setUp() {
        plainEngine = new DefaultNioEngine();
        plain = DefaultNioFlow.from(Integer.class, plainEngine);
        plain.handle("only", value -> value * 2);
        plainEngine.seal();

        forkedEngine = new DefaultNioEngine();
        forked = DefaultNioFlow.from(Integer.class, forkedEngine);
        forked.match()
                .is(value -> value % 3 == 0, lane -> lane.handle(value -> value * 2))
                .is(value -> value % 3 == 1, lane -> lane.handle(value -> value * 2))
                .otherwise(lane -> lane.handle(value -> value * 2));
        forkedEngine.seal();
    }

    @Benchmark
    public Object plainChain() {
        return plain.just(7).execute();
    }

    @Benchmark
    public Object matchRouting() {
        return forked.just(7).execute();
    }
}
