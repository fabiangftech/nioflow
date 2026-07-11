package dev.nioflow.benchmark;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.core.model.OverflowPolicy;
import dev.nioflow.core.model.Stage;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Admission cost of bounded in-flight capacity on the uncontended path:
 *
 * - injectAwaitUnbounded: default engine — no admission at all (baseline).
 * - injectAwaitBounded:   capacity 1024 (never full here) with BLOCK — pays
 *                         one Semaphore acquire per inject and one release
 *                         per await. Must stay at parity with unbounded.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class BackpressureBenchmark {

    DefaultNioEngine unbounded;
    DefaultNioEngine bounded;

    @Setup
    public void setUp() {
        unbounded = new DefaultNioEngine();
        bounded = new DefaultNioEngine(1024, OverflowPolicy.BLOCK);
        for (DefaultNioEngine engine : new DefaultNioEngine[]{unbounded, bounded}) {
            engine.append(new Stage("plus", value -> (int) value + 1, false, null, null, List.of()));
            engine.append(new Stage("double", value -> (int) value * 2, false, null, null, List.of()));
            engine.seal();
        }
    }

    @Benchmark
    public Object injectAwaitUnbounded() {
        unbounded.inject(1);
        return unbounded.await();
    }

    @Benchmark
    public Object injectAwaitBounded() {
        bounded.inject(1);
        return bounded.await();
    }
}
