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
 * Cost of recover() in the pipeline:
 *
 * - stagesOnly:        stage → stage (baseline, one fused run)
 * - recoverHappyPath:  stage → recover → stage with no failure. A Recovery
 *                      link ends the fusion run by design (a failure inside a
 *                      run recovers from the run's end, so recoveries cannot
 *                      be inside one) — this measures that extra hop pair.
 * - recoverTriggered:  failing stage → recover → stage. Full error path:
 *                      exception, forward scan, recovery dispatch, resume.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class RecoveryBenchmark {

    NioFlow<Integer, Integer> stagesOnly;
    NioFlow<Integer, Integer> recoverHappyPath;
    NioFlow<Integer, Integer> recoverTriggered;

    @Setup
    public void setUp() {
        stagesOnly = build(false, false);
        recoverHappyPath = build(true, false);
        recoverTriggered = build(true, true);
    }

    private static NioFlow<Integer, Integer> build(boolean withRecover, boolean failing) {
        NioEngine engine = new DefaultNioEngine();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("head", failing
                ? value -> {
                    throw new IllegalStateException("boom");
                }
                : value -> value + 1);
        if (withRecover) {
            flow.recover("fallback", error -> -1);
        }
        flow.handle("tail", value -> value * 2);
        engine.seal();
        return flow;
    }

    @Benchmark
    public Object stagesOnly() {
        return stagesOnly.just(1).execute();
    }

    @Benchmark
    public Object recoverHappyPath() {
        return recoverHappyPath.just(1).execute();
    }

    @Benchmark
    public Object recoverTriggered() {
        return recoverTriggered.just(1).execute();
    }
}
