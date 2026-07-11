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
 * Cost of filter in the pipeline:
 *
 * - stagesOnly:  stage → stage (baseline, one fused run)
 * - filterPass:  stage → filter(pass) → stage. If the filter breaks stage
 *                fusion, this pays extra thread hops vs stagesOnly; if the
 *                predicate is fused into the worker run, the gap disappears.
 * - filterCut:   stage → filter(reject) → stage. Early exit: the flow
 *                completes without running the tail stage.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class FilterBenchmark {

    NioFlow<Integer, Integer> stagesOnly;
    NioFlow<Integer, Integer> filterPass;
    NioFlow<Integer, Integer> filterCut;

    @Setup
    public void setUp() {
        stagesOnly = build(false, false);
        filterPass = build(true, false);
        filterCut = build(true, true);
    }

    private static NioFlow<Integer, Integer> build(boolean withFilter, boolean rejecting) {
        NioEngine engine = new DefaultNioEngine();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("head", value -> value + 1);
        if (withFilter) {
            flow.filter(value -> rejecting ? value < 0 : value > 0);
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
    public Object filterPass() {
        return filterPass.just(1).execute();
    }

    @Benchmark
    public Object filterCut() {
        return filterCut.just(1).execute();
    }
}
