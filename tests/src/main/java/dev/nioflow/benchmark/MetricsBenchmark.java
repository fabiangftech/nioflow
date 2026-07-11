package dev.nioflow.benchmark;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.NioFlowMetrics;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.TimeUnit;

/**
 * Instrumentation overhead of the metrics SPI:
 *
 * - metricsOff: default engine, no metrics installed — the hot path only pays
 *               null checks (baseline).
 * - metricsOn:  a no-op recorder installed — pays the real instrumentation
 *               (System.nanoTime per stage and per execution, queue-depth
 *               pushes) without exporter costs. The gap between the two IS
 *               the price of turning metrics on.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class MetricsBenchmark {

    NioFlow<Integer, Integer> metricsOff;
    NioFlow<Integer, Integer> metricsOn;

    @Setup
    public void setUp() {
        metricsOff = build(false);
        metricsOn = build(true);
    }

    private static NioFlow<Integer, Integer> build(boolean withMetrics) {
        var engine = new DefaultNioEngine();
        if (withMetrics) {
            engine.metrics(new NioFlowMetrics() {
            });
        }
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("one", value -> value + 1)
                .handle("two", value -> value * 2)
                .handle("three", value -> value - 3)
                .handle("four", value -> value * 5);
        engine.seal();
        return flow;
    }

    @Benchmark
    public Object metricsOff() {
        return metricsOff.just(1).execute();
    }

    @Benchmark
    public Object metricsOn() {
        return metricsOn.just(1).execute();
    }
}
