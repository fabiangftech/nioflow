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
import org.openjdk.jmh.annotations.Threads;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Happy-path cost of arming a stage timeout:
 *
 * - plainStages:  stage → stage (baseline, one fused run)
 * - timeoutArmed: stage(1s budget, finishes instantly) → stage. A timeout
 *                 stage dispatches alone (it cannot join a fused run: the
 *                 budget covers exactly that stage) and schedules an
 *                 orTimeout timer that is cancelled on completion — this
 *                 measures both costs together.
 *
 * The triggered path is not benchmarked: it waits for the budget by design.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class TimeoutBenchmark {

    NioFlow<Integer, Integer> plainStages;
    NioFlow<Integer, Integer> timeoutArmed;

    @Setup
    public void setUp() {
        plainStages = build(null);
        timeoutArmed = build(Duration.ofSeconds(1));
    }

    private static NioFlow<Integer, Integer> build(Duration timeout) {
        NioEngine engine = new DefaultNioEngine();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        if (timeout != null) {
            flow.handle("head", value -> value + 1, timeout);
        } else {
            flow.handle("head", value -> value + 1);
        }
        flow.handle("tail", value -> value * 2);
        engine.seal();
        return flow;
    }

    @Benchmark
    public Object plainStages() {
        return plainStages.just(1).execute();
    }

    @Benchmark
    public Object timeoutArmed() {
        return timeoutArmed.just(1).execute();
    }

    // La contención es donde el scheduling del timer importa: 8 threads
    // armando y cancelando presupuestos golpean la estructura compartida
    // del timer (heap con lock global en orTimeout; staging queue lock-free
    // en la timer wheel).
    @Benchmark
    @Threads(8)
    public Object timeoutArmedContended8() {
        return timeoutArmed.just(1).execute();
    }
}
