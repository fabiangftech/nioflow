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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Cost and benefit of executeAsync():
 *
 * - executeBlocking:     single execute() (baseline).
 * - executeAsyncJoin:    single executeAsync().join() — must be at parity with
 *                        executeBlocking (execute IS executeAsync().join()).
 * - sequentialBatch16:   16 orders, waiting at the door for each one before
 *                        placing the next (blocking execute in a loop).
 * - pipelinedBatch16:    16 orders launched with executeAsync (tickets in
 *                        hand), then joined — the executions overlap across
 *                        the boss pool and workers instead of queueing behind
 *                        the caller's patience.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class ExecuteAsyncBenchmark {

    private static final int BATCH = 16;

    NioFlow<Integer, Integer> flow;

    @Setup
    public void setUp() {
        NioEngine engine = new DefaultNioEngine();
        flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("plus", value -> value + 1);
        flow.handle("double", value -> value * 2);
        engine.seal();
    }

    @Benchmark
    public Object executeBlocking() {
        return flow.just(1).execute();
    }

    @Benchmark
    public Object executeAsyncJoin() {
        return flow.just(1).executeAsync().join();
    }

    @Benchmark
    public Object sequentialBatch16() {
        int sum = 0;
        for (int i = 0; i < BATCH; i++) {
            sum += flow.just(i).execute();
        }
        return sum;
    }

    @Benchmark
    public Object pipelinedBatch16() {
        @SuppressWarnings("unchecked")
        CompletableFuture<Integer>[] tickets = new CompletableFuture[BATCH];
        for (int i = 0; i < BATCH; i++) {
            tickets[i] = flow.just(i).executeAsync();
        }
        int sum = 0;
        for (CompletableFuture<Integer> ticket : tickets) {
            sum += ticket.join();
        }
        return sum;
    }
}
