package dev.nioflow.benchmark;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.Retry;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cost of the retry policy:
 *
 * - noRetryDeclared:   stage → stage baseline (one fused run).
 * - retryNeverNeeded:  same chain with a retry policy declared but never
 *                      triggered — must be at parity (one null-free branch).
 * - retryOneFailure:   the stage fails on every first attempt and succeeds on
 *                      the second (zero backoff): measures the inline retry
 *                      loop on the error path, still inside the fused run.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class RetryBenchmark {

    NioFlow<Integer, Integer> noRetryDeclared;
    NioFlow<Integer, Integer> retryNeverNeeded;
    NioFlow<Integer, Integer> retryOneFailure;
    final AtomicLong flakyCalls = new AtomicLong();

    @Setup
    public void setUp() {
        noRetryDeclared = build(null, false);
        retryNeverNeeded = build(Retry.of(3, Duration.ofMillis(1)), false);
        retryOneFailure = build(Retry.of(2, Duration.ZERO), true);
    }

    private NioFlow<Integer, Integer> build(Retry retry, boolean flaky) {
        NioEngine engine = new DefaultNioEngine();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("head", value -> value + 1);
        java.util.function.Function<Integer, Integer> work = flaky
                ? value -> {
                    // Odd call: first attempt fails; even call: the retry succeeds.
                    if (flakyCalls.incrementAndGet() % 2 == 1) {
                        throw new IllegalStateException("transient");
                    }
                    return value * 2;
                }
                : value -> value * 2;
        if (retry != null) {
            flow.handle("work", work, retry);
        } else {
            flow.handle("work", work);
        }
        engine.seal();
        return flow;
    }

    @Benchmark
    public Object noRetryDeclared() {
        return noRetryDeclared.just(1).execute();
    }

    @Benchmark
    public Object retryNeverNeeded() {
        return retryNeverNeeded.just(1).execute();
    }

    @Benchmark
    public Object retryOneFailure() {
        return retryOneFailure.just(1).execute();
    }
}
