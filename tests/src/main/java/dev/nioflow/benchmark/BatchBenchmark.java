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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Batching contra un downstream SERIALIZADO (una conexión: cada roundtrip
 * toma un lock y cuesta ~20µs) — el modelo honesto de "bulk insert": el
 * valor del batch no es paralelismo (parks independientes ya corren en
 * paralelo en los workers) sino UN roundtrip para N valores.
 *
 * - individual16: 16 ejecuciones pipelined, cada una su roundtrip → 16
 *   roundtrips serializados por el lock.
 * - batched16:    batch(16, 5ms) → UN roundtrip para las 16.
 * - plainSingle / batchSize1: overhead puro del punto de batch por valor
 *   (size 1 = flush inmediato), sin downstream.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class BatchBenchmark {

    private static final int PIPELINED = 16;
    private static final long ROUNDTRIP_NANOS = 20_000;

    final ReentrantLock connection = new ReentrantLock();
    NioEngine individualEngine;
    NioEngine batchedEngine;
    NioEngine plainEngine;
    NioEngine batchOneEngine;
    NioFlow<Integer, Integer> individual;
    NioFlow<Integer, Integer> batched;
    NioFlow<Integer, Integer> plain;
    NioFlow<Integer, Integer> batchOne;

    @Setup
    public void setUp() {
        individualEngine = new DefaultNioEngine();
        individual = DefaultNioFlow.from(Integer.class, individualEngine);
        individual.handle("roundtrip", value -> {
            connection.lock();
            try {
                LockSupport.parkNanos(ROUNDTRIP_NANOS);
            } finally {
                connection.unlock();
            }
            return value + 1;
        });
        individualEngine.seal();

        batchedEngine = new DefaultNioEngine();
        batched = DefaultNioFlow.from(Integer.class, batchedEngine);
        batched.batch("bulk-roundtrip", PIPELINED, Duration.ofMillis(5), values -> {
            connection.lock();
            try {
                LockSupport.parkNanos(ROUNDTRIP_NANOS);
            } finally {
                connection.unlock();
            }
            return values.stream().map(value -> value + 1).toList();
        });
        batchedEngine.seal();

        plainEngine = new DefaultNioEngine();
        plain = DefaultNioFlow.from(Integer.class, plainEngine);
        plain.handle("stage", value -> value + 1);
        plainEngine.seal();

        batchOneEngine = new DefaultNioEngine();
        batchOne = DefaultNioFlow.from(Integer.class, batchOneEngine);
        batchOne.batch("bulk-one", 1, Duration.ofMillis(5), values ->
                values.stream().map(value -> value + 1).toList());
        batchOneEngine.seal();
    }

    @Benchmark
    public Object individual16() {
        return roundOf16(individual);
    }

    @Benchmark
    public Object batched16() {
        return roundOf16(batched);
    }

    @Benchmark
    public Object plainSingle() {
        return plain.just(7).execute();
    }

    @Benchmark
    public Object batchSize1() {
        return batchOne.just(7).execute();
    }

    @SuppressWarnings("unchecked")
    private static Object roundOf16(NioFlow<Integer, Integer> flow) {
        CompletableFuture<Integer>[] calls = new CompletableFuture[PIPELINED];
        for (int i = 0; i < PIPELINED; i++) {
            calls[i] = flow.just(i).executeAsync();
        }
        Integer last = null;
        for (CompletableFuture<Integer> call : calls) {
            last = call.join();
        }
        return last;
    }
}
