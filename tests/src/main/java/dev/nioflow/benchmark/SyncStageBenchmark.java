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
 * Stages boss-inlined con handleSync vs el mismo trabajo con handle: la misma
 * cadena de 4 stages triviales (puro CPU, sub-µs) viaja como una run fusionada
 * al worker (2 hops) o corre entera en el boss (0 hops). La brecha medida es
 * el costo de los 2 thread hops que el marker sync elimina. La variante de 1
 * stage muestra el mismo delta sin fusión de por medio.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class SyncStageBenchmark {

    NioEngine workerEngine;
    NioEngine syncEngine;
    NioEngine singleWorkerEngine;
    NioEngine singleSyncEngine;
    NioFlow<Integer, Integer> worker4;
    NioFlow<Integer, Integer> sync4;
    NioFlow<Integer, Integer> worker1;
    NioFlow<Integer, Integer> sync1;

    @Setup
    public void setUp() {
        workerEngine = new DefaultNioEngine();
        worker4 = DefaultNioFlow.from(Integer.class, workerEngine);
        for (int i = 0; i < 4; i++) {
            worker4.handle("worker-" + i, value -> value + 1);
        }
        workerEngine.seal();

        syncEngine = new DefaultNioEngine();
        sync4 = DefaultNioFlow.from(Integer.class, syncEngine);
        for (int i = 0; i < 4; i++) {
            sync4.handleSync("sync-" + i, value -> value + 1);
        }
        syncEngine.seal();

        singleWorkerEngine = new DefaultNioEngine();
        worker1 = DefaultNioFlow.from(Integer.class, singleWorkerEngine);
        worker1.handle("only", value -> value * 2);
        singleWorkerEngine.seal();

        singleSyncEngine = new DefaultNioEngine();
        sync1 = DefaultNioFlow.from(Integer.class, singleSyncEngine);
        sync1.handleSync("only", value -> value * 2);
        singleSyncEngine.seal();
    }

    @Benchmark
    public Object workerChain4() {
        return worker4.just(1).execute();
    }

    @Benchmark
    public Object syncChain4() {
        return sync4.just(1).execute();
    }

    @Benchmark
    public Object workerSingle() {
        return worker1.just(7).execute();
    }

    @Benchmark
    public Object syncSingle() {
        return sync1.just(7).execute();
    }
}
