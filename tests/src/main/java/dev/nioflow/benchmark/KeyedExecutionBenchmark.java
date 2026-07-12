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
 * Overhead del keyed execution en secuencial (sin cola real: cada llamada
 * encuentra su lane libre). unkeyed es el baseline; keyedSameKey reusa una
 * clave (lane creado y retirado por llamada); keyedDistinctKeys estrena
 * clave por llamada (peor churn del mapa). La serialización bajo
 * concurrencia no se benchmarkea: ES la semántica pedida.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class KeyedExecutionBenchmark {

    NioEngine engine;
    NioFlow<Integer, Integer> flow;
    int sequence;

    @Setup
    public void setUp() {
        engine = new DefaultNioEngine();
        flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("stage", value -> value * 2);
        engine.seal();
    }

    @Benchmark
    public Object unkeyed() {
        return flow.just(7).execute();
    }

    @Benchmark
    public Object keyedSameKey() {
        return flow.just(7).key("hot-key").execute();
    }

    @Benchmark
    public Object keyedDistinctKeys() {
        return flow.just(7).key(sequence++).execute();
    }
}
