package dev.nioflow.benchmark;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlow;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.TimeUnit;

/**
 * El caso de uso del event loop dedicado: un flujo crítico compartiendo
 * bosses con un vecino ruidoso vs teniendo los suyos.
 *
 * Grupos asimétricos (7 threads de ruido + 1 medido): en sharedVictim la
 * víctima y el ruido usan el MISMO pool compartido de bosses; en
 * dedicatedVictim el ruido sigue en el pool compartido pero la víctima tiene
 * su engine dedicado (2 bosses propios). Comparar victim() entre grupos: el
 * dedicado no hace cola detrás de la orquestación del vecino.
 *
 * singleShared/singleDedicated: paridad sin contención (mismo código de
 * dispatch, solo cambia el pool).
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class DedicatedPoolBenchmark {

    NioEngine noiseEngine;
    NioEngine sharedVictimEngine;
    NioEngine dedicatedVictimEngine;
    NioFlow<Integer, Integer> noise;
    NioFlow<Integer, Integer> sharedVictim;
    NioFlow<Integer, Integer> dedicatedVictim;

    @Setup
    public void setUp() {
        noiseEngine = new DefaultNioEngine();
        noise = pipeline(noiseEngine);
        sharedVictimEngine = new DefaultNioEngine();
        sharedVictim = pipeline(sharedVictimEngine);
        dedicatedVictimEngine = DefaultNioEngine.dedicated(2);
        dedicatedVictim = pipeline(dedicatedVictimEngine);
    }

    private static NioFlow<Integer, Integer> pipeline(NioEngine engine) {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("a", value -> value + 1)
                .handle("b", value -> value * 2);
        engine.seal();
        return flow;
    }

    @Benchmark
    @Group("sharedVictim")
    @GroupThreads(7)
    public Object noiseOnSharedPool() {
        return noise.just(7).execute();
    }

    @Benchmark
    @Group("sharedVictim")
    @GroupThreads(1)
    public Object victimOnSharedPool() {
        return sharedVictim.just(7).execute();
    }

    @Benchmark
    @Group("dedicatedVictim")
    @GroupThreads(7)
    public Object noiseBesideDedicated() {
        return noise.just(7).execute();
    }

    @Benchmark
    @Group("dedicatedVictim")
    @GroupThreads(1)
    public Object victimOnDedicatedPool() {
        return dedicatedVictim.just(7).execute();
    }

    @Benchmark
    public Object singleShared() {
        return sharedVictim.just(7).execute();
    }

    @Benchmark
    public Object singleDedicated() {
        return dedicatedVictim.just(7).execute();
    }
}
