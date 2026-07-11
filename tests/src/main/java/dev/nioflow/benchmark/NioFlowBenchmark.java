package dev.nioflow.benchmark;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.Stage;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Costos base del pipeline. Qué busca cada uno:
 *
 * - engineCall:        costo puro del event loop por request (2 saltos de hilo
 *                      por Stage: boss→worker→boss). Escalar `stages` mide el
 *                      costo por salto — si crece lineal fuerte, la mejora es
 *                      ejecutar stages baratos inline en el boss.
 * - fluentExecute:     lo mismo vía just().execute() — la diferencia contra
 *                      engineCall es el overhead del builder efímero.
 * - perRequestBuilder: just().handle().execute() — agrega la copia perezosa de
 *                      la chain compartida por request; crece con `stages`.
 * - engineCallContended: 8 hilos contra el MISMO boss compartido. Si el
 *                      throughput no escala respecto de engineCall, el boss
 *                      único es el cuello de botella (mejora: N bosses con
 *                      afinidad por engine, estilo EventLoopGroup de Netty).
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class NioFlowBenchmark {

    @Param({"1", "8", "32"})
    public int stages;

    NioEngine engine;
    NioFlow<Integer, Integer> flow;

    @Setup
    public void setUp() {
        engine = new DefaultNioEngine();
        for (int i = 0; i < stages; i++) {
            engine.append(new Stage("stage-" + i, value -> (int) value + 1, false, null, List.of()));
        }
        engine.seal();
        flow = DefaultNioFlow.from(Integer.class, engine);
    }

    @Benchmark
    public Object engineCall() {
        return engine.call(1, null).join();
    }

    @Benchmark
    public Object fluentExecute() {
        return flow.just(1).execute();
    }

    @Benchmark
    public Object perRequestBuilder() {
        return flow.just(1).handle(value -> value + 1).execute();
    }

    @Benchmark
    @Threads(8)
    public Object engineCallContended() {
        return engine.call(1, null).join();
    }
}
