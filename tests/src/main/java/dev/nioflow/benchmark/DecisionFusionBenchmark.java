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
 * Fusión a través de Decisions ya resueltos, sobre chain sellada: match de 3
 * casos seguido de 2 stages de línea principal.
 *
 * - firstCase: la ruta del caso 1 — su lane termina justo antes del Decision
 *   (guardado) del caso 2. Sin extensión de ventana son 2 dispatches (lane y
 *   cola); extendiendo la ventana a través de los Decisions/lanes saltados es
 *   UNO (2 hops menos).
 * - otherwiseCase: la ruta del otherwise ya fusiona lane+cola hoy (no quedan
 *   Decisions después) — chequeo de paridad.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class DecisionFusionBenchmark {

    NioEngine engine;
    NioFlow<Integer, Integer> flow;

    @Setup
    public void setUp() {
        engine = new DefaultNioEngine();
        flow = DefaultNioFlow.from(Integer.class, engine);
        flow.match()
                .is(value -> value % 3 == 0, lane -> lane.handle(value -> value * 2))
                .is(value -> value % 3 == 1, lane -> lane.handle(value -> value * 3))
                .otherwise(lane -> lane.handle(value -> value * 5))
                .handle("tail-1", value -> value + 1)
                .handle("tail-2", value -> value + 1);
        engine.seal();
    }

    @Benchmark
    public Object firstCase() {
        return flow.just(9).execute();
    }

    @Benchmark
    public Object otherwiseCase() {
        return flow.just(5).execute();
    }
}
