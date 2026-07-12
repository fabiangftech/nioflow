package dev.nioflow.benchmark;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.infrastructure.Resilience4jStages;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.TimeUnit;

/**
 * Costo de los decoradores Resilience4j cuando están sanos (breaker
 * cerrado, bulkhead libre): la brecha contra el stage plano es la
 * contabilidad de r4j por aplicación. El corto-circuito (abierto/lleno)
 * no se benchmarkea: ES el comportamiento pedido.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class Resilience4jBenchmark {

    NioEngine plainEngine;
    NioEngine guardedEngine;
    NioFlow<Integer, Integer> plain;
    NioFlow<Integer, Integer> guarded;

    @Setup
    public void setUp() {
        plainEngine = new DefaultNioEngine();
        plain = DefaultNioFlow.from(Integer.class, plainEngine);
        plain.handle("stage", value -> value * 2);
        plainEngine.seal();

        guardedEngine = new DefaultNioEngine();
        guarded = DefaultNioFlow.from(Integer.class, guardedEngine);
        guarded.handle("stage", Resilience4jStages.guarded(
                CircuitBreaker.ofDefaults("bench"),
                Bulkhead.ofDefaults("bench"),
                value -> value * 2));
        guardedEngine.seal();
    }

    @Benchmark
    public Object plainStage() {
        return plain.just(7).execute();
    }

    @Benchmark
    public Object guardedStage() {
        return guarded.just(7).execute();
    }
}
