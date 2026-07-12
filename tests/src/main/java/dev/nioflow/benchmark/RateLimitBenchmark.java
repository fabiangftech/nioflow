package dev.nioflow.benchmark;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.RateLimit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.TimeUnit;

/**
 * Overhead del rate limiting cuando NO limita: un bucket enorme (100M/s)
 * nunca hace esperar, así que la brecha contra el stage plano es el costo
 * puro del acquire (un nanoTime + un updateAndGet por aplicación). Medir un
 * bucket que sí limita no tiene sentido en JMH: mediría el throttle, que es
 * el comportamiento pedido.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class RateLimitBenchmark {

    NioEngine plainEngine;
    NioEngine limitedEngine;
    NioFlow<Integer, Integer> plain;
    NioFlow<Integer, Integer> limited;

    @Setup
    public void setUp() {
        plainEngine = new DefaultNioEngine();
        plain = DefaultNioFlow.from(Integer.class, plainEngine);
        plain.handle("stage", value -> value * 2);
        plainEngine.seal();

        limitedEngine = new DefaultNioEngine();
        limited = DefaultNioFlow.from(Integer.class, limitedEngine);
        limited.handle("stage", value -> value * 2, RateLimit.perSecond(100_000_000));
        limitedEngine.seal();
    }

    @Benchmark
    public Object plainStage() {
        return plain.just(7).execute();
    }

    @Benchmark
    public Object rateLimitedStage() {
        return limited.just(7).execute();
    }
}
