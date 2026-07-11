package dev.nioflow.benchmark;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.Context;
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
 * Costo del Context API. plainStages es el baseline (el instanceof del punto
 * de aplicación es el único costo cuando nadie usa contexto — debe quedar en
 * paridad con corridas históricas ~727 B/op); contextualStages paga el
 * wrapper, la vista y el HashMap lazy: put en un stage, get dos stages
 * después.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class ContextBenchmark {

    private static final Context.Key<Integer> SEED = Context.Key.of("seed");

    NioEngine plainEngine;
    NioEngine contextEngine;
    NioFlow<Integer, Integer> plain;
    NioFlow<Integer, Integer> contextual;

    @Setup
    public void setUp() {
        plainEngine = new DefaultNioEngine();
        plain = DefaultNioFlow.from(Integer.class, plainEngine);
        plain.handle("a", value -> value + 1)
                .handle("b", value -> value + 1)
                .handle("c", value -> value + 1);
        plainEngine.seal();

        contextEngine = new DefaultNioEngine();
        contextual = DefaultNioFlow.from(Integer.class, contextEngine);
        contextual.handleContextual("a", (value, ctx) -> {
            ctx.put(SEED, value);
            return value + 1;
        }).handle("b", value -> value + 1)
                .handleContextual("c", (value, ctx) -> value + 1 + ctx.get(SEED));
        contextEngine.seal();
    }

    @Benchmark
    public Object plainStages() {
        return plain.just(7).execute();
    }

    @Benchmark
    public Object contextualStages() {
        return contextual.just(7).execute();
    }
}
