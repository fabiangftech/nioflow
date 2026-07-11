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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Costo de onComplete/onError en la API fluida.
 *
 * - noCallbacks: ejecución sin callbacks — debe quedar en paridad con el
 *   baseline (sin callbacks no se asigna ningún future dependiente).
 * - executionCallback: onComplete por ejecución (un whenComplete extra).
 * - sharedCallback: onComplete en la definición compartida (handler del
 *   engine: costo por invocación, cero allocation extra por request).
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class CallbacksBenchmark {

    final AtomicInteger sink = new AtomicInteger();
    NioEngine plainEngine;
    NioEngine sharedEngine;
    NioFlow<Integer, Integer> plain;
    NioFlow<Integer, Integer> shared;

    @Setup
    public void setUp() {
        plainEngine = new DefaultNioEngine();
        plain = DefaultNioFlow.from(Integer.class, plainEngine);
        plain.handle("double", value -> value * 2);
        plainEngine.seal();

        sharedEngine = new DefaultNioEngine();
        shared = DefaultNioFlow.from(Integer.class, sharedEngine);
        shared.handle("double", value -> value * 2);
        shared.onComplete(sink::addAndGet);
        sharedEngine.seal();
    }

    @Benchmark
    public Object noCallbacks() {
        return plain.just(7).execute();
    }

    @Benchmark
    public Object executionCallback() {
        return plain.just(7).onComplete(sink::addAndGet).execute();
    }

    @Benchmark
    public Object sharedCallback() {
        return shared.just(7).execute();
    }
}
