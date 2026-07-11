package dev.nioflow.benchmark;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.Segment;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.TimeUnit;

/**
 * Segments are build-time only: use(segment) appends the same plain links the
 * inline definition would. Both flows here have IDENTICAL chains — any gap
 * between the two benchmarks would mean segments leak runtime cost.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class SegmentBenchmark {

    NioFlow<Integer, Integer> inline;
    NioFlow<Integer, Integer> segmented;

    @Setup
    public void setUp() {
        NioEngine inlineEngine = new DefaultNioEngine();
        inline = DefaultNioFlow.from(Integer.class, inlineEngine);
        inline.handle("plus", value -> value + 1)
                .handle("double", value -> value * 2)
                .handle("minus", value -> value - 3)
                .handle("tail", value -> value * 5);
        inlineEngine.seal();

        Segment<Integer, Integer> middle = lane -> lane
                .handle("double", value -> value * 2)
                .handle("minus", value -> value - 3);
        NioEngine segmentedEngine = new DefaultNioEngine();
        segmented = DefaultNioFlow.from(Integer.class, segmentedEngine);
        segmented.handle("plus", value -> value + 1)
                .use(middle)
                .handle("tail", value -> value * 5);
        segmentedEngine.seal();
    }

    @Benchmark
    public Object inlineDefinition() {
        return inline.just(1).execute();
    }

    @Benchmark
    public Object segmentedDefinition() {
        return segmented.just(1).execute();
    }
}
