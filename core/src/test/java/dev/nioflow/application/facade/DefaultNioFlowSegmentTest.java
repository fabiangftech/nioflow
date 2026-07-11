package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.Segment;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DefaultNioFlowSegmentTest {

    /** A reusable, independently defined piece of pipeline. */
    private static Segment<Integer, Integer> plusThenDouble() {
        return lane -> lane
                .handle("plus", value -> value + 1)
                .handle("double", value -> value * 2);
    }

    @Test
    void theSameSegmentIsReusableAcrossFlows() {
        NioFlow<Integer, Integer> first = DefaultNioFlow.from(Integer.class);
        NioFlow<Integer, Integer> second = DefaultNioFlow.from(Integer.class);
        first.use(plusThenDouble());
        second.handle("shift", value -> value + 100).use(plusThenDouble());

        assertEquals(12, first.just(5).execute());   // (5+1)*2
        assertEquals(212, second.just(5).execute()); // ((5+100)+1)*2
    }

    @Test
    void segmentsRetypeThePipeline() {
        Segment<Integer, String> describe = lane -> lane
                .handle("plus", value -> value + 1)
                .adapt(value -> "value:" + value);
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        String result = flow.just(5)
                .use(describe)
                .handle(value -> value.toUpperCase())
                .execute();

        assertEquals("VALUE:6", result);
    }

    @Test
    void segmentsCompose() {
        Segment<Integer, Integer> outer = lane -> lane
                .handle("before", value -> value + 10)
                .use(plusThenDouble())
                .handle("after", value -> value - 1);
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        assertEquals(31, flow.just(5).use(outer).execute()); // ((5+10+1)*2)-1
    }

    @Test
    void segmentInsideALaneInheritsItsGuards() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);
        flow.when(value -> value % 2 == 0)
                .then(lane -> lane.use(plusThenDouble()))
                .otherwise(lane -> lane.handle(value -> -value));

        assertEquals(10, flow.just(4).execute()); // even: the segment runs
        assertEquals(-3, flow.just(3).execute()); // odd: the segment's links are skipped
    }

    @Test
    void segmentsCanCarryForksFiltersAndRecovery() {
        Segment<Integer, Integer> guarded = lane -> lane
                .filter(value -> value != 0)
                .handle("risky", value -> {
                    if (value < 0) {
                        throw new IllegalStateException("negative");
                    }
                    return value * 10;
                })
                .recover("segment-fallback", error -> -1);
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);
        flow.use(guarded);

        assertEquals(50, flow.just(5).execute());
        assertEquals(-1, flow.just(-5).execute()); // recovered inside the segment
        assertNull(flow.just(0).execute());        // filtered inside the segment
    }

    @Test
    void segmentsWorkOnSealedCompiledChains() {
        var engine = new DefaultNioEngine();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.use(plusThenDouble()).handle("tail", value -> value + 1);
        engine.seal();

        assertEquals(13, flow.just(5).execute()); // segments leave plain links: fully compiled
    }
}
