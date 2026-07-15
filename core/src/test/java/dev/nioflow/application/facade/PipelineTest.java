package dev.nioflow.application.facade;

import dev.nioflow.core.facade.ChainValidationException;
import dev.nioflow.core.facade.Context;
import dev.nioflow.core.facade.FlowResult;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.Pipeline;
import dev.nioflow.core.facade.Segment;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A prebuilt {@link Pipeline} must produce the exact result the equivalent
 * {@code just()}-build produces — the plan is an optimization, never a
 * semantic. Parity is checked by running the SAME {@link Segment} both ways:
 * once through {@code pipeline(seg)} (recorded, validated, compiled once) and
 * once through {@code just(x).use(seg)} (built per request), and comparing.
 */
class PipelineTest {

    private static NioFlow<Integer, String> flow() {
        return DefaultNioFlow.from(Integer.class);
    }

    private static NioFlow<Integer, String> flow(int... sharedStages) {
        NioFlow<Integer, String> flow = DefaultNioFlow.from(Integer.class);
        for (int stage : sharedStages) {
            flow.handle("shared-" + stage, value -> value + stage);
        }
        return flow;
    }

    @Test
    void prebuiltMatchesJustBuildOnAPlainChain() {
        NioFlow<Integer, String> flow = flow();
        Segment<Integer, String> seg = step -> step
                .handle("charge", item -> item * 2)
                .adapt(item -> "EUR " + item);

        Pipeline<Integer, String> pipeline = flow.pipeline(seg);
        for (int input : new int[]{0, 7, 100}) {
            assertEquals(flow.just(input).use(seg).execute(), pipeline.just(input).execute());
            assertEquals("EUR " + (input * 2), pipeline.just(input).execute());
        }
    }

    @Test
    void prebuiltRunsTheSharedChainFirst() {
        // The pipeline snapshots shared + segment: the shared stage runs before
        // the segment, exactly as a just() pipeline would.
        NioFlow<Integer, String> flow = flow(10);
        Segment<Integer, String> seg = step -> step.adapt(item -> "v=" + item);

        Pipeline<Integer, String> pipeline = flow.pipeline(seg);
        assertEquals("v=15", pipeline.just(5).execute());
        assertEquals(flow.just(5).use(seg).execute(), pipeline.just(5).execute());
    }

    @Test
    void prebuiltMatchesJustBuildAcrossBranching() {
        NioFlow<Integer, String> flow = flow();
        Segment<Integer, String> seg = step -> step
                .match()
                .is(item -> item < 0, lane -> lane.handle(item -> item * -1))
                .is(item -> item == 0, lane -> lane.handle(item -> item + 1000))
                .otherwise(lane -> lane.handle(item -> item * 10))
                .adapt(item -> "n=" + item);

        Pipeline<Integer, String> pipeline = flow.pipeline(seg);
        for (int input : new int[]{-5, 0, 3}) {
            assertEquals(flow.just(input).use(seg).execute(), pipeline.just(input).execute());
        }
        assertEquals("n=5", pipeline.just(-5).execute());
        assertEquals("n=1000", pipeline.just(0).execute());
        assertEquals("n=30", pipeline.just(3).execute());
    }

    @Test
    void prebuiltMatchesJustBuildWithAFork() throws Exception {
        var forked = new ConcurrentLinkedQueue<Integer>();
        var forkRan = new java.util.concurrent.CountDownLatch(1);
        NioFlow<Integer, String> flow = flow();
        Segment<Integer, String> seg = step -> step
                .fork("audit", sub -> sub.background(value -> {
                    forked.add(value);
                    forkRan.countDown();
                }))
                .adapt(item -> "done:" + item);

        Pipeline<Integer, String> pipeline = flow.pipeline(seg);
        assertEquals("done:9", pipeline.just(9).execute());
        assertEquals(flow.just(9).use(seg).execute(), pipeline.just(9).execute());

        // The fork ran as a detached child (its value reached the background).
        assertTrue(forkRan.await(2, java.util.concurrent.TimeUnit.SECONDS));
        assertTrue(forked.contains(9));
    }

    @Test
    void prebuiltMatchesJustBuildWithABatch() throws Exception {
        NioFlow<Integer, String> flow = flow();
        Segment<Integer, String> seg = step -> step
                .batch("bulk", 2, Duration.ofSeconds(1),
                        values -> values.stream().map(value -> value + 1).toList())
                .adapt(item -> "b=" + item);

        Pipeline<Integer, String> pipeline = flow.pipeline(seg);
        // Two in flight so the batch flushes by size (2).
        CompletableFuture<String> a = pipeline.just(1).executeAsync();
        CompletableFuture<String> b = pipeline.just(2).executeAsync();
        assertEquals("b=2", a.get(2, java.util.concurrent.TimeUnit.SECONDS));
        assertEquals("b=3", b.get(2, java.util.concurrent.TimeUnit.SECONDS));
    }

    @Test
    void validatesAtBuildTime() {
        NioFlow<Integer, String> flow = flow();
        // Duplicate anchor name: a build-time structural problem the validator
        // rejects. Without a Pipeline this would only ever have been (not)
        // caught at seal(), which the just() path never reaches.
        Segment<Integer, String> broken = step -> step
                .handle("dup", item -> item)
                .handle("dup", item -> item)
                .adapt(String::valueOf);

        ChainValidationException failure =
                assertThrows(ChainValidationException.class, () -> flow.pipeline(broken));
        assertTrue(failure.getMessage().contains("dup"), failure.getMessage());
    }

    @Test
    void filterCutIsObservableThroughExecuteResult() {
        NioFlow<Integer, String> flow = flow();
        Pipeline<Integer, String> pipeline = flow.pipeline(step -> step
                .filter(item -> item > 0)
                .adapt(item -> "ok:" + item));

        assertEquals("ok:5", pipeline.just(5).execute());
        assertEquals(null, pipeline.just(-1).execute());
        assertInstanceOf(FlowResult.Filtered.class, pipeline.just(-1).executeResult());
        assertInstanceOf(FlowResult.Completed.class, pipeline.just(5).executeResult());
    }

    @Test
    void seedsPerRequestContext() {
        Context.Key<String> tenant = Context.Key.of("tenant");
        var seen = new java.util.concurrent.atomic.AtomicReference<String>();
        NioFlow<Integer, String> flow = flow();
        Pipeline<Integer, String> pipeline = flow.pipeline(step -> step
                .handleContextual((item, ctx) -> {
                    seen.set(ctx.get(tenant));
                    return item;
                })
                .adapt(item -> "ok"));

        // with() seeds the context the contextual stage then reads.
        assertEquals("ok", pipeline.just(1).with(tenant, "acme").execute());
        assertEquals("acme", seen.get());

        // The executeAsync(map) overload seeds a per-run context too.
        assertEquals("ok", pipeline.just(2).executeAsync(java.util.Map.of("tenant", "globex")).join());
        assertEquals("globex", seen.get());
    }

    @Test
    void keyedRequestsOnAPipelineRunInOrder() throws Exception {
        NioFlow<Integer, String> flow = flow();
        var order = new ConcurrentLinkedQueue<Integer>();
        Pipeline<Integer, String> pipeline = flow.pipeline(step -> step
                .handle("record", item -> {
                    order.add(item);
                    return item;
                })
                .adapt(String::valueOf));

        // Same key: strict submission order. Fire a handful and join them all.
        List<CompletableFuture<String>> futures = new java.util.ArrayList<>();
        for (int i = 0; i < 20; i++) {
            futures.add(pipeline.just(i).key("k").executeAsync());
        }
        for (CompletableFuture<String> future : futures) {
            future.get(2, java.util.concurrent.TimeUnit.SECONDS);
        }
        assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19),
                new java.util.ArrayList<>(order));
    }

    @Test
    void rejectsAnInputThatIsNotTheDeclaredType() {
        // The Class token from from() reaches the pipeline, so just() rejects a
        // value that is not an I — the net for unchecked casts (a raw type, a
        // framework injecting by generics), exactly as DefaultNioFlow.just does.
        NioFlow<Integer, String> typed = DefaultNioFlow.from(Integer.class);
        Pipeline<Integer, String> pipeline = typed.pipeline(step -> step.adapt(String::valueOf));

        @SuppressWarnings({"unchecked", "rawtypes"})
        Pipeline rawPipeline = pipeline;
        assertThrows(IllegalArgumentException.class, () -> rawPipeline.just("not an int"));
    }
}
