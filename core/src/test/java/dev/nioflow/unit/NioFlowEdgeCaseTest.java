package dev.nioflow.unit;

import dev.nioflow.application.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class NioFlowEdgeCaseTest {

    @Test
    void aPipelineWithNoStagesReturnsTheInputFromJoin() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            assertEquals(42, pipeline.just(42).join());
        }
    }

    @Test
    void nullInputsFlowThroughAsyncStages() {
        try (NioFlow<String> pipeline = new NioFlow<>()) {
            String result = pipeline.just(null)
                    .submit(x -> x == null ? "was-null" : x)
                    .join();

            assertEquals("was-null", result);
        }
    }

    @Test
    void aStageReturningNullPropagatesTheNull() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            int result = pipeline.just(1)
                    .handle(x -> null)
                    .submit(x -> x == null ? -1 : x)
                    .join();

            assertEquals(-1, result);
        }
    }

    @Test
    void errorsLikeAssertionErrorAreCapturedToo() throws InterruptedException {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            AssertionError boom = new AssertionError("fatal");
            AtomicReference<Throwable> captured = new AtomicReference<>();
            CountDownLatch notified = new CountDownLatch(1);
            pipeline.onError(error -> {
                captured.set(error);
                notified.countDown();
            });
            pipeline.submit(x -> {
                throw boom;
            });
            pipeline.just(1);

            assertTrue(notified.await(1, TimeUnit.SECONDS));
            assertSame(boom, captured.get());
        }
    }

    @Test
    void aPipelineKeepsWorkingAfterJoin() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            List<Integer> results = new CopyOnWriteArrayList<>();
            assertEquals(2, pipeline.just(1).handle(x -> x + 1).join());

            pipeline.handle(x -> x * 10);
            pipeline.handle(x -> {
                results.add(x);
                return x;
            });
            pipeline.just(5);
            pipeline.join();

            // the value parked by the first join resumes through the stages added later
            assertEquals(2, results.size());
            assertTrue(results.containsAll(List.of(20, 60)));
        }
    }
}
