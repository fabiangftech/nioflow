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
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            assertEquals(42, nioFlow.just(42).join());
        }
    }

    @Test
    void nullInputsFlowThroughAsyncStages() {
        try (NioFlow<String> nioFlow = new NioFlow<>()) {
            String result = nioFlow.just(null)
                    .submit(x -> x == null ? "was-null" : x)
                    .join();

            assertEquals("was-null", result);
        }
    }

    @Test
    void aStageReturningNullPropagatesTheNull() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            int result = nioFlow.just(1)
                    .handle(x -> null)
                    .submit(x -> x == null ? -1 : x)
                    .join();

            assertEquals(-1, result);
        }
    }

    @Test
    void errorsLikeAssertionErrorAreCapturedToo() throws InterruptedException {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            AssertionError boom = new AssertionError("fatal");
            AtomicReference<Throwable> captured = new AtomicReference<>();
            CountDownLatch notified = new CountDownLatch(1);
            nioFlow.onError(error -> {
                captured.set(error);
                notified.countDown();
            });
            nioFlow.submit(x -> {
                throw boom;
            });
            nioFlow.just(1);

            assertTrue(notified.await(1, TimeUnit.SECONDS));
            assertSame(boom, captured.get());
        }
    }

    @Test
    void aPipelineKeepsWorkingAfterJoin() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            List<Integer> results = new CopyOnWriteArrayList<>();
            assertEquals(2, nioFlow.just(1).handle(x -> x + 1).join());

            nioFlow.handle(x -> x * 10);
            nioFlow.handle(x -> {
                results.add(x);
                return x;
            });
            nioFlow.just(5);
            nioFlow.join();

            // the value parked by the first join resumes through the stages added later
            assertEquals(2, results.size());
            assertTrue(results.containsAll(List.of(20, 60)));
        }
    }
}
