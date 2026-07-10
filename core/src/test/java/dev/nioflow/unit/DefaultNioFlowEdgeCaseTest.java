package dev.nioflow.unit;

import dev.nioflow.application.facade.DefaultNioFlow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class DefaultNioFlowEdgeCaseTest {

    @Test
    void aPipelineWithNoStagesReturnsTheInputFromJoin() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            assertEquals(42, defaultNioFlow.just(42).join());
        }
    }

    @Test
    void nullInputsFlowThroughAsyncStages() {
        try (DefaultNioFlow<String> defaultNioFlow = new DefaultNioFlow<>()) {
            String result = defaultNioFlow.just(null)
                    .submit(x -> x == null ? "was-null" : x)
                    .join();

            assertEquals("was-null", result);
        }
    }

    @Test
    void aStageReturningNullPropagatesTheNull() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            int result = defaultNioFlow.just(1)
                    .handle(x -> null)
                    .submit(x -> x == null ? -1 : x)
                    .join();

            assertEquals(-1, result);
        }
    }

    @Test
    void errorsLikeAssertionErrorAreCapturedToo() throws InterruptedException {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            AssertionError boom = new AssertionError("fatal");
            AtomicReference<Throwable> captured = new AtomicReference<>();
            CountDownLatch notified = new CountDownLatch(1);
            defaultNioFlow.onError(error -> {
                captured.set(error);
                notified.countDown();
            });
            defaultNioFlow.submit(x -> {
                throw boom;
            });
            defaultNioFlow.just(1);

            assertTrue(notified.await(1, TimeUnit.SECONDS));
            assertSame(boom, captured.get());
        }
    }

    @Test
    void aPipelineKeepsWorkingAfterJoin() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> results = new CopyOnWriteArrayList<>();
            assertEquals(2, defaultNioFlow.just(1).handle(x -> x + 1).join());

            defaultNioFlow.handle(x -> x * 10);
            defaultNioFlow.handle(x -> {
                results.add(x);
                return x;
            });
            defaultNioFlow.just(5);
            defaultNioFlow.join();

            // the value parked by the first join resumes through the stages added later
            assertEquals(2, results.size());
            assertTrue(results.containsAll(List.of(20, 60)));
        }
    }
}
