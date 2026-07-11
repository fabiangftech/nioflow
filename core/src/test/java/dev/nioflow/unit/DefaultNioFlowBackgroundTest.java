package dev.nioflow.unit;

import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.FlowContext;
import dev.nioflow.core.model.StageException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class DefaultNioFlowBackgroundTest {

    @Test
    void joinNeverWaitsForABackgroundEffect() throws Exception {
        try (NioFlow<String> flow = DefaultNioFlow.autoScoped()) {
            CountDownLatch ran = new CountDownLatch(1);
            AtomicReference<String> seen = new AtomicReference<>();

            long start = System.nanoTime();
            String greeting = flow.just("Hello")
                    .handle(s -> s + ", World!")
                    .background(s -> {
                        sleep(1500);
                        seen.set(s);
                        ran.countDown();
                    })
                    .join();
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

            assertEquals("Hello, World!", greeting);
            assertTrue(elapsedMillis < 1000, "join waited for the background effect: " + elapsedMillis + "ms");
            assertTrue(ran.await(5, TimeUnit.SECONDS), "the effect must still run");
            assertEquals("Hello, World!", seen.get(), "the effect sees the payload as of its link");
        }
    }

    @Test
    void callNeverWaitsForABackgroundEffect() throws Exception {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            CountDownLatch ran = new CountDownLatch(1);
            defaultNioFlow.handle(x -> x * 2)
                    .background("audit", x -> {
                        sleep(1500);
                        ran.countDown();
                    });

            long start = System.nanoTime();
            int result = defaultNioFlow.<Integer>call(21).get(1, TimeUnit.SECONDS);
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

            assertEquals(42, result);
            assertTrue(elapsedMillis < 1000, "call waited for the background effect: " + elapsedMillis + "ms");
            assertTrue(ran.await(5, TimeUnit.SECONDS), "the effect must still run");
        }
    }

    @Test
    void aThrowingEffectIsReportedButNeverFailsTheValue() throws Exception {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            CountDownLatch reported = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            defaultNioFlow.handle(x -> x + 1)
                    .background("boom", x -> {
                        throw new IllegalStateException("effect failed");
                    })
                    .onError(error -> {
                        failure.set(error);
                        reported.countDown();
                    });

            assertEquals(2, defaultNioFlow.just(1).join(), "the value must complete normally");
            assertTrue(reported.await(5, TimeUnit.SECONDS), "the failure must reach onError");
            StageException described = assertInstanceOf(StageException.class, failure.get());
            assertInstanceOf(IllegalStateException.class, described.getCause());

            assertEquals(3, defaultNioFlow.just(2).join(), "a later join must not rethrow it");
        }
    }

    @Test
    void backgroundSeesACopyOfTheFlowContext() throws Exception {
        try (DefaultNioFlow<String> defaultNioFlow = new DefaultNioFlow<>()) {
            CountDownLatch ran = new CountDownLatch(1);
            AtomicReference<Object> traceId = new AtomicReference<>();
            defaultNioFlow.background(s -> {
                traceId.set(FlowContext.get("traceId"));
                ran.countDown();
            });

            defaultNioFlow.just("v", Map.of("traceId", "abc-123")).join();

            assertTrue(ran.await(5, TimeUnit.SECONDS));
            assertEquals("abc-123", traceId.get());
        }
    }

    @Test
    void backgroundInsideALaneRunsOnlyForThatLane() throws Exception {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> notified = new CopyOnWriteArrayList<>();
            CountDownLatch ran = new CountDownLatch(1);
            defaultNioFlow.when(x -> x > 10)
                    .then(lane -> lane.background(x -> {
                        notified.add(x);
                        ran.countDown();
                    }))
                    .handle(x -> x);

            defaultNioFlow.justAll(List.of(20, 5)).join();

            assertTrue(ran.await(5, TimeUnit.SECONDS));
            assertEquals(List.of(20), notified, "the other lane must not run the effect");
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
