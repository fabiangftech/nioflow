package dev.nioflow.unit;

import dev.nioflow.application.facade.NioFlow;
import dev.nioflow.core.model.Diagnostics;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class NioFlowDiagnosticsTest {

    @Test
    void theDumpDescribesTheChainShape() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            pipeline.handle("validate", x -> x)
                    .submit(x -> x, Duration.ofSeconds(2))
                    .when(x -> x > 10)
                    .then(lane -> lane
                            .filter(x -> true))
                    .otherwise(lane -> lane
                            .fanOut(x -> List.of(x)))
                    .onErrorResume(error -> -1)
                    .batch(3, Duration.ofSeconds(1), group -> group);

            List<String> chain = pipeline.diagnostics().chain();

            assertEquals("handle[validate]", chain.get(0));
            assertEquals("submit timeout=PT2S", chain.get(1));
            assertEquals("when#0", chain.get(2));
            assertEquals("filter if{0=true}", chain.get(3));
            assertEquals("fanOut if{0=false}", chain.get(4));
            assertEquals("onErrorResume", chain.get(5));
            assertEquals("batch[size=3, maxWait=PT1S]", chain.get(6));
        }
    }

    @Test
    void countsReflectTheRuntimeState() throws InterruptedException {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            pipeline.submit(x -> {
                entered.countDown();
                try {
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return x;
            });

            pipeline.just(1);
            assertTrue(entered.await(2, TimeUnit.SECONDS));

            Diagnostics whileRunning = pipeline.diagnostics();
            assertEquals(1, whileRunning.active());
            assertEquals(1, whileRunning.injected());
            assertEquals(0, whileRunning.parked());

            release.countDown();
            pipeline.join();

            Diagnostics afterJoin = pipeline.diagnostics();
            assertEquals(0, afterJoin.active());
            assertEquals(1, afterJoin.parked());
        }
    }

    @Test
    void batchedCountsValuesWaitingInBuffers() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            pipeline.batch(10, Duration.ofSeconds(5), group -> group);

            pipeline.just(1);
            pipeline.just(2);
            awaitBatched(pipeline, 2);

            assertEquals(2, pipeline.diagnostics().batched());
        }
    }

    @Test
    void sealedAndClosedShowInTheDump() {
        NioFlow<Integer> pipeline = new NioFlow<>();
        pipeline.handle(x -> x).seal();
        assertTrue(pipeline.diagnostics().sealed());
        assertFalse(pipeline.diagnostics().closed());

        pipeline.close();
        assertTrue(pipeline.diagnostics().closed());
    }

    @Test
    void toStringRendersTheFullDump() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            pipeline.handle("validate", x -> x);
            pipeline.just(1);
            pipeline.join();

            String dump = pipeline.toString();
            assertTrue(dump.contains("active=0"));
            assertTrue(dump.contains("parked=1"));
            assertTrue(dump.contains("injected=1"));
            assertTrue(dump.contains("1. handle[validate]"));
        }
    }

    private static void awaitBatched(NioFlow<Integer> pipeline, int expected) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (pipeline.diagnostics().batched() < expected && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
    }
}
