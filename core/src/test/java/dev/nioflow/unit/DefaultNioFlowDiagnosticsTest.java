package dev.nioflow.unit;

import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.model.Diagnostics;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DefaultNioFlowDiagnosticsTest {

    @Test
    void theDumpDescribesTheChainShape() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle("validate", x -> x)
                    .submit(x -> x, Duration.ofSeconds(2))
                    .when(x -> x > 10)
                    .then(lane -> lane
                            .filter(x -> true))
                    .otherwise(lane -> lane
                            .fanOut(x -> List.of(x)))
                    .onErrorResume(error -> -1)
                    .batch(3, Duration.ofSeconds(1), group -> group);

            List<String> chain = defaultNioFlow.diagnostics().chain();

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
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            defaultNioFlow.submit(x -> {
                entered.countDown();
                try {
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return x;
            });

            defaultNioFlow.just(1);
            assertTrue(entered.await(2, TimeUnit.SECONDS));

            Diagnostics whileRunning = defaultNioFlow.diagnostics();
            assertEquals(1, whileRunning.active());
            assertEquals(1, whileRunning.injected());
            assertEquals(0, whileRunning.parked());

            release.countDown();
            defaultNioFlow.join();

            Diagnostics afterJoin = defaultNioFlow.diagnostics();
            assertEquals(0, afterJoin.active());
            assertEquals(1, afterJoin.parked());
        }
    }

    @Test
    void batchedCountsValuesWaitingInBuffers() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.batch(10, Duration.ofSeconds(5), group -> group);

            defaultNioFlow.just(1);
            defaultNioFlow.just(2);
            awaitBatched(defaultNioFlow, 2);

            assertEquals(2, defaultNioFlow.diagnostics().batched());
        }
    }

    @Test
    void sealedAndClosedShowInTheDump() {
        DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>();
        defaultNioFlow.handle(x -> x).seal();
        assertTrue(defaultNioFlow.diagnostics().sealed());
        assertFalse(defaultNioFlow.diagnostics().closed());

        defaultNioFlow.close();
        assertTrue(defaultNioFlow.diagnostics().closed());
    }

    @Test
    void toStringRendersTheFullDump() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle("validate", x -> x);
            defaultNioFlow.just(1);
            defaultNioFlow.join();

            String dump = defaultNioFlow.toString();
            assertTrue(dump.startsWith("NioFlow["));
            assertTrue(dump.contains("active=0"));
            assertTrue(dump.contains("parked=1"));
            assertTrue(dump.contains("injected=1"));
            assertTrue(dump.contains("1. handle[validate]"));
        }
    }

    private static void awaitBatched(DefaultNioFlow<Integer> defaultNioFlow, int expected) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (defaultNioFlow.diagnostics().batched() < expected && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
    }
}
