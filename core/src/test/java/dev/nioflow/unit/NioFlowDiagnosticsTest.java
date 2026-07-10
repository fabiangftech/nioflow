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
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            nioFlow.handle("validate", x -> x)
                    .submit(x -> x, Duration.ofSeconds(2))
                    .when(x -> x > 10)
                    .then(lane -> lane
                            .filter(x -> true))
                    .otherwise(lane -> lane
                            .fanOut(x -> List.of(x)))
                    .onErrorResume(error -> -1)
                    .batch(3, Duration.ofSeconds(1), group -> group);

            List<String> chain = nioFlow.diagnostics().chain();

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
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            nioFlow.submit(x -> {
                entered.countDown();
                try {
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return x;
            });

            nioFlow.just(1);
            assertTrue(entered.await(2, TimeUnit.SECONDS));

            Diagnostics whileRunning = nioFlow.diagnostics();
            assertEquals(1, whileRunning.active());
            assertEquals(1, whileRunning.injected());
            assertEquals(0, whileRunning.parked());

            release.countDown();
            nioFlow.join();

            Diagnostics afterJoin = nioFlow.diagnostics();
            assertEquals(0, afterJoin.active());
            assertEquals(1, afterJoin.parked());
        }
    }

    @Test
    void batchedCountsValuesWaitingInBuffers() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            nioFlow.batch(10, Duration.ofSeconds(5), group -> group);

            nioFlow.just(1);
            nioFlow.just(2);
            awaitBatched(nioFlow, 2);

            assertEquals(2, nioFlow.diagnostics().batched());
        }
    }

    @Test
    void sealedAndClosedShowInTheDump() {
        NioFlow<Integer> nioFlow = new NioFlow<>();
        nioFlow.handle(x -> x).seal();
        assertTrue(nioFlow.diagnostics().sealed());
        assertFalse(nioFlow.diagnostics().closed());

        nioFlow.close();
        assertTrue(nioFlow.diagnostics().closed());
    }

    @Test
    void toStringRendersTheFullDump() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            nioFlow.handle("validate", x -> x);
            nioFlow.just(1);
            nioFlow.join();

            String dump = nioFlow.toString();
            assertTrue(dump.startsWith("NioFlow["));
            assertTrue(dump.contains("active=0"));
            assertTrue(dump.contains("parked=1"));
            assertTrue(dump.contains("injected=1"));
            assertTrue(dump.contains("1. handle[validate]"));
        }
    }

    private static void awaitBatched(NioFlow<Integer> nioFlow, int expected) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (nioFlow.diagnostics().batched() < expected && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
    }
}
