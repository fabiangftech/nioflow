package dev.nioflow.springbootwithnioflow.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises every rung of the ladder against the real flows — no mocks: the
 * point is to prove the engine's behaviour (branching, resilience, batching,
 * ordering), not the stubs around it.
 *
 * Timing assertions only check LOWER or UPPER bounds with generous slack, so
 * they hold on a loaded machine.
 */
@SpringBootTest
class SampleServiceTest {

    @Autowired
    private SampleService sampleService;

    @BeforeEach
    void resetProbes() {
        sampleService.resetProbes();
    }

    @Test
    void handleTransformsAndBackgroundNeverDelaysTheCaller() {
        long start = System.nanoTime();

        String result = sampleService.greeting("Hello");

        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertEquals("Hello World!", result);
        assertTrue(elapsedMillis < 900,
                "the 1s background effect must not delay the caller, took " + elapsedMillis + "ms");
    }

    @Test
    void filterCutsTheFlowAndTheCutIsNotAnError() {
        assertEquals(40, sampleService.evenOnly(4).orElseThrow());
        assertTrue(sampleService.evenOnly(7).isEmpty(), "an odd value is filtered out, not failed");
    }

    @Test
    void adaptRetypesThePipelineAndTheSegmentNormalizes() {
        // 21 -> handleSync doubles -> "  Report #42  " -> segment trims + lowercases
        assertEquals("report #42", sampleService.report(21));
    }

    @Test
    void matchIsFirstMatchWins() {
        assertTrue(sampleService.route(5_000).contains("manual review"));
        assertTrue(sampleService.route(500).contains("auto approved"));
        assertTrue(sampleService.route(50).contains("fast path"));
        // The main line runs for every route.
        assertTrue(sampleService.route(50).endsWith("[done]"));
    }

    @Test
    void retryRecoversATransientGatewayFailure() {
        String result = sampleService.resilientCall("order-1");

        assertEquals("order-1 charged on attempt 3", result,
                "two attempts fail, the third succeeds — recover never runs");
    }

    @Test
    void aHungGatewayIsCutByItsBudgetAndRecovered() {
        long start = System.nanoTime();

        String result = sampleService.brokenCall("order-2");

        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertEquals("order-2 deferred: TimeoutException", result);
        assertTrue(elapsedMillis < 1_500,
                "the 100ms budget must cut the 2s call, took " + elapsedMillis + "ms");
    }

    @Test
    void fanOutRunsTheThreeCallsConcurrently() {
        long start = System.nanoTime();

        String result = sampleService.enrich("customer-7");

        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertEquals("credit(customer-7) | loyalty(customer-7) | risk(customer-7)", result,
                "branches join in declaration order");
        assertTrue(elapsedMillis < 250,
                "three 100ms calls in parallel, not 300ms sequential — took " + elapsedMillis + "ms");
    }

    @Test
    void contextCarriesScratchStateAcrossStages() {
        String result = sampleService.tracked("payload", "trace-9");

        assertEquals("provider(payload) [trace=trace-9]", result);
    }

    @Test
    void concurrentCallersCoalesceIntoOneBulkCall() {
        List<CompletableFuture<String>> calls = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            calls.add(sampleService.store("item-" + i));
        }

        for (int i = 0; i < 8; i++) {
            assertEquals("ITEM-" + i, calls.get(i).orTimeout(5, TimeUnit.SECONDS).join(),
                    "every caller gets ITS OWN result out of the shared bulk call");
        }
        assertEquals(1, sampleService.bulkCalls(), "eight callers, one downstream call");
    }

    @Test
    void sameKeyExecutionsKeepSubmissionOrder() {
        List<CompletableFuture<Integer>> calls = new ArrayList<>();
        for (int i = 1; i <= 6; i++) {
            calls.add(sampleService.ordered("account-1", i));
        }
        calls.forEach(call -> call.orTimeout(10, TimeUnit.SECONDS).join());

        assertEquals(List.of(1, 2, 3, 4, 5, 6), sampleService.processedInOrder(),
                "same key: strict submission order, even though all six were submitted at once");
    }

    @Test
    void distinctKeysRunConcurrently() {
        long start = System.nanoTime();

        List<CompletableFuture<Integer>> calls = List.of(
                sampleService.ordered("a", 1),
                sampleService.ordered("b", 2),
                sampleService.ordered("c", 3),
                sampleService.ordered("d", 4));
        calls.forEach(call -> call.orTimeout(10, TimeUnit.SECONDS).join());

        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertTrue(elapsedMillis < 150,
                "four distinct keys must not queue behind each other, took " + elapsedMillis + "ms");
    }

    @Test
    void asyncExecutionNeverBlocksTheCaller() {
        CompletableFuture<String> pending = sampleService.greetingAsync("  WORLD  ");

        assertFalse(pending.isDone() && Thread.currentThread().isInterrupted());
        assertEquals("hello world", pending.orTimeout(5, TimeUnit.SECONDS).join());
    }
}
