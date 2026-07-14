package dev.nioflow.stress;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.infrastructure.reactive.Reactive;
import dev.nioflow.infrastructure.reactive.ReactiveFlow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The number the whole trade-off turns on: what does ONE in-flight request cost
 * in retained heap while it waits on a remote call?
 *
 * <p>The JMH benchmarks cannot answer this — their Monos resolve immediately, so
 * nothing ever parks, and allocation rate is not retention. Here the calls never
 * complete, so every request sits exactly where a real one sits while the
 * downstream thinks: a nioflow request holds an Execution plus a PARKED VIRTUAL
 * THREAD (whose stack lives on the heap); a pure-Reactor request holds a few
 * state-machine objects.
 *
 * <p>Rough by nature (heap probes are), so it asserts only the order of
 * magnitude the RFC's decision tree depends on — and prints the real numbers,
 * which are what belongs in the docs.
 */
class ReactiveHeapProbeTest {

    private static final int IN_FLIGHT = 10_000;

    @Test
    void aParkedRequestCostsKilobytesInNioflowAndBytesInPlainReactor() throws Exception {
        long nioflowBytes = measureNioflow();
        long reactorBytes = measureReactor();

        System.out.printf("retained heap per in-flight request (%d concurrent):%n", IN_FLIGHT);
        System.out.printf("  nioflow + handleMono : %,d B%n", nioflowBytes);
        System.out.printf("  pure Reactor chain   : %,d B%n", reactorBytes);
        System.out.printf("  ratio                : %.1fx%n", (double) nioflowBytes / Math.max(reactorBytes, 1));

        // The claim under test: a parked request is cheap, but NOT free — and it
        // is the heavier of the two. If this ever inverts, the RFC's decision
        // tree is wrong and the docs must change with it.
        assertTrue(nioflowBytes > 0, "a parked request must retain something");
        assertTrue(nioflowBytes > reactorBytes,
                "nioflow parks a virtual thread; Reactor holds a state machine — nioflow: "
                        + nioflowBytes + " B, reactor: " + reactorBytes + " B");
        assertTrue(nioflowBytes < 100_000,
                "a parked virtual thread must stay in the KB range, not the 100s of KB; got " + nioflowBytes + " B");
    }

    /** IN_FLIGHT executions, each parked on a Mono that never completes. */
    private long measureNioflow() throws Exception {
        var engine = new DefaultNioEngine();
        try {
            ReactiveFlow<Integer, Integer> flow = Reactive.flow(DefaultNioFlow.from(Integer.class, engine));
            // One sink every request awaits: it never emits, so every worker parks.
            Sinks.One<Integer> never = Sinks.one();
            var parked = new CountDownLatch(IN_FLIGHT);
            flow.handleMono("remote", value -> {
                parked.countDown();
                return never.asMono();
            });
            engine.seal();

            long before = usedHeap();
            List<CompletableFuture<Integer>> pending = new ArrayList<>(IN_FLIGHT);
            for (int i = 0; i < IN_FLIGHT; i++) {
                pending.add(flow.just(i).executeAsync());
            }
            assertTrue(parked.await(30, TimeUnit.SECONDS), "every request must reach the remote call");
            // Let the last workers actually park before reading the heap.
            Thread.sleep(500);
            long perRequest = (usedHeap() - before) / IN_FLIGHT;

            never.tryEmitValue(0);   // release them all
            CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new))
                    .orTimeout(30, TimeUnit.SECONDS).join();
            return perRequest;
        } finally {
            engine.shutdown(Duration.ofSeconds(10));
        }
    }

    /** IN_FLIGHT pure-Reactor chains, each awaiting the same never-completing Mono. */
    private long measureReactor() throws Exception {
        Sinks.One<Integer> never = Sinks.one();

        long before = usedHeap();
        List<CompletableFuture<Integer>> pending = new ArrayList<>(IN_FLIGHT);
        for (int i = 0; i < IN_FLIGHT; i++) {
            int input = i;
            pending.add(Mono.just(input)
                    .flatMap(value -> never.asMono().map(ignored -> value))
                    .toFuture());
        }
        Thread.sleep(500);
        long perRequest = (usedHeap() - before) / IN_FLIGHT;

        never.tryEmitValue(0);
        CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new))
                .orTimeout(30, TimeUnit.SECONDS).join();
        return perRequest;
    }

    private static long usedHeap() throws InterruptedException {
        for (int i = 0; i < 3; i++) {
            System.gc();
            Thread.sleep(100);
        }
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}
