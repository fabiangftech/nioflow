package dev.nioflow.stress;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.infrastructure.reactive.Reactive;
import dev.nioflow.infrastructure.reactive.ReactiveFlow;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The number the whole trade-off turns on: what does ONE in-flight request cost
 * in retained heap while it waits on a remote call?
 *
 * <p>The JMH benchmarks cannot answer this — their Monos resolve immediately, so
 * nothing ever parks, and allocation rate is not retention. Here the calls never
 * complete, so every request sits exactly where a real one sits while the
 * downstream thinks. Three columns, because there are three answers:
 *
 * <ul>
 * <li><b>handleMono</b> — an Execution plus a PARKED VIRTUAL THREAD, whose stack
 *     chunk lives on the heap. This is the 16.8× that RFC 0002 measured and that
 *     its decision tree used to send people away from the library;</li>
 * <li><b>handleMonoAsync</b> — an Execution plus a CompletableFuture. No thread
 *     waits, so no stack is retained. This is the acceptance gate of RFC 0006:
 *     if it does not land near pure Reactor, the async link buys nothing and
 *     does not ship;</li>
 * <li><b>pure Reactor</b> — a few state-machine objects, and the floor.</li>
 * </ul>
 *
 * <p>Rough by nature (heap probes are), so it asserts orders of magnitude — and
 * prints the real numbers, which are what belongs in the docs.
 */
class ReactiveHeapProbeTest {

    private static final int IN_FLIGHT = 10_000;

    @Test
    void theAsyncStageRetainsNoParkedThreadAndLandsNearPlainReactor() throws Exception {
        long parkingBytes = measureNioflow((flow, sink) ->
                flow.handleMono("remote", value -> sink.get()));
        long asyncBytes = measureNioflow((flow, sink) ->
                flow.handleMonoAsync("remote", value -> sink.get()));
        long reactorBytes = measureReactor();

        System.out.printf("retained heap per in-flight request (%d concurrent):%n", IN_FLIGHT);
        System.out.printf("  nioflow + handleMono      : %,d B%n", parkingBytes);
        System.out.printf("  nioflow + handleMonoAsync : %,d B%n", asyncBytes);
        System.out.printf("  pure Reactor chain        : %,d B%n", reactorBytes);
        System.out.printf("  parking vs reactor        : %.1fx%n", ratio(parkingBytes, reactorBytes));
        System.out.printf("  async   vs reactor        : %.1fx%n", ratio(asyncBytes, reactorBytes));

        assertTrue(parkingBytes > 0, "a parked request must retain something");
        assertTrue(parkingBytes > reactorBytes,
                "handleMono parks a virtual thread; Reactor holds a state machine — parking: "
                        + parkingBytes + " B, reactor: " + reactorBytes + " B");
        assertTrue(parkingBytes < 100_000,
                "a parked virtual thread must stay in the KB range; got " + parkingBytes + " B");

        // The gate. An async stage that retained a thread's worth of heap would
        // be a fusion loss bought with nothing, and RFC 0006 would not ship.
        assertTrue(asyncBytes < parkingBytes / 2,
                "handleMonoAsync must retain far less than the parked thread it replaces — async: "
                        + asyncBytes + " B, parking: " + parkingBytes + " B");
        assertTrue(asyncBytes < 4 * Math.max(reactorBytes, 1),
                "handleMonoAsync must land within a small factor of pure Reactor — async: "
                        + asyncBytes + " B, reactor: " + reactorBytes + " B");
    }

    /**
     * The RFC 0015 gate: a {@code pipe} at high concurrency holds futures, not
     * parked workers. Same never-completing remote call, but reached through
     * {@code pipe} — whose {@code handleMono} routes to the async path by default,
     * so every in-flight element retains an Execution and a CompletableFuture, not
     * a parked virtual thread's stack.
     */
    @Test
    void anAsyncRoutedPipeHoldsFuturesNotParkedThreads() throws Exception {
        long parkingBytes = measureNioflow((flow, sink) ->
                flow.handleMono("remote", value -> sink.get()));
        long pipeBytes = measureAsyncPipe();

        System.out.printf("retained heap per in-flight element (%d concurrent):%n", IN_FLIGHT);
        System.out.printf("  nioflow + handleMono (parked) : %,d B%n", parkingBytes);
        System.out.printf("  pipe (async-routed)           : %,d B%n", pipeBytes);

        assertTrue(pipeBytes > 0, "an in-flight element must retain something");
        assertTrue(pipeBytes < parkingBytes,
                "an async-routed pipe must retain less than a parked worker per element — pipe: "
                        + pipeBytes + " B, parking: " + parkingBytes + " B");
        // The parked worker's stack chunk is ~2 KB+; the async element (Execution
        // + a CompletableFuture + Reactor's flatMap inner) stays around 1 KB. An
        // absolute bound, because the parked measurement is the noisy one.
        assertTrue(pipeBytes < 2_000,
                "an async-routed pipe element must not retain a parked thread's stack — pipe: "
                        + pipeBytes + " B");
    }

    /** IN_FLIGHT elements through a pipe, each awaiting the same never-completing Mono. */
    private long measureAsyncPipe() throws Exception {
        var engine = new DefaultNioEngine();
        try {
            ReactiveFlow<Integer, Integer> flow = Reactive.flow(DefaultNioFlow.from(Integer.class, engine));
            Sinks.One<Integer> never = Sinks.one();
            var reached = new CountDownLatch(IN_FLIGHT);
            Function<Flux<Integer>, Flux<Integer>> pipe = flow.pipe(IN_FLIGHT, (input, step) -> step
                    .handleMono("remote", value -> {
                        reached.countDown();
                        return never.asMono();
                    }));
            engine.seal();

            long before = usedHeap();
            Disposable subscription = pipe.apply(Flux.range(1, IN_FLIGHT)).subscribe();
            assertTrue(reached.await(30, TimeUnit.SECONDS), "every element must reach the remote call");
            Thread.sleep(500);
            long perElement = (usedHeap() - before) / IN_FLIGHT;

            never.tryEmitValue(0);   // release them all
            Thread.sleep(500);
            subscription.dispose();
            return perElement;
        } finally {
            engine.shutdown(Duration.ofSeconds(10));
        }
    }

    /**
     * IN_FLIGHT executions, each waiting on a Mono that never completes. The
     * step under test is the parameter: the only difference between the two
     * nioflow columns is which one of them the flow declared.
     */
    private long measureNioflow(BiConsumer<ReactiveFlow<Integer, Integer>, Sink> step) throws Exception {
        var engine = new DefaultNioEngine();
        try {
            ReactiveFlow<Integer, Integer> flow = Reactive.flow(DefaultNioFlow.from(Integer.class, engine));
            // One sink every request awaits: it never emits, so every request
            // sits at the remote call — parked on a worker, or on nothing.
            Sinks.One<Integer> never = Sinks.one();
            var reached = new CountDownLatch(IN_FLIGHT);
            step.accept(flow, () -> {
                reached.countDown();
                return never.asMono();
            });
            engine.seal();

            long before = usedHeap();
            List<CompletableFuture<Integer>> pending = new ArrayList<>(IN_FLIGHT);
            for (int i = 0; i < IN_FLIGHT; i++) {
                pending.add(flow.just(i).executeAsync());
            }
            assertTrue(reached.await(30, TimeUnit.SECONDS), "every request must reach the remote call");
            // Let the last workers actually park (or actually leave) before reading.
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

    /** The call the probe hands to whichever step is being measured. */
    private interface Sink {
        Mono<Integer> get();
    }

    private static double ratio(long value, long floor) {
        return (double) value / Math.max(floor, 1);
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
