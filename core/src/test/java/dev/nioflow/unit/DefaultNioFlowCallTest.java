package dev.nioflow.unit;

import dev.nioflow.application.facade.DefaultNioFlow;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

class DefaultNioFlowCallTest {

    @Test
    void eachCallResolvesWithItsOwnValuesResult() throws Exception {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.submit(x -> {
                if (x == 1) {
                    sleep(200); // the slow value must not steal the fast value's reply
                }
                return x * 10;
            });

            CompletableFuture<Integer> slow = defaultNioFlow.call(1);
            CompletableFuture<Integer> fast = defaultNioFlow.call(2);

            assertEquals(20, fast.get());
            assertEquals(10, slow.get());
        }
    }

    @Test
    void callDeliversTheEndOfChainTypeAcrossAdapt() throws Exception {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle(x -> x + 1)
                    .adapt(x -> "value-" + x);

            CompletableFuture<String> reply = defaultNioFlow.call(41);

            assertEquals("value-42", reply.get());
        }
    }

    @Test
    void aFailingValueFailsOnlyItsOwnFuture() throws Exception {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle(x -> {
                if (x < 0) {
                    throw new IllegalStateException("negative");
                }
                return x;
            });

            CompletableFuture<Integer> failing = defaultNioFlow.call(-1);
            CompletableFuture<Integer> fine = defaultNioFlow.call(7);

            assertEquals(7, fine.get());
            ExecutionException failure = assertThrows(ExecutionException.class, failing::get);
            assertInstanceOf(IllegalStateException.class, failure.getCause());
        }
    }

    @Test
    void aRecoveredValueDeliversTheFallback() throws Exception {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle(x -> {
                        throw new IllegalStateException("boom");
                    })
                    .onErrorResume(error -> -1)
                    .handle(x -> x * 10);

            assertEquals(-10, defaultNioFlow.call(1).get());
        }
    }

    @Test
    void aFilteredValueCancelsItsFuture() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.filter(x -> x > 0);

            CompletableFuture<Integer> dropped = defaultNioFlow.call(-1);

            assertThrows(CancellationException.class, dropped::get);
        }
    }

    @Test
    void anEmptyFanOutCancelsItsFuture() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.fanOut(x -> List.of());

            CompletableFuture<Object> vanished = defaultNioFlow.call(1);

            assertThrows(CancellationException.class, vanished::get);
        }
    }

    @Test
    void aRejectedCallFailsTheFutureInsteadOfThrowing() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle(x -> x);
            defaultNioFlow.close();

            CompletableFuture<Integer> rejected = defaultNioFlow.call(1);

            ExecutionException failure = assertThrows(ExecutionException.class, rejected::get);
            assertInstanceOf(RejectedExecutionException.class, failure.getCause());
        }
    }

    @Test
    void aBoundedCallTimesOutWhileTheValueKeepsFlowing() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.submit(x -> {
                sleep(500);
                return x;
            });

            CompletableFuture<Integer> bounded = defaultNioFlow.call(1, Duration.ofMillis(50));

            ExecutionException failure = assertThrows(ExecutionException.class, bounded::get);
            assertInstanceOf(TimeoutException.class, failure.getCause());
        }
    }

    @Test
    void aReleasedFlowKeepsMemoryFlatWithoutFreezingTheChain() throws Exception {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle("increment", x -> x + 1).release();

            for (int i = 0; i < 100; i++) {
                assertEquals(i + 1, defaultNioFlow.call(i).get());
            }
            assertEquals(0, defaultNioFlow.diagnostics().parked(), "released flows must not retain values");

            defaultNioFlow.replace("increment", segment -> segment.handle(x -> x * 2));
            assertEquals(6, defaultNioFlow.call(3).get(), "the chain must stay editable after release");
        }
    }

    @Test
    void oneSharedFlowServesManyCallersAcrossRuntimeEdits() throws Exception {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle("routes", x -> x).release();

            defaultNioFlow.replace("routes", segment -> segment.match()
                    .is(x -> x > 100, lane -> lane.handle(x -> x - 100))
                    .is(x -> x > 10, lane -> lane.handle(x -> x - 10)));

            assertEquals(1, defaultNioFlow.call(101, Duration.ofSeconds(2)).get());
            assertEquals(1, defaultNioFlow.call(11, Duration.ofSeconds(2)).get());

            defaultNioFlow.replace("routes", segment -> segment.match()
                    .is(x -> x > 100, lane -> lane.handle(x -> x + 100)));

            assertEquals(201, defaultNioFlow.call(101, Duration.ofSeconds(2)).get());
            assertEquals(11, defaultNioFlow.call(11, Duration.ofSeconds(2)).get(),
                    "the retired route must be gone");
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
