package dev.nioflow.application.facade;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultNioEngineDrainTest extends EngineTestSupport {

    @Test
    void cleanShutdownWithNothingInFlightReturnsZero() {
        engine.append(stage("plus", value -> (int) value + 1));

        assertEquals(6, engine.call(5, new ConcurrentHashMap<>()).join());
        assertEquals(0, engine.shutdown(Duration.ofSeconds(1)));
    }

    @Test
    void drainWaitsForInFlightExecutionsToFinish() throws Exception {
        var gate = new CountDownLatch(1);
        engine.append(stage("slow", value -> {
            try {
                gate.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return value + ":done";
        }));

        CompletableFuture<Object> inFlight = engine.call("a", new ConcurrentHashMap<>());
        var releaser = Thread.ofVirtual().start(() -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            gate.countDown();
        });

        int pending = engine.shutdown(Duration.ofSeconds(5)); // waits for the execution

        assertEquals(0, pending);
        assertEquals("a:done", inFlight.join());
        releaser.join();
    }

    @Test
    void drainReportsExecutionsStillRunningAfterTheGrace() {
        var gate = new CountDownLatch(1);
        engine.append(stage("stuck", value -> {
            try {
                gate.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return value;
        }));

        CompletableFuture<Object> straggler = engine.call("a", new ConcurrentHashMap<>());

        int pending = engine.shutdown(Duration.ofMillis(150)); // the gate never opened in time

        assertEquals(1, pending);
        gate.countDown();
        assertEquals("a", straggler.join()); // shared executors: the straggler still finishes
    }

    @Test
    void shutdownRejectsNewCallsAndInjections() {
        engine.append(stage("plus", value -> (int) value + 1));
        var rejections = new AtomicReference<Throwable>();
        engine.addErrorHandler(rejections::set);

        assertEquals(0, engine.shutdown(Duration.ofMillis(50)));

        var failure = assertThrows(CompletionException.class,
                () -> engine.call(1, new ConcurrentHashMap<>()).join());
        assertInstanceOf(RejectedExecutionException.class, failure.getCause());

        rejections.set(null);
        engine.inject(2);
        assertInstanceOf(RejectedExecutionException.class, rejections.get());
    }
}
