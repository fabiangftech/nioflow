package dev.nioflow.application.facade;

import dev.nioflow.core.model.Decision;
import dev.nioflow.core.model.OverflowPolicy;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Dedicated event loop per engine: DefaultNioEngine.dedicated(bossCount)
 * owns its boss pool (nobody else queues orchestration behind it) and its
 * worker pool; shutdown() terminates both. Executions keep their one-boss
 * affinity, and bossCount bounds the pool.
 */
class DefaultNioEngineDedicatedPoolTest {

    @Test
    void dedicatedEngineOrchestratesOnItsOwnBosses() {
        var engine = DefaultNioEngine.dedicated(2);
        var bossThread = new AtomicReference<String>();
        engine.append(new Decision(value -> {
            bossThread.set(Thread.currentThread().getName());
            return true;
        }, engine.nextDecision(), List.of()));

        engine.call("x", new ConcurrentHashMap<>()).join();

        assertTrue(bossThread.get().startsWith("nio-flow-boss-dedicated-"),
                "expected a dedicated boss, was " + bossThread.get());
        engine.shutdown(Duration.ofMillis(200));
    }

    @Test
    void bossCountBoundsThePoolAndAffinityHolds() {
        var engine = DefaultNioEngine.dedicated(2);
        List<String> firstLink = new CopyOnWriteArrayList<>();
        List<String> secondLink = new CopyOnWriteArrayList<>();
        engine.append(new Decision(value -> {
            firstLink.add(Thread.currentThread().getName());
            return true;
        }, engine.nextDecision(), List.of()));
        engine.append(EngineTestSupport.stage("hop", value -> value));
        engine.append(new Decision(value -> {
            secondLink.add(Thread.currentThread().getName());
            return true;
        }, engine.nextDecision(), List.of()));

        // Sequential on purpose: the two capture lists must line up per
        // execution, which concurrent interleaving would not guarantee.
        for (int i = 0; i < 40; i++) {
            engine.call(i, new ConcurrentHashMap<>()).join();
        }

        // The pool never exceeds bossCount...
        assertEquals(2, Set.copyOf(firstLink).size());
        // ...and each execution stays pinned to ITS boss across the worker hop.
        for (int i = 0; i < firstLink.size(); i++) {
            assertEquals(firstLink.get(i), secondLink.get(i),
                    "execution " + i + " changed boss across a dispatch");
        }
        engine.shutdown(Duration.ofMillis(200));
    }

    @Test
    void shutdownStopsDedicatedBossThreads() throws Exception {
        var engine = DefaultNioEngine.dedicated(1);
        engine.append(EngineTestSupport.stage("noop", value -> value));
        engine.call(1, new ConcurrentHashMap<>()).join();

        assertEquals(0, engine.shutdown(Duration.ofSeconds(1)));

        // The engine-owned boss thread must die; shared engines keep theirs.
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (dedicatedBossAlive() && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertTrue(!dedicatedBossAlive(), "dedicated boss thread survived shutdown");
    }

    private static boolean dedicatedBossAlive() {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(thread -> thread.isAlive() && thread.getName().startsWith("nio-flow-boss-dedicated-"));
    }

    @Test
    void sharedEnginesSurviveADedicatedShutdown() {
        var dedicated = DefaultNioEngine.dedicated(1);
        var shared = new DefaultNioEngine();
        shared.append(EngineTestSupport.stage("double", value -> (int) value * 2));

        dedicated.shutdown(Duration.ofMillis(200));

        assertEquals(10, shared.call(5, new ConcurrentHashMap<>()).join());
        shared.shutdown(Duration.ofMillis(200));
    }

    @Test
    void dedicatedComposesWithBackpressure() {
        var engine = DefaultNioEngine.dedicated(1, 1, OverflowPolicy.FAIL);
        engine.append(EngineTestSupport.stage("slow", value -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return value;
        }));

        engine.inject(1);
        assertThrows(RejectedExecutionException.class, () -> engine.inject(2));
        assertEquals(1, engine.await());
        engine.shutdown(Duration.ofMillis(500));
    }

    @Test
    void atLeastOneBossIsRequired() {
        assertThrows(IllegalArgumentException.class, () -> DefaultNioEngine.dedicated(0));
    }
}
