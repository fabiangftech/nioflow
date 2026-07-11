package dev.nioflow.application.facade;

import dev.nioflow.core.model.Decision;
import dev.nioflow.core.model.Filter;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.Recovery;
import dev.nioflow.core.model.Splice;
import dev.nioflow.core.model.Stage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultNioEngineTest {

    private DefaultNioEngine engine;

    @BeforeEach
    void setUp() {
        engine = new DefaultNioEngine();
    }

    @AfterEach
    void tearDown() {
        engine.shutdown(Duration.ofMillis(100));
    }

    private static Stage stage(String name, Function<Object, Object> function) {
        return new Stage(name, function, false, null, List.of());
    }

    @Test
    void executesStagesInOrderAndReturnsResult() {
        engine.append(stage("plus-one", value -> (int) value + 1));
        engine.append(stage("times-two", value -> (int) value * 2));

        assertEquals(8, engine.call(3, new ConcurrentHashMap<>()).join());
    }

    @Test
    void orchestratesOnBossAndRunsStagesOnWorkers() {
        var bossThread = new AtomicReference<String>();
        var stageThread = new AtomicReference<String>();
        engine.append(new Decision(value -> {
            bossThread.set(Thread.currentThread().getName());
            return true;
        }, engine.nextDecision(), List.of()));
        engine.append(stage("capture", value -> {
            stageThread.set(Thread.currentThread().getName());
            return value;
        }));

        engine.call("x", new ConcurrentHashMap<>()).join();

        assertTrue(bossThread.get().startsWith("nio-flow-boss-"));
        assertFalse(stageThread.get().startsWith("nio-flow-boss-"));
    }

    @Test
    void guardsRouteLinksPerRequestWithoutSharingDecisions() {
        int decision = engine.nextDecision();
        engine.append(new Decision(value -> (int) value > 10, decision, List.of()));
        engine.append(new Stage("big", value -> "big:" + value, false, null,
                List.of(new Guard(decision, true))));
        engine.append(new Stage("small", value -> "small:" + value, false, null,
                List.of(new Guard(decision, false))));

        assertEquals("big:42", engine.call(42, new ConcurrentHashMap<>()).join());
        assertEquals("small:3", engine.call(3, new ConcurrentHashMap<>()).join());
    }

    @Test
    void filterCutsTheFlow() {
        engine.append(new Filter(value -> (int) value > 0, List.of()));
        engine.append(stage("after-filter", value -> "reached"));

        assertEquals("reached", engine.call(1, new ConcurrentHashMap<>()).join());
        assertNull(engine.call(-1, new ConcurrentHashMap<>()).join());
    }

    @Test
    void recoveryHandlesStageFailure() {
        engine.append(stage("boom", value -> {
            throw new IllegalStateException("boom");
        }));
        engine.append(new Recovery(error -> "recovered:" + error.getMessage(), List.of()));

        assertEquals("recovered:boom", engine.call("in", new ConcurrentHashMap<>()).join());
    }

    @Test
    void stageTimeoutTriggersRecovery() {
        engine.append(new Stage("sleepy", value -> {
            try {
                Thread.sleep(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return value;
        }, false, Duration.ofMillis(50), List.of()));
        engine.append(new Recovery(error -> "timed-out", List.of()));

        assertEquals("timed-out", engine.call("in", new ConcurrentHashMap<>()).join());
    }

    @Test
    void failureWithoutRecoveryReachesErrorHandlers() {
        var seen = new AtomicReference<Throwable>();
        engine.addErrorHandler(seen::set);
        engine.append(stage("boom", value -> {
            throw new IllegalStateException("boom");
        }));

        CompletableFuture<Object> result = engine.call("in", new ConcurrentHashMap<>());

        assertThrows(CompletionException.class, result::join);
        assertInstanceOf(IllegalStateException.class, seen.get());
    }

    @Test
    void spliceAtRuntimeDoesNotAffectInFlightCalls() {
        var gate = new CountDownLatch(1);
        engine.append(stage("slow", value -> {
            try {
                gate.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return value + ":slow";
        }));
        engine.seal();

        CompletableFuture<Object> inFlight = engine.call("a", new ConcurrentHashMap<>());
        engine.splice("slow", Splice.AFTER, List.of(stage("extra", value -> value + ":extra")));
        gate.countDown();

        assertEquals("a:slow", inFlight.join());
        assertEquals("b:slow:extra", engine.call("b", new ConcurrentHashMap<>()).join());
    }

    @Test
    void sealBlocksAppendUntilRelease() {
        engine.append(stage("first", value -> value));
        engine.seal();

        assertThrows(IllegalStateException.class, () -> engine.append(stage("second", value -> value)));

        engine.release();
        engine.append(stage("second", value -> value));
        assertEquals(2, engine.chain().size());
    }

    @Test
    void concurrentCallsDoNotShareState() {
        engine.append(stage("double", value -> (int) value * 2));

        List<CompletableFuture<Object>> calls = IntStream.range(0, 200)
                .mapToObj(i -> engine.call(i, new ConcurrentHashMap<>()))
                .toList();

        for (int i = 0; i < calls.size(); i++) {
            assertEquals(i * 2, calls.get(i).join());
        }
    }

    @Test
    void defaultEnginesShareTheBossPoolAndSurviveEachOthersShutdown() {
        var other = new DefaultNioEngine();
        var bossOfThis = new AtomicReference<String>();
        var bossOfOther = new AtomicReference<String>();
        engine.append(new Decision(value -> {
            bossOfThis.set(Thread.currentThread().getName());
            return true;
        }, engine.nextDecision(), List.of()));
        other.append(new Decision(value -> {
            bossOfOther.set(Thread.currentThread().getName());
            return true;
        }, other.nextDecision(), List.of()));

        engine.call("x", new ConcurrentHashMap<>()).join();
        // Shutting one engine down must never starve the others: executors are shared.
        engine.shutdown(Duration.ofMillis(100));
        assertEquals("y", other.call("y", new ConcurrentHashMap<>()).join());

        assertTrue(bossOfThis.get().startsWith("nio-flow-boss-"));
        assertTrue(bossOfOther.get().startsWith("nio-flow-boss-"));
    }

    @Test
    void injectIsAsyncAndAwaitCollectsTheResult() {
        engine.append(stage("upper", value -> value.toString().toUpperCase()));

        engine.inject("hola");

        assertEquals("HOLA", engine.await());
    }
}
