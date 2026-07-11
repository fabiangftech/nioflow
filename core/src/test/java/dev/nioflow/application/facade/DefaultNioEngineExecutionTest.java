package dev.nioflow.application.facade;

import dev.nioflow.core.model.Decision;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.Stage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultNioEngineExecutionTest extends EngineTestSupport {

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
        engine.append(new Stage("big", value -> "big:" + value, false, null, null,
                List.of(new Guard(decision, true))));
        engine.append(new Stage("small", value -> "small:" + value, false, null, null,
                List.of(new Guard(decision, false))));

        assertEquals("big:42", engine.call(42, new ConcurrentHashMap<>()).join());
        assertEquals("small:3", engine.call(3, new ConcurrentHashMap<>()).join());
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
    void injectIsAsyncAndAwaitCollectsTheResult() {
        engine.append(stage("upper", value -> value.toString().toUpperCase()));

        engine.inject("hola");

        assertEquals("HOLA", engine.await());
    }
}
