package dev.nioflow.application.facade;

import dev.nioflow.core.model.Decision;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.Stage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    @Test
    void handlersRunBeforeTheCallerObservesTheResult() {
        // The raw result future is returned to the caller directly, so the
        // bookkeeping must run BEFORE it completes — a caller returning from
        // join() must already see complete handlers applied.
        var observed = new AtomicReference<Object>();
        engine.append(stage("double", value -> (int) value * 2));
        engine.addCompleteHandler(observed::set);

        assertEquals(6, engine.call(3, new ConcurrentHashMap<>()).join());
        assertEquals(6, observed.get());
    }

    @Test
    void callOnAShutDownOwnedBossFailsTheFutureInsteadOfHangingIt() {
        ExecutorService boss = Executors.newSingleThreadExecutor();
        ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
        var ownedEngine = new DefaultNioEngine(boss, workers);
        ownedEngine.append(stage("never", value -> value));
        boss.shutdownNow();

        CompletableFuture<Object> result = ownedEngine.call(1, new ConcurrentHashMap<>())
                .orTimeout(5, TimeUnit.SECONDS);

        assertThrows(CompletionException.class, result::join);
        workers.shutdownNow();
    }

    @Test
    void bossShutdownMidFlightFailsTheFutureInsteadOfHangingIt() throws Exception {
        ExecutorService boss = Executors.newSingleThreadExecutor();
        ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
        var ownedEngine = new DefaultNioEngine(boss, workers);
        var stageEntered = new CountDownLatch(1);
        var bossGone = new CountDownLatch(1);
        engineStageAwaiting(ownedEngine, stageEntered, bossGone);

        CompletableFuture<Object> result = ownedEngine.call(1, new ConcurrentHashMap<>())
                .orTimeout(5, TimeUnit.SECONDS);
        assertTrue(stageEntered.await(5, TimeUnit.SECONDS));
        boss.shutdownNow();
        bossGone.countDown();

        // The worker finishes but cannot resume on the boss: the execution
        // must end exceptionally, never leave the future hanging.
        assertThrows(CompletionException.class, result::join);
        workers.shutdownNow();
    }

    private static void engineStageAwaiting(DefaultNioEngine engine, CountDownLatch entered, CountDownLatch resume) {
        engine.append(stage("parked", value -> {
            entered.countDown();
            try {
                resume.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return value;
        }));
    }
}
