package dev.nioflow.application.facade;

import dev.nioflow.core.model.Link;
import dev.nioflow.core.model.Splice;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The chain and the plan compiled for it are published as ONE value. A reader
 * can never pair a chain with a plan built for another version of it — which is
 * what a separate chain field and a separate plan field would allow, since a
 * writer lands them one at a time.
 *
 * <p>The plan is an optimization, never a semantic: whichever version a call
 * lands on, compiled or interpreted, its result must be that version's result.
 */
class DefaultNioEngineChainPublicationTest extends EngineTestSupport {

    @Test
    void aSnapshotTakenBeforeAnEditKeepsRunningItsOwnLinks() {
        engine.append(stage("base", value -> value + ":base"));
        engine.seal();

        List<Link> snapshot = engine.chain();
        engine.splice("base", Splice.AFTER, List.of(stage("extra", value -> value + ":extra")));

        // The snapshot is no longer the live version, so it carries no plan and
        // is interpreted — but it still produces exactly what it describes.
        assertEquals("x:base", engine.call("x", null, snapshot).join());
        assertEquals("x:base:extra", engine.call("x", null).join());
        assertNotSame(snapshot, engine.chain());
    }

    @Test
    void appendInvalidatesThePlanAndSealRebuildsItForTheSameChain() {
        engine.append(stage("first", value -> value + ":1"));
        engine.seal();
        assertEquals("x:1", engine.call("x", null).join());

        engine.release();
        engine.append(stage("second", value -> value + ":2"));   // no plan: interpreted
        assertEquals("x:1:2", engine.call("x", null).join());

        engine.seal();                                           // recompiled for this exact chain
        assertEquals("x:1:2", engine.call("x", null).join());
    }

    /**
     * Splices landing while calls are in flight: every result must belong to
     * SOME version of the chain (never a hybrid, never a failure, never a hang).
     */
    @Test
    void concurrentEditsAndCallsNeverPairAChainWithAForeignPlan() throws Exception {
        engine.append(stage("head", value -> value + ":head"));
        engine.append(stage("tail", value -> value + ":tail"));
        engine.seal();

        Set<String> valid = Set.of("x:head:tail", "x:head:mid:tail");
        Set<String> observed = ConcurrentHashMap.newKeySet();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(5);

        for (int i = 0; i < 4; i++) {
            Thread caller = new Thread(() -> {
                try {
                    start.await();
                    for (int call = 0; call < 200; call++) {
                        observed.add((String) engine.call("x", null).join());
                    }
                } catch (Throwable error) {
                    failure.compareAndSet(null, error);
                } finally {
                    done.countDown();
                }
            });
            caller.start();
        }

        Thread editor = new Thread(() -> {
            try {
                start.await();
                List<Link> mid = List.of(stage("mid", value -> value + ":mid"));
                for (int edit = 0; edit < 50; edit++) {
                    engine.splice("tail", Splice.BEFORE, mid);
                    engine.splice("mid", Splice.REPLACE, List.of());
                }
            } catch (Throwable error) {
                failure.compareAndSet(null, error);
            } finally {
                done.countDown();
            }
        });
        editor.start();

        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS), "every caller and the editor finished");
        assertNull(failure.get(), () -> "no call or edit failed: " + failure.get());
        assertTrue(valid.containsAll(observed), () -> "every result belongs to a real version: " + observed);
    }

    /** An execution-local chain is a foreign list: it never matches the engine's plan. */
    @Test
    void executionLocalChainsAreAlwaysInterpreted() {
        engine.append(stage("shared", value -> value + ":shared"));
        engine.seal();

        List<Link> local = List.of(stage("local", value -> value + ":local"));

        CompletableFuture<Object> result = engine.call("x", null, local);

        assertEquals("x:local", result.join());
        assertEquals("x:shared", engine.call("x", null).join());   // the shared chain is untouched
    }
}
