package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Keyed execution: just(x).key(k) pins same-key executions to one boss and
 * runs them strictly one at a time in submission order (Kafka-partition
 * style). Distinct keys — and unkeyed executions — keep full parallelism.
 */
class DefaultNioFlowKeyedTest extends EngineTestSupport {

    @Test
    void sameKeyExecutionsProcessInSubmissionOrder() {
        List<Integer> processed = new CopyOnWriteArrayList<>();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("record", value -> {
            // Jitter makes unordered processing visible: later submissions
            // would overtake earlier ones without the key lane.
            LockSupport.parkNanos((10 - value % 10) * 1_000_000L);
            processed.add(value);
            return value;
        });
        engine.seal();

        List<CompletableFuture<Integer>> calls = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            calls.add(flow.just(i).key("order-42").executeAsync());
        }
        calls.forEach(call -> call.orTimeout(30, TimeUnit.SECONDS).join());

        assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19),
                processed, "same-key executions must process in submission order");
    }

    @Test
    void sameKeyExecutionsNeverInterleaveTheirStages() {
        List<String> steps = new CopyOnWriteArrayList<>();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("first", value -> {
            steps.add(value + "a");
            LockSupport.parkNanos(5_000_000L);
            return value;
        }).handle("second", value -> {
            steps.add(value + "b");
            return value;
        });
        engine.seal();

        CompletableFuture<Integer> one = flow.just(1).key("acct").executeAsync();
        CompletableFuture<Integer> two = flow.just(2).key("acct").executeAsync();
        one.orTimeout(10, TimeUnit.SECONDS).join();
        two.orTimeout(10, TimeUnit.SECONDS).join();

        assertEquals(List.of("1a", "1b", "2a", "2b"), steps,
                "execution 2 must not start before execution 1 finished its whole chain");
    }

    @Test
    void distinctKeysRunConcurrently() throws Exception {
        var slowKeyEntered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("gate", value -> {
            if (value == 1) {
                slowKeyEntered.countDown();
                try {
                    release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return value * 10;
        });
        engine.seal();

        CompletableFuture<Integer> slow = flow.just(1).key("slow-key").executeAsync();
        assertTrue(slowKeyEntered.await(5, TimeUnit.SECONDS));

        // A DIFFERENT key completes while slow-key's execution is parked.
        assertEquals(20, flow.just(2).key("fast-key").executeAsync()
                .orTimeout(5, TimeUnit.SECONDS).join());
        // And unkeyed traffic is untouched too.
        assertEquals(30, flow.just(3).execute());

        release.countDown();
        assertEquals(10, slow.orTimeout(5, TimeUnit.SECONDS).join());
    }

    @Test
    void aFailedExecutionReleasesItsLane() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("boom-on-one", value -> {
            if (value == 1) {
                throw new IllegalStateException("boom");
            }
            return value * 10;
        });
        engine.seal();

        CompletableFuture<Integer> failing = flow.just(1).key("k").executeAsync()
                .orTimeout(5, TimeUnit.SECONDS);
        CompletableFuture<Integer> following = flow.just(2).key("k").executeAsync()
                .orTimeout(5, TimeUnit.SECONDS);

        assertThrows(CompletionException.class, failing::join);
        assertEquals(20, following.join(), "a failure must hand the lane to the next execution");
    }

    @Test
    void sameKeyAlwaysLandsOnTheSameBoss() {
        List<String> bosses = new CopyOnWriteArrayList<>();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.when(value -> {
            bosses.add(Thread.currentThread().getName());
            return true;
        }).then(lane -> lane.handle(value -> value));

        for (int i = 0; i < 10; i++) {
            flow.just(i).key("sticky").execute();
        }

        assertEquals(1, java.util.Set.copyOf(bosses).size(),
                "same key must always orchestrate on the same boss: " + bosses);
    }

    @Test
    void keyedLanesDrainAndDisappear() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("id", value -> value);
        engine.seal();

        for (int i = 0; i < 50; i++) {
            flow.just(i).key("key-" + i).execute();
        }

        assertEquals(0, engine.activeKeyLanes(), "drained lanes must be removed — no key leak");
    }

    @Test
    void keyOnTheSharedDefinitionIsRejected() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        assertThrows(IllegalStateException.class, () -> flow.key("nope"));
    }
}
