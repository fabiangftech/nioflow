package dev.nioflow.application.facade;

import dev.nioflow.core.facade.ChainValidationException;
import dev.nioflow.core.facade.Context.Key;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.NioFlowMetrics;
import dev.nioflow.core.facade.Segment;
import dev.nioflow.core.model.Decision;
import dev.nioflow.core.model.Fork;
import dev.nioflow.core.model.Link;
import dev.nioflow.core.model.Splice;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * fork(name, segment): a DETACHED sub-flow. The main line hands it the value and
 * keeps going — it never waits for the fork, the fork's failures never reach the
 * caller's future, and the fork is a full pipeline (every step type, branching,
 * nested forks) rather than a lambda.
 *
 * <p>Branching (when/match) is a different feature and lives in
 * DefaultNioFlowBranchingTest: that one ROUTES the value, this one DETACHES it.
 */
class DefaultNioFlowForkTest extends EngineTestSupport {

    private static final Key<String> TRACE = Key.of("trace");

    @Test
    void mainLineDoesNotWaitForTheFork() throws InterruptedException {
        NioFlow<String, String> flow = DefaultNioFlow.from(String.class, engine);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch forkFinished = new CountDownLatch(1);

        String result = flow.just("value")
                .fork("slow", sub -> sub
                        .handle("park", awaitOn(release))
                        .handle("finish", value -> {
                            forkFinished.countDown();
                            return value;
                        }))
                .handle("main", value -> value + "-main")
                .execute();

        // The fork is still parked on the latch, yet the request is done: that
        // IS the feature. Nothing but the fork can release it.
        assertEquals("value-main", result);
        assertEquals(1, forkFinished.getCount());

        release.countDown();
        assertTrue(forkFinished.await(2, TimeUnit.SECONDS), "the fork must finish on its own");
    }

    @Test
    void forkFailureNeverFailsTheCallerAndReachesTheErrorHandler() throws InterruptedException {
        NioFlow<String, String> flow = DefaultNioFlow.from(String.class, engine);
        CountDownLatch reported = new CountDownLatch(1);
        List<Throwable> errors = new ArrayList<>();
        flow.onError(error -> {
            errors.add(error);
            reported.countDown();
        });

        String result = flow.just("value")
                .fork("boom", sub -> sub.handle("explode", value -> {
                    throw new IllegalStateException("fork blew up");
                }))
                .handle("main", value -> value + "-main")
                .execute();

        assertEquals("value-main", result);
        assertTrue(reported.await(2, TimeUnit.SECONDS), "an unrecovered fork failure must reach onError");
        assertEquals("fork blew up", errors.get(0).getMessage());
    }

    @Test
    void recoverInsideTheForkCatchesItsOwnFailure() throws InterruptedException {
        NioFlow<String, String> flow = DefaultNioFlow.from(String.class, engine);
        CountDownLatch recovered = new CountDownLatch(1);
        List<Throwable> errors = new ArrayList<>();
        flow.onError(errors::add);
        ConcurrentLinkedQueue<String> seen = new ConcurrentLinkedQueue<>();

        flow.just("value")
                .fork("guarded", sub -> sub
                        .handle("explode", value -> {
                            throw new IllegalStateException("caught inside");
                        })
                        .recover(error -> "recovered")
                        .handle("after", value -> {
                            seen.add(value);
                            recovered.countDown();
                            return value;
                        }))
                .execute();

        assertTrue(recovered.await(2, TimeUnit.SECONDS));
        assertEquals("recovered", seen.poll());
        assertTrue(errors.isEmpty(), "a recovered fork failure must not reach onError");
    }

    @Test
    void filterInsideTheForkCutsOnlyTheFork() throws InterruptedException {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        CountDownLatch forkReached = new CountDownLatch(1);
        ConcurrentLinkedQueue<Integer> forkTail = new ConcurrentLinkedQueue<>();

        Integer result = flow.just(3)
                .fork("cut", sub -> sub
                        .handle("mark", value -> {
                            forkReached.countDown();
                            return value;
                        })
                        .filter(value -> value > 100)
                        .handle("never", value -> {
                            forkTail.add(value);
                            return value;
                        }))
                .handle("main", value -> value * 2)
                .execute();

        assertEquals(6, result);
        assertTrue(forkReached.await(2, TimeUnit.SECONDS));
        assertTrue(forkTail.isEmpty(), "the filter cut the fork before its tail");
    }

    @Test
    void theForkRunsEveryStepType() throws InterruptedException {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        CountDownLatch done = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> outcome = new ConcurrentLinkedQueue<>();
        Segment<String, String> shout = lane -> lane.handle("shout", value -> value + "!");

        flow.just(4)
                .fork("everything", sub -> sub
                        .handleSync("double", value -> value * 2)                       // boss-inlined
                        .handle("plus", value -> value + 1)                             // worker
                        .handleContextual("stash", (value, ctx) -> {
                            ctx.put(TRACE, "seen-" + value);
                            return value;
                        })
                        .background("effect", value -> outcome.add("bg-" + value))      // fire-and-forget
                        .<Integer, Integer>fanOut(List.of(value -> value * 10, value -> value + 5),
                                results -> results.get(0) + results.get(1))             // 90 + 14 = 104
                        .when(value -> value > 100)
                            .then(lane -> lane.handle("big", value -> value + 1000))
                            .otherwise(lane -> lane.handle("small", value -> value - 1000))
                        .adapt(value -> "n=" + value)                                   // re-types freely
                        .use(shout)
                        .fork("nested", inner -> inner.background(value -> outcome.add("nested-" + value)))
                        .handleContextual("read", (value, ctx) -> value + "/" + ctx.get(TRACE))
                        .handle("done", value -> {
                            outcome.add(value);
                            done.countDown();
                            return value;
                        }))
                .execute();

        assertTrue(done.await(2, TimeUnit.SECONDS));
        // 4 -> 8 -> 9 -> fanOut(90, 14) -> 104 -> >100 so +1000 -> 1104
        assertTrue(outcome.contains("n=1104!/seen-9"), "unexpected fork outcome: " + outcome);
    }

    @Test
    void batchInsideTheForkCoalescesAcrossExecutions() throws InterruptedException {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        CountDownLatch batched = new CountDownLatch(3);
        ConcurrentLinkedQueue<Integer> bulkSizes = new ConcurrentLinkedQueue<>();
        // Declared on the SHARED definition: only then do executions of the
        // same flow meet at the same Batch link instance.
        flow.fork("bulk", sub -> sub
                .batch("group", 3, Duration.ofSeconds(1), values -> {
                    bulkSizes.add(values.size());
                    return values.stream().map(value -> value * 2).toList();
                })
                .handle("count", value -> {
                    batched.countDown();
                    return value;
                }));

        for (int i = 0; i < 3; i++) {
            flow.just(i).execute();
        }

        assertTrue(batched.await(3, TimeUnit.SECONDS));
        assertEquals(3, bulkSizes.poll(), "the three forks coalesced into one bulk call");
    }

    @Test
    void aForkDeclaredInsideALaneOnlySpawnsForValuesRoutedThere() throws InterruptedException {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        ConcurrentLinkedQueue<Integer> forked = new ConcurrentLinkedQueue<>();
        CountDownLatch spawned = new CountDownLatch(1);
        flow.when(value -> value > 10)
                .then(lane -> lane.fork("audit", sub -> sub.handle("record", value -> {
                    forked.add(value);
                    spawned.countDown();
                    return value;
                })))
                .otherwise(lane -> lane.handle("small", value -> value))
                .handle("main", value -> value + 1);
        engine.seal();

        assertEquals(4, flow.just(3).execute());
        assertEquals(43, flow.just(42).execute());

        assertTrue(spawned.await(2, TimeUnit.SECONDS));
        assertEquals(42, forked.poll());
        assertTrue(forked.isEmpty(), "the value routed down the other lane must not fork");
    }

    @Test
    void nestedForksInsideTheForkRunTheirOwnBranching() throws InterruptedException {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        CountDownLatch done = new CountDownLatch(2);
        ConcurrentLinkedQueue<String> routed = new ConcurrentLinkedQueue<>();

        Function<Integer, Integer> run = input -> flow.just(input)
                .fork("route", sub -> sub
                        .match()
                            .is(value -> value % 2 == 0, lane -> lane.handle("even", value -> {
                                routed.add("even-" + value);
                                done.countDown();
                                return value;
                            }))
                            .otherwise(lane -> lane.handle("odd", value -> {
                                routed.add("odd-" + value);
                                done.countDown();
                                return value;
                            })))
                .execute();

        run.apply(4);
        run.apply(7);

        assertTrue(done.await(2, TimeUnit.SECONDS));
        assertTrue(routed.contains("even-4") && routed.contains("odd-7"), "routed: " + routed);
    }

    @Test
    void theForkGetsACopyOfTheContextAndItsWritesStayInside() throws InterruptedException {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        CountDownLatch forkDone = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> insideFork = new ConcurrentLinkedQueue<>();

        Integer mainSaw = flow.just(1)
                .handleContextual("seed", (value, ctx) -> {
                    ctx.put(TRACE, "from-main");
                    return value;
                })
                .fork("detached", sub -> sub.handleContextual("touch", (value, ctx) -> {
                    insideFork.add(String.valueOf(ctx.get(TRACE)));   // sees the snapshot
                    ctx.put(TRACE, "from-fork");                      // invisible to the main line
                    forkDone.countDown();
                    return value;
                }))
                .handle("wait", value -> {
                    awaitQuietly(forkDone);
                    return value;
                })
                .handleContextual("read-back", (value, ctx) ->
                        "from-main".equals(ctx.get(TRACE)) ? value : -1)
                .execute();

        assertTrue(forkDone.await(2, TimeUnit.SECONDS));
        assertEquals("from-main", insideFork.poll(), "the fork reads the context as it was at the fork point");
        assertEquals(1, mainSaw, "a write inside the fork must not leak into the main line");
    }

    @Test
    void theForkNeverReachesTheCompleteHandlers() throws InterruptedException {
        NioFlow<String, String> flow = DefaultNioFlow.from(String.class, engine);
        ConcurrentLinkedQueue<String> completed = new ConcurrentLinkedQueue<>();
        CountDownLatch forkDone = new CountDownLatch(1);
        flow.onComplete(completed::add);

        flow.just("value")
                .fork("side", sub -> sub.handle("side-value", value -> {
                    forkDone.countDown();
                    return "fork-terminal";
                }))
                .handle("main", value -> value + "-main")
                .execute();

        assertTrue(forkDone.await(2, TimeUnit.SECONDS));
        // The handler promises the flow's output O; a fork's terminal value is
        // not one, so only the request reports here. Poll once more to make the
        // absence meaningful (the fork ran before we look).
        assertEquals("value-main", completed.poll());
        assertNull(completed.poll(), "the fork's terminal value must not reach onComplete");
    }

    @Test
    void shutdownDrainsInFlightForks() throws InterruptedException {
        DefaultNioEngine ownEngine = new DefaultNioEngine();
        NioFlow<String, String> flow = DefaultNioFlow.from(String.class, ownEngine);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch forkFinished = new CountDownLatch(1);

        flow.just("value")
                .fork("slow", sub -> sub
                        .handle("park", awaitOn(release))
                        .handle("finish", value -> {
                            forkFinished.countDown();
                            return value;
                        }))
                .execute();

        // The request is done, the fork is not: an impatient shutdown reports it.
        assertEquals(1, ownEngine.shutdown(Duration.ofMillis(50)));

        release.countDown();
        assertTrue(forkFinished.await(2, TimeUnit.SECONDS), "the straggler finishes on the shared executor");
        assertEquals(0, ownEngine.shutdown(Duration.ofSeconds(1)), "and the drain is clean afterwards");
    }

    @Test
    void compiledAndInterpretedChainsForkIdentically() throws InterruptedException {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        CountDownLatch done = new CountDownLatch(2);
        ConcurrentLinkedQueue<Integer> forked = new ConcurrentLinkedQueue<>();
        flow.handle("pre", value -> value + 1)
                .fork("audit", sub -> sub
                        .when(value -> value > 10)
                            .then(lane -> lane.handle("big", value -> value * 100))
                            .otherwise(lane -> lane.handle("small", value -> value * -1))
                        .handle("record", value -> {
                            forked.add(value);
                            done.countDown();
                            return value;
                        }))
                .handle("post", value -> value * 2);

        assertEquals(24, flow.just(11).execute());   // interpreted (no plan yet)
        engine.seal();                               // compiles the chain AND the fork's
        assertEquals(24, flow.just(11).execute());   // compiled

        assertTrue(done.await(2, TimeUnit.SECONDS));
        assertEquals(1200, forked.poll());
        assertEquals(1200, forked.poll(), "the plan is an optimization, never a semantic");
    }

    @Test
    void spliceReplacesTheWholeForkAtRuntime() throws InterruptedException {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        ConcurrentLinkedQueue<String> seen = new ConcurrentLinkedQueue<>();
        CountDownLatch first = new CountDownLatch(1);
        CountDownLatch second = new CountDownLatch(1);
        flow.fork("audit", sub -> sub.handle("v1", value -> {
            seen.add("v1-" + value);
            first.countDown();
            return value;
        }));
        engine.seal();

        flow.just(1).execute();
        assertTrue(first.await(2, TimeUnit.SECONDS));

        // The fork's name is a main-chain anchor: the whole sub-flow swaps at once.
        List<Link> replacement = recordFork("audit", lane -> lane.handle("v2", value -> {
            seen.add("v2-" + value);
            second.countDown();
            return value;
        }));
        engine.splice("audit", Splice.REPLACE, replacement);

        flow.just(2).execute();
        assertTrue(second.await(2, TimeUnit.SECONDS));
        assertEquals("v1-1", seen.poll());
        assertEquals("v2-2", seen.poll());
    }

    @Test
    void anEmptyForkIsRejectedAtBuildTime() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> flow.fork("nothing", sub -> sub));

        assertTrue(failure.getMessage().contains("Fork 'nothing' is empty"));
    }

    @Test
    void theForkSubChainIsValidatedInItsOwnScope() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        // Duplicate anchors INSIDE the fork: splice over the sub-chain would be
        // ambiguous, so seal() must reject it — and name the fork.
        flow.fork("audit", sub -> sub
                .handle("twice", value -> value)
                .handle("twice", value -> value));

        ChainValidationException failure = assertThrows(ChainValidationException.class, engine::seal);

        assertTrue(failure.getMessage().contains("fork 'audit'"), failure.getMessage());
        assertTrue(failure.getMessage().contains("twice"), failure.getMessage());
    }

    @Test
    void theForkSubChainIsGuardClosedAndItsDecisionsAreCompacted() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        // Burn engine-wide decision ids first: the fork's own ids must not
        // inherit them (they would push the child's bitset into its overflow map).
        flow.when(value -> value > 0).then(lane -> lane.handle(value -> value));
        flow.fork("audit", sub -> sub
                .when(value -> value > 1)
                    .then(lane -> lane.handle("a", value -> value))
                    .otherwise(lane -> lane.handle("b", value -> value)));
        engine.seal();

        Fork fork = engine.chain().stream()
                .filter(Fork.class::isInstance).map(Fork.class::cast)
                .findFirst().orElseThrow();
        Decision inner = fork.chain().stream()
                .filter(Decision.class::isInstance).map(Decision.class::cast)
                .findFirst().orElseThrow();

        // The main chain already burned id 0, so the fork's raw id was 1 —
        // renumbered to 0 because inside a fork the ids are private.
        assertEquals(0, inner.id());
        assertTrue(fork.chain().stream().allMatch(link -> link.guards().stream()
                .allMatch(guard -> guard.decision() == inner.id())),
                "every guard inside the fork names a decision declared inside the fork");
    }

    /**
     * Decision compaction has to rewrite the guards of EVERY link kind, not just
     * stages: the ids inside a fork are private, so they are renumbered to
     * 0..n-1, and each guarded link has to be rebuilt carrying the new ids. A
     * link kind missed by that switch would silently keep a stale id and stop
     * matching its own decision — the branch would go dead. So the sub-flow here
     * puts one of every guarded kind inside a lane.
     */
    @Test
    void compactionRewritesTheGuardsOfEveryLinkKind() throws InterruptedException {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        // Burn engine-wide ids first, so the fork's own ids really are renumbered.
        flow.when(value -> value > 0).then(lane -> lane.handle(value -> value));
        CountDownLatch done = new CountDownLatch(1);
        ConcurrentLinkedQueue<Object> seen = new ConcurrentLinkedQueue<>();
        List<Function<Integer, Integer>> branches = List.of(value -> value, value -> 1);

        flow.fork("everything", sub -> sub
                .when(value -> value > 10)
                    .then(lane -> lane
                            .filter(value -> value > 0)                     // guarded Filter
                            .background("effect", seen::add)                // guarded Background
                            .fanOut("split", branches,
                                    results -> results.get(0) + results.get(1))   // guarded FanOut
                            .batch("bulk", 1, Duration.ofMillis(50),
                                    values -> values.stream().map(value -> value + 1).toList())  // guarded Batch
                            .fork("nested", inner -> inner.handle("deep", value -> value))       // guarded Fork
                            .handle("boom", value -> {
                                throw new IllegalStateException("caught in-lane");
                            })
                            .recover("in-lane", error -> -1))               // guarded Recovery
                    .otherwise(lane -> lane.handle("small", value -> -999))
                .handle("done", value -> {
                    seen.add(value);
                    done.countDown();
                    return value;
                }));
        engine.seal();

        flow.just(42).execute();

        assertTrue(done.await(3, TimeUnit.SECONDS), () -> "the fork ran: " + seen);
        // 42 -> guarded lane -> fanOut(42, 1) = 43 -> batch +1 = 44 -> boom -> recover -> -1
        assertTrue(seen.contains(-1), () -> "every guarded link ran with its renumbered id: " + seen);
    }

    /** The no-op defaults of the metrics SPI: a sink that overrides nothing. */
    @Test
    void aMetricsSinkThatOverridesNothingStillWorks() {
        engine.metrics(new NioFlowMetrics() {
        });
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.fork("audit", sub -> sub.handle("persist", value -> value));
        // A failing fork too: the default forkFailed() must swallow it quietly.
        flow.fork("doomed", sub -> sub.handle("boom", value -> {
            throw new IllegalStateException("boom");
        }));
        flow.handle("main", value -> value + 1);

        assertEquals(2, flow.just(1).execute());
        assertEquals(0, engine.shutdown(Duration.ofSeconds(2)));
    }

    /** fork() reached through a branch object (the delegate plumbing). */
    @Test
    void forkChainedOnABranchIsMainLine() throws InterruptedException {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        CountDownLatch forks = new CountDownLatch(2);

        flow.when(value -> value > 10)
                .then(lane -> lane.handle("big", value -> value))
                // Chained ON the Branch: unguarded, so it forks for EVERY value.
                .fork(sub -> sub.handle("anon", value -> {
                    forks.countDown();
                    return value;
                }))
                .fork("named", sub -> sub.handle("named-fork", value -> {
                    forks.countDown();
                    return value;
                }));
        engine.seal();

        flow.just(3).execute();   // does not take the lane, still forks twice

        assertTrue(forks.await(3, TimeUnit.SECONDS), "both forks are on the main line");
    }

    /** fork() and with() reached through a per-request branch object. */
    @Test
    void forkAndWithChainedOnAStepBranch() throws InterruptedException {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        CountDownLatch forked = new CountDownLatch(2);
        Key<String> trace = Key.of("trace");

        Integer result = flow.just(3)
                .when(value -> value > 10)
                    .then(lane -> lane.handle("big", value -> value * 2))
                .fork(sub -> sub.handle("anon", value -> {
                    forked.countDown();
                    return value;
                }))
                .fork("named", sub -> sub.handle("named-fork", value -> {
                    forked.countDown();
                    return value;
                }))
                .with(trace, "abc")
                .handleContextual("read", (value, ctx) -> value + ctx.get(trace).length())
                .execute();

        assertEquals(6, result);
        assertTrue(forked.await(3, TimeUnit.SECONDS));
    }

    // Records a Fork link off-chain, the way the flow builder does, so the test
    // can hand it to splice().
    private List<Link> recordFork(String name, Segment<Integer, Integer> body) {
        List<Link> recorded = new ArrayList<>();
        new DefaultLane<>(new RecordingChain<Integer>(engine, recorded, prefix -> prefix + "-spliced"))
                .fork(name, body);
        return List.copyOf(recorded);
    }

    private static UnaryOperator<String> awaitOn(CountDownLatch latch) {
        return value -> {
            awaitQuietly(latch);
            return value;
        };
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("latch never released");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
