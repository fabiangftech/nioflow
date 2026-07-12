package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Branch;
import dev.nioflow.core.facade.Cases;
import dev.nioflow.core.facade.FlowResult;
import dev.nioflow.core.facade.NioStep;
import dev.nioflow.core.facade.Segment;
import dev.nioflow.core.facade.StepBranch;
import dev.nioflow.core.facade.StepCases;
import dev.nioflow.core.model.RateLimit;
import dev.nioflow.core.model.Retry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Chaining AFTER a fork is the main line: whatever you chain on a Branch or a
 * Cases lands on the flow the fork came from, unguarded, and runs for every
 * value — routed through a lane or not. These tests walk every step kind over
 * the fork objects (the NioFlowDelegate / NioStepDelegate plumbing).
 */
class DefaultNioFlowForkChainingTest {

    private static final Duration GENEROUS = Duration.ofSeconds(2);
    private static final Retry TWICE = Retry.of(2, Duration.ofMillis(1));

    /** Every step kind appended after a Branch, summing a distinct bit each. */
    @Test
    void everyStepKindChainedOnABranchRunsOnTheMainLine() {
        DefaultNioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);
        List<Integer> effects = new CopyOnWriteArrayList<>();
        List<Object> completions = new CopyOnWriteArrayList<>();

        Branch<Integer, Integer> branch = flow
                .when(value -> value > 10)
                .then(lane -> lane.handle("double", value -> value * 2));

        branch.handle(value -> value + 1);
        branch.handle("named", value -> value + 2);
        branch.handle("timed", value -> value + 4, GENEROUS);
        branch.handle("retried", value -> value + 8, TWICE);
        branch.handle("timedRetried", value -> value + 16, GENEROUS, TWICE);
        branch.handle("limited", value -> value + 32, RateLimit.perSecond(1_000));
        branch.handleContextual((value, context) -> value + 64);
        branch.handleContextual("contextual", (value, context) -> value + 128);
        branch.handleSync(value -> value + 256);
        branch.handleSync("sync", value -> value + 512);
        branch.background(effects::add);
        branch.background("effect", effects::add);
        branch.filter(value -> true);
        branch.recover(error -> -1);
        branch.recover("recovery", error -> -1);
        branch.onComplete(completions::add);
        branch.onError(error -> completions.add("error"));

        List<Function<Integer, Integer>> split = List.of(value -> value, value -> 0);
        branch.fanOut(split, parts -> parts.get(0) + parts.get(1));             // identity, joined from both branches
        branch.fanOut("fan", split, parts -> parts.get(0) + parts.get(1));
        branch.batch(1, Duration.ofMillis(50), values -> values);               // identity, size 1 flushes at once
        branch.batch("bulk", 1, Duration.ofMillis(50), values -> values);
        Segment<Integer, Integer> plusThousand = lane -> lane.handle(value -> value + 1024);
        branch.use(plusThousand);
        branch.use("region", lane -> lane.handle(value -> value + 2048));

        // 1+2+4+8+16+32+64+128+256+512 = 1023, plus the two segments = 4095.
        assertEquals(4100, branch.just(5).execute());        // 5, no lane
        assertEquals(4135, branch.just(20).execute());       // 20 doubled by the lane
        assertTrue(effects.containsAll(List.of(1028, 1063)), () -> "background effects ran: " + effects);
        assertEquals(List.of(4100, 4135), completions);
    }

    /** Same for the per-request side: the step kinds a StepBranch forwards. */
    @Test
    void everyStepKindChainedOnAStepBranchRunsOnTheExecution() {
        DefaultNioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);
        List<Integer> effects = new CopyOnWriteArrayList<>();
        List<Object> completions = new CopyOnWriteArrayList<>();

        StepBranch<Integer, Integer> branch = flow.just(20)
                .when(value -> value > 10)
                .then(lane -> lane.handle("double", value -> value * 2));

        branch.handle(value -> value + 1);
        branch.handle("named", value -> value + 2);
        branch.handle("timed", value -> value + 4, GENEROUS);
        branch.handle("retried", value -> value + 8, TWICE);
        branch.handle("timedRetried", value -> value + 16, GENEROUS, TWICE);
        branch.handle("limited", value -> value + 32, RateLimit.perSecond(1_000));
        branch.handleContextual((value, context) -> value + 64);
        branch.handleContextual("contextual", (value, context) -> value + 128);
        branch.handleSync(value -> value + 256);
        branch.handleSync("sync", value -> value + 512);
        branch.background(effects::add);
        branch.background("effect", effects::add);
        branch.filter(value -> true);
        branch.adapt(value -> value + 1024);
        branch.recover(error -> -1);
        branch.recover("recovery", error -> -1);
        branch.onComplete(completions::add);
        branch.onError(error -> completions.add("error"));
        branch.key("customer-1");

        List<Function<Integer, Integer>> split = List.of(value -> value, value -> 0);
        branch.fanOut(split, parts -> parts.get(0) + parts.get(1));
        branch.fanOut("fan", split, parts -> parts.get(0) + parts.get(1));
        branch.batch(1, Duration.ofMillis(50), values -> values);
        branch.batch("bulk", 1, Duration.ofMillis(50), values -> values);
        branch.use(lane -> lane.handle(value -> value + 2048));

        // 20 doubled = 40, + 1023 from the handles, + 1024 (adapt) + 2048 (segment).
        assertEquals(4135, branch.execute());
        assertEquals(List.of(4135), completions);
        assertTrue(effects.contains(1063), () -> "background effects ran: " + effects);
    }

    @Test
    void executeAsyncAndExecuteResultOnAStepBranchSeeTheChainedSteps() {
        DefaultNioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        StepBranch<Integer, Integer> async = flow.just(20)
                .when(value -> value > 10)
                .then(lane -> lane.handle(value -> value * 2));
        async.handle(value -> value + 1);
        assertEquals(41, async.executeAsync().join());

        StepBranch<Integer, Integer> result = flow.just(3)
                .when(value -> value > 10)
                .then(lane -> lane.handle(value -> value * 2));
        result.handle(value -> value + 1);
        assertEquals(new FlowResult.Completed<>(4), result.executeResult());
    }

    /** A fork nested on a fork object: when()/match() also forward to the main line. */
    @Test
    void forksChainedOnAForkAreThemselvesMainLine() {
        DefaultNioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        Branch<Integer, Integer> branch = flow
                .when(value -> value > 10)
                .then(lane -> lane.handle(value -> value * 2));
        branch.when(value -> value % 2 == 0)
                .then(lane -> lane.handle(value -> value + 1))
                .otherwise(lane -> lane.handle(value -> value + 1000));
        Cases<Integer, Integer> cases = branch.match();
        cases.is(value -> value > 100, lane -> lane.handle(value -> value * 10))
                .otherwise(lane -> lane.handle(value -> value - 5));

        assertEquals(10030, branch.just(3).execute());   // not doubled, odd: +1000 = 1003, > 100: * 10
        assertEquals(36, branch.just(20).execute());     // doubled: 40, even: +1 = 41, <= 100: -5
    }

    /** justAll on a fork object injects through the shared chain like the flow's own. */
    @Test
    void justAllOnAForkInjectsThroughTheSharedChain() {
        DefaultNioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        Branch<Integer, Integer> branch = flow
                .when(value -> value > 10)
                .then(lane -> lane.handle(value -> value * 2));
        branch.handle("tag", value -> value + 1);

        branch.justAll(List.of(20, 3));

        assertEquals(List.of(41, 4), List.of(flow.engine().await(), flow.engine().await()));
    }

    /** A Cases object forwards steps to the main line too, after the last case. */
    @Test
    void stepsChainedOnCasesRunForEveryValue() {
        DefaultNioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        Cases<Integer, Integer> cases = flow.match();
        cases.is(value -> value % 2 == 0, lane -> lane.handle(value -> value * 10));
        cases.is(value -> value % 3 == 0, lane -> lane.handle(value -> value * 100));
        cases.handle("common", value -> value + 1);

        assertEquals(41, cases.just(4).execute());     // even lane: 4 * 10 + 1
        assertEquals(901, cases.just(9).execute());    // odd, multiple of 3: 9 * 100 + 1
        assertEquals(8, cases.just(7).execute());      // no case: 7 + 1

        NioStep<Integer, Integer> step = cases.just(5);
        assertEquals(6, step.execute());
    }

    /** The per-request Cases: same contract on a just() pipeline. */
    @Test
    void stepsChainedOnStepCasesRunForEveryValue() {
        DefaultNioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        StepCases<Integer, Integer> cases = flow.just(4).match();
        cases.is(value -> value % 2 == 0, lane -> lane.handle(value -> value * 10));
        cases.handle("common", value -> value + 1);
        assertEquals(41, cases.execute());
    }

    /** A fork opened ON a StepBranch: when()/match() forward to the execution's main line. */
    @Test
    void forksChainedOnAStepBranchAreMainLineToo() {
        DefaultNioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        StepBranch<Integer, Integer> branch = flow.just(20)
                .when(value -> value > 10)
                .then(lane -> lane.handle(value -> value * 2));
        branch.when(value -> value % 2 == 0)
                .then(inner -> inner.handle(value -> value + 1))
                .otherwise(inner -> inner.handle(value -> value + 1000));
        branch.match()
                .is(value -> value > 100, inner -> inner.handle(value -> value * 10))
                .otherwise(inner -> inner.handle(value -> value - 5));

        assertEquals(36, branch.execute());   // 40 (lane), even: + 1 = 41, <= 100: - 5
    }
}
