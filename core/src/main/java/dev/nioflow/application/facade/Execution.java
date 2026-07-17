package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Context;
import dev.nioflow.core.facade.NioFlowMetrics;
import dev.nioflow.core.model.AsyncStage;
import dev.nioflow.core.model.Background;
import dev.nioflow.core.model.Batch;
import dev.nioflow.core.model.Decision;
import dev.nioflow.core.model.FanOut;
import dev.nioflow.core.model.Filter;
import dev.nioflow.core.model.Fork;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.Link;
import dev.nioflow.core.model.Recovery;
import dev.nioflow.core.model.Retry;
import dev.nioflow.core.model.Stage;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * One request's (or fork's) orchestration state and its event-loop driver.
 * Extracted from {@code DefaultNioEngine} by RFC 0032; it holds a reference back
 * to the engine it runs on (its shared executors, chain version, drain counter,
 * metrics and handlers) — every {@code engine.X} below was an implicit reference
 * to the enclosing instance before the extraction.
 */
final class Execution implements Runnable {

    private static final VarHandle LANE_HELD;

    static {
        try {
            LANE_HELD = MethodHandles.lookup().findVarHandle(Execution.class, "laneHeld", boolean.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // The engine this execution runs on: its shared executors, chain version,
    // drain counter, metrics and handlers. Before RFC 0032 this was the implicit
    // enclosing DefaultNioEngine.this; now every engine member is reached through
    // this field.
    private final DefaultNioEngine engine;
    final ExecutorService boss;
    private final List<Link> links;
    private final Object input;
    // Non-null = ordered lane: executions of this key run one at a time,
    // in submission order, all on the same boss.
    private final Object key;
    // True only while this execution HOLDS its key's lane: a keyed
    // execution that failed before enrolling (submission rejected at
    // shutdown) must not release a lane it never took. VOLATILE (RFC 0040):
    // the boss writes it (run()/releaseKey), but the off-boss shutdown
    // terminal reads it — cancel() runs complete(CANCELLED) on the CALLER's
    // thread when boss.execute is rejected, and the finished CAS orders the
    // racing terminals but carries no happens-before for this earlier boss
    // write. A stale false there would skip releaseKey() and hang the lane's
    // successors, so a clean drain would never complete. Off the hot path
    // (written once when a keyed execution takes its lane), so the volatile
    // costs nothing measurable. Released through LANE_HELD's CAS, which
    // elects a single releaser among the two that can reach releaseKey().
    private volatile boolean laneHeld;
    // Per-execution context: null until a contextual stage puts the first
    // entry (or the caller handed a map to call/inject). Stage
    // applications are serialized by the executor handoffs — one
    // continuation at a time, each hop a happens-before edge — so a plain
    // HashMap is enough. Only touched through ExecutionContext views.
    Map<String, Object> context;
    // Exactly-once completion guard, an atomic CAS (RFC 0024). Normally the
    // boss is the only writer, but shutdown of a dedicated engine has TWO
    // off-boss terminals that can race each other: a worker whose
    // resumeOnBoss is rejected (fail), and an outside cancel() whose
    // boss.execute is rejected (complete). A plain check-then-set would let
    // both win and run finishBookkeeping — and its activeExecutions.decrement
    // — twice, so a graceful drain could report clean while work still runs.
    // The CAS elects exactly one terminal; the loser returns before touching
    // the drain counter.
    private final AtomicBoolean finished = new AtomicBoolean();
    // Raised by cancel() from ANY thread, read only on the boss (between
    // links, before a recovery, before a retry) — so rule 1 holds: the flag
    // crosses threads, the orchestration state it guards does not.
    private volatile boolean cancelled;
    // The CompletionStage of the async stage currently in flight, if any:
    // the one piece of in-flight work cancellation can actually reach. Set
    // by the invoking worker, read by the timeout and by cancel(). A blocking
    // Stage has no equivalent — a parked worker is not a handle, which is
    // the whole reason cancellation waited for AsyncStage (RFC 0006).
    private volatile CompletionStage<Object> pendingCall;
    // Decisions as a bitset, 2 bits per id (bit 0 = recorded, bit 1 = value),
    // sized to the chain's highest Decision id: recording and guard checks
    // are O(1) with zero allocation, and an unrecorded decision fails any
    // guard on it — the property match() first-match-wins relies on. null
    // when the chain has no decisions, or when its ids outgrew the limit
    // (then decisionsOverflow takes over, lazily).
    private final long[] decisionBits;
    private Map<Integer, Boolean> decisionsOverflow;
    final CompletableFuture<Object> result = new CompletableFuture<>();
    // Precompiled dispatch plan for this exact chain version; null = interpret.
    private final CompiledChain plan;
    // Non-null = this execution IS a detached sub-flow (the link that
    // spawned it). It reports fork metrics instead of execution metrics and
    // never notifies the complete handlers: its terminal value is not the
    // flow's output. Failures still reach the error handlers.
    private final Fork forkOf;
    // Set true (by submit, before dispatch) only for a request/response
    // (call/callCancellable) execution that took an in-flight permit at
    // admission: finishBookkeeping releases it on whichever terminal fires
    // (value, FILTERED, CANCELLED, failure). An inject execution leaves this
    // false — its permit bounds the un-collected results queue and is released
    // by await(); a fork leaves it false too (it is spawned internally, never
    // admitted). Non-final, set-once before dispatch like laneHeld, so the
    // constructor stays within its parameter budget. See RFC 0031.
    boolean releasesPermit;
    // Snapshot of the installed metrics for this execution; null = untimed.
    private final NioFlowMetrics metrics;
    private final long startNanos;

    Execution(DefaultNioEngine engine, ExecutorService boss, List<Link> links,
              CompiledChain plan, Object input, Object key, Fork forkOf) {
        this.engine = engine;
        this.boss = boss;
        this.links = links;
        this.plan = plan;
        this.input = input;
        this.key = key;
        this.forkOf = forkOf;
        int maxDecision = plan != null ? plan.maxDecisionId() : CompiledChain.maxDecisionId(links);
        this.decisionBits = maxDecision >= 0 && maxDecision <= DefaultNioEngine.MAX_BITSET_DECISION_ID
                ? new long[(maxDecision >>> 5) + 1]
                : null;
        this.metrics = engine.metrics.get();
        this.startNanos = metrics != null ? System.nanoTime() : 0;
    }

    @Override
    public void run() {
        // Runs on this execution's boss. Keyed executions enroll in
        // their key's FIFO lane; only the lane's head advances.
        if (key == null) {
            advance(0, input);
            return;
        }
        DefaultNioEngine.KeyLane lane = engine.keyLanes.computeIfAbsent(key, ignored -> new DefaultNioEngine.KeyLane());
        if (lane.active) {
            lane.waiting.add(this);
            meterKeyLaneDepth(lane.waiting.size());   // head-of-line buildup, visible
        } else {
            lane.active = true;
            laneHeld = true;
            meterKeyLanesActive();                    // a new key started serializing
            advance(0, input);
        }
    }

    // Called exactly once from complete()/fail(): hand the lane to the next
    // same-key execution, or retire it. Normally on the key's boss; the only
    // off-boss caller is the shutdown-rejection path (a rejected resumeOnBoss
    // on a worker, or a rejected cancel on the caller), and then the boss is
    // gone. See releaseKey's body for how each case is handled without
    // recursion or a racy deque read (RFC 0026).
    private void releaseKey() {
        // Exactly one release per execution that took the lane. TWO callers can
        // reach here for the same execution: its own terminal (complete/fail,
        // which read laneHeld) and the hand-off below, when the successor it
        // hands to already reached a terminal while queued. Both can run at once
        // in the RFC 0040 shutdown window — a boss still drains queued tasks
        // while rejecting new ones — and a double release would hand ONE lane to
        // TWO successors: two heads running concurrently under the same key,
        // the one thing key() promises never happens. The CAS on the volatile
        // RFC 0040 already made cross-thread-visible elects the single releaser,
        // and costs no new field and no allocation.
        if (!LANE_HELD.compareAndSet(this, true, false)) {
            return;
        }
        DefaultNioEngine.KeyLane lane = engine.keyLanes.get(key);
        if (lane == null) {
            return;
        }
        Execution next = lane.waiting.poll();
        if (next == null) {
            engine.keyLanes.remove(key);
            meterKeyLanesActive();      // the key stopped serializing
            engine.signalKeyLaneVacancy();      // its backlog is empty: any BLOCK waiter may proceed
            return;
        }
        meterKeyLaneDepth(lane.waiting.size());   // backlog shrank by one
        engine.signalKeyLaneVacancy();                    // a slot freed for a BLOCK waiter
        // Hand the lane to the successor as a FRESH boss task, never an inline
        // call. A successor advancing inside its predecessor's terminal frame
        // recurses one stack frame per queued execution — the very thing the
        // iterative-advance invariant forbids — and off-boss (shutdown) that
        // frame is a worker's, whose stack the invariant never protected.
        // Posting also keeps it iterative in steady state for an all-inline
        // (handleSync) chain, whose successors would otherwise recurse on the
        // boss stack.
        try {
            boss.execute(() -> {
                next.laneHeld = true;
                // A successor CANCELLED WHILE QUEUED already ran its terminal:
                // it won the finished CAS back when laneHeld was still false, so
                // it released nothing (correctly — it held nothing), and its
                // complete() will never run again to release what we just handed
                // it. advance() would call complete(CANCELLED), lose the CAS and
                // return at its first line, leaving the lane active with no head
                // and every later execution on this key hanging forever. Hand the
                // lane straight on instead. Still iterative: each hand-off is a
                // fresh boss task, so a backlog of cancelled successors drains
                // one task at a time and never recurses on the boss stack.
                if (next.finished.get()) {
                    next.releaseKey();
                } else {
                    next.advance(0, next.input);
                }
            });
        } catch (RejectedExecutionException gone) {
            // The boss is gone: fail the whole remaining backlog iteratively.
            drainLaneOnShutdown(lane, next, gone);
        }
    }

    // Boss gone (dedicated-engine shutdown): fail the successor and every
    // execution still queued behind it in a LOOP, never recursively. None of
    // them is the lane head (laneHeld stays false, because the boss task that
    // would set it never ran), so their fail() does not re-enter releaseKey —
    // this method owns the drain. The ConcurrentLinkedQueue makes the poll
    // safe from this off-boss thread; the lane is retired at the end so
    // activeKeyLanes() reaches zero.
    private void drainLaneOnShutdown(DefaultNioEngine.KeyLane lane, Execution head, Throwable cause) {
        Execution current = head;
        while (current != null) {
            current.fail(cause);
            current = lane.waiting.poll();
        }
        engine.keyLanes.remove(key);
    }

    /**
     * Exactly-once terminal completion. Bookkeeping (drain slot, metrics,
     * handlers) runs BEFORE the future completes so a joining caller
     * always observes it done — same ordering the old whenComplete
     * wrapper gave, without allocating a dependent future per call.
     */
    private void complete(Object value) {
        if (!finished.compareAndSet(false, true)) {
            return;
        }
        finishBookkeeping(value, null);
        result.complete(value);
        if (laneHeld) {
            releaseKey();
        }
    }

    void fail(Throwable error) {
        if (!finished.compareAndSet(false, true)) {
            return;
        }
        finishBookkeeping(null, DefaultNioEngine.unwrap(error));
        result.completeExceptionally(error);
        if (laneHeld) {
            releaseKey();
        }
    }

    /**
     * Stops this execution from the outside — the only method on Execution
     * that runs on the CALLER's thread, and it is careful about it: it
     * raises a volatile flag, cancels the in-flight async call (a
     * CompletableFuture, safe from anywhere), and then hands the TERMINAL to
     * the boss, which is the thread allowed to touch orchestration state.
     *
     * <p>That last hop is not ceremony, it is the feature: an execution
     * parked on an async call that never answers has no resume coming, so
     * nobody would ever read the flag. Posting the terminal is what ends it.
     *
     * <p>Cooperative: a blocking stage already running on a worker is NOT
     * interrupted. It runs to its end and its result is dropped at the next
     * boundary, because by then the execution is finished.
     */
    void cancel() {
        if (finished.get() || cancelled) {
            return;
        }
        cancelled = true;
        // Kills the remote call itself so the subscription dies and the
        // connection is released. Off the caller's thread (RFC 0025), because
        // the teardown can block and a client-facing thread must not wear
        // that. The current pendingCall is snapshotted at the call site, so a
        // racing retry cannot send this cancel to the wrong call.
        cancelOffThread(pendingCall);
        try {
            boss.execute(() -> complete(DefaultNioEngine.CANCELLED));
        } catch (RejectedExecutionException gone) {
            // The boss is gone (engine shut down): nothing can race us.
            complete(DefaultNioEngine.CANCELLED);
        }
    }

    /**
     * The drain slot is released LAST, in a finally: "in flight" means
     * "not fully reported yet", so a clean shutdown(grace) guarantees every
     * metric and handler of every execution — request or fork — already
     * ran. For a request this still lands before its result future
     * completes, so a joining caller observes the same ordering as before.
     */
    private void finishBookkeeping(Object value, Throwable error) {
        try {
            if (forkOf != null) {
                reportFork(error);
            } else {
                reportExecution(value, error);
            }
        } finally {
            // A call-path permit frees on the terminal (RFC 0031): the
            // bounded resource is in-flight calls, so admission opens up the
            // moment this one is fully reported. Exactly-once via the
            // finished CAS that gates complete()/fail(). Before the drain
            // decrement so a freed slot never outlives the "in flight" count.
            if (releasesPermit) {
                engine.releasePermit();
            }
            // Released last: "in flight" means "not fully reported yet", so
            // a clean drain guarantees every metric and handler already ran.
            // awaitDrain observes the decrement by polling sum().
            engine.activeExecutions.decrement();
        }
    }

    /**
     * Every NioFlowMetrics call goes through a guard: a throwing sink is
     * reported through the error handlers, never allowed to hang a future,
     * skip an advance, or misroute a good value into recover(). Symmetric
     * with how completeHandlers and notifyError already treat user code —
     * the metrics SPI is user code too, and a hung future is the exact
     * failure mode the engine otherwise prevents religiously. See RFC 0023.
     */
    private void meter(Runnable sinkCall) {
        try {
            sinkCall.run();
        } catch (Throwable failure) {
            engine.notifyError(failure);
        }
    }

    // Key-lane gauges (RFC 0039), emitted on the boss as a lane fills or
    // drains. Guarded like every other SPI call; a null snapshot means no
    // metrics installed for this execution, so nothing is read or pushed.
    private void meterKeyLaneDepth(int depth) {
        if (metrics != null) {
            meter(() -> metrics.keyLaneDepth(depth));
        }
    }

    private void meterKeyLanesActive() {
        if (metrics != null) {
            int active = engine.keyLanes.size();
            meter(() -> metrics.keyLanesActive(active));
        }
    }

    // Pushes the terminal metric under whatever classification the outcome
    // earned; kept out of reportExecution so that method stays under the
    // cognitive-complexity limit (the meter() lambda is one call, not a
    // four-way branch nested inside the handler section).
    private void classifyExecution(Object value, Throwable error, long elapsed) {
        if (error != null) {
            metrics.executionFailed(error, elapsed);
        } else if (value == DefaultNioEngine.FILTERED) {
            metrics.executionFiltered(elapsed);
        } else if (value == DefaultNioEngine.CANCELLED) {
            metrics.executionCancelled(elapsed);
        } else {
            metrics.executionCompleted(elapsed);
        }
    }

    // The per-stage timing call, guarded WITHOUT a lambda so the hot stage
    // path allocates nothing extra for a metrics-enabled flow (a captured
    // Runnable per stage would show up in the allocation gate). Callers
    // already null-check metrics before reaching here.
    private void meterStageCompleted(String name, long elapsed) {
        try {
            metrics.stageCompleted(name, elapsed);
        } catch (Throwable failure) {
            engine.notifyError(failure);
        }
    }

    private void reportExecution(Object value, Throwable error) {
        if (metrics != null) {
            long elapsed = System.nanoTime() - startNanos;
            meter(() -> classifyExecution(value, error, elapsed));
        }
        // Hardened on purpose: this runs BEFORE the result future
        // completes, so a throwing handler must never escape — it would
        // leave the caller's future hanging forever. A failing complete
        // handler is reported through the error handlers instead.
        if (error != null) {
            engine.notifyError(error);
        } else if (value != DefaultNioEngine.CANCELLED) {
            // A cancelled execution reaches neither side: it produced no
            // output for the complete handlers (which promise an O) and it
            // is not a failure for the error handlers. Nobody was waiting
            // for it — that is what cancelled means.
            Object exposed = value == DefaultNioEngine.FILTERED ? null : value;
            for (Consumer<Object> handler : engine.completeHandlers) {
                try {
                    handler.accept(exposed);
                } catch (Throwable failure) {
                    engine.notifyError(failure);
                }
            }
        }
    }

    /**
     * A detached sub-flow ends here. It reports its own metrics — its
     * terminal value is not the flow's output, so it must not land in the
     * execution-latency histogram nor reach the complete handlers, which
     * promise an O. A failure it did not recover() is reported exactly
     * where a failing Background effect is: the error handlers.
     */
    private void reportFork(Throwable error) {
        int running = engine.activeForks.decrementAndGet();
        if (metrics != null) {
            long elapsed = System.nanoTime() - startNanos;
            meter(() -> {
                if (error != null) {
                    metrics.forkFailed(forkOf.name(), error, elapsed);
                } else {
                    metrics.forkCompleted(forkOf.name(), elapsed);
                }
                metrics.forksInFlight(running);
            });
        }
        if (error != null) {
            engine.notifyError(error);
        }
    }

    /**
     * Detaches the sub-flow: a child execution over the fork's own chain,
     * submitted as a NEW boss task so the main line resumes on the very
     * next instruction — running it inline would make the parent wait for
     * the fork's boss-inlined links (handleSync, when/match predicates),
     * which is exactly what a fork promises not to do.
     *
     * <p>The child gets a COPY of the context: parent and child run
     * concurrently on different threads, and the plain HashMap behind
     * Context is only safe because one execution's stages are serialized by
     * its executor handoffs. Sharing it would be a data race; the price is
     * that context writes inside a fork stay inside the fork.
     *
     * <p>Never keyed: inheriting the parent's key would queue the child
     * behind the very execution it was detached from.
     */
    private void spawnFork(Fork fork, Object value) {
        Execution child = new Execution(engine, boss, fork.chain(),
                plan == null ? null : plan.forkPlans().get(fork), value, null, fork);
        child.context = copyOfContext();   // set-once before dispatch (keeps the constructor lean)
        // Both counters BEFORE the submission: a fork is in-flight work, so
        // shutdown(grace) must wait for it even if it is still queued.
        engine.activeExecutions.increment();
        int running = engine.activeForks.incrementAndGet();
        if (child.metrics != null) {
            meter(() -> {
                child.metrics.forkStarted(fork.name());
                child.metrics.forksInFlight(running);
            });
        }
        try {
            boss.execute(child);
        } catch (RejectedExecutionException rejected) {
            // The boss is gone (engine-owned executor shut down mid-flight):
            // the child must still end, reporting through the error handlers.
            child.fail(rejected);
        }
    }

    private Map<String, Object> copyOfContext() {
        // Null stays null: a flow that never touched the context allocates
        // nothing for its forks either.
        return context == null ? null : new HashMap<>(context);
    }

    // Iterative, never recursive: a deep chain of cheap links is walked
    // entirely on the boss and must not depend on stack size.
    private void advance(int index, Object value) {
        Object current = value;
        while (index < links.size()) {
            // The cancellation check, at the one place where the chain is
            // between two links and no user code is running: one volatile
            // read per link, on the boss.
            if (cancelled) {
                complete(DefaultNioEngine.CANCELLED);
                return;
            }
            Link link = links.get(index);
            if (passesGuards(link)) {
                Object next;
                try {
                    next = step(link, index, current);
                } catch (Throwable error) {
                    // A throwing Decision/Filter predicate fails the value, never
                    // the boss task — otherwise the request future hangs forever.
                    recover(index + 1, error);
                    return;
                }
                if (next == DefaultNioEngine.HANDED_OFF) {
                    return;
                }
                current = next;
            }
            index++;
        }
        complete(current);
    }

    /**
     * Runs one link on the boss and returns the value the next link sees, or
     * HANDED_OFF when this link took the execution off the boss (worker
     * dispatch, fan-out, batch) or ended it (a Filter cut) — the caller must
     * then stop walking.
     */
    private Object step(Link link, int index, Object current) {
        switch (link) {
            case Stage stage -> {
                // Opt-in boss inline: a sync stage skips both thread hops.
                // timeout/retry force the dispatch path (they need a worker);
                // validation flags that combination.
                if (stage.sync() && stage.timeout() == null && stage.retry() == null) {
                    return timedApply(stage, current);
                }
                dispatch(index, current); // the worker resumes on the boss when done
                return DefaultNioEngine.HANDED_OFF;
            }
            case AsyncStage async -> {
                // A worker INVOKES the call and is released; the boss resumes
                // when the CompletionStage completes. Nothing parks. A run of
                // consecutive unguarded async stages is driven from the worker
                // side (RFC 0013): the boss is touched once, not per stage.
                AsyncStage[] run = plan != null ? plan.asyncRuns()[index] : null;
                if (run != null) {
                    new AsyncRun(run, index).begin(current);
                } else {
                    attemptAsync(async, index + 1, current, 1);
                }
                return DefaultNioEngine.HANDED_OFF;
            }
            case Decision decision -> recordDecision(decision.id(), decision.predicate().test(current));
            case Filter filter -> {
                if (!filter.predicate().test(current)) {
                    complete(DefaultNioEngine.FILTERED);
                    return DefaultNioEngine.HANDED_OFF;
                }
            }
            case Background background -> {
                Object snapshot = current;
                engine.workersExecutorService.execute(() -> runBackground(background, snapshot));
            }
            // Detaches a child execution and keeps walking with the SAME
            // value: the main line never waits for a fork.
            case Fork fork -> spawnFork(fork, current);
            case FanOut fanOut -> {
                // the join resumes on the boss when all branches finish
                dispatchFanOut(fanOut, index + 1, current);
                return DefaultNioEngine.HANDED_OFF;
            }
            case Batch batch -> {
                // Parks this execution in the link's shared group; the flush
                // (size or window) resumes it on ITS boss with its own element
                // of the bulk result.
                joinBatch(batch, index + 1, current);
                return DefaultNioEngine.HANDED_OFF;
            }
            case Recovery ignored -> {
                // Only applies on the error path (see recover)
            }
        }
        return current;
    }

    /**
     * Stage fusion: starting at index, take the maximal run of consecutive
     * no-timeout Stages, Filters and Recoveries (guard-skipped links inside
     * the run are stepped over — decisions cannot change until the next
     * passing Decision, which ends the run). The whole run travels
     * boss→worker→boss as ONE composed function: 2 thread hops per run
     * instead of 2 per link. Fused Filter predicates run on the worker; a
     * rejection returns the FILTERED sentinel and completes the flow with
     * null, same as a boss-side cut. Fused Recoveries preserve positional
     * semantics inside the run: a failure looks forward for the next
     * Recovery in the run and continues from there; with none left, the
     * failure escapes the run and recover(resume) scans the rest of the
     * chain — equivalent, because everything between the failure and the
     * run's end has already been searched.
     */
    private void dispatch(int index, Object value) {
        Stage first = (Stage) links.get(index);
        if (first.timeout() != null) {
            dispatchWithTimeout(first, index + 1, value);
            return;
        }
        Link[] run;
        int resume;
        if (plan != null && plan.runs()[index] != null) {
            // Precompiled unguarded run: zero scanning, zero allocation.
            run = plan.runs()[index];
            resume = plan.runEnds()[index];
        } else {
            // Interpreted scan (no plan, or the window contains guarded links
            // whose selection depends on this execution's decisions), bounded
            // to the precompiled window when one exists.
            int limit = plan != null ? plan.runEnds()[index] : links.size();
            List<Link> selected = null;
            int next = index + 1;
            while (next < limit) {
                Link link = links.get(next);
                if (!passesGuards(link)) {
                    next++;
                    continue;
                }
                boolean fusable = link instanceof Filter
                        || link instanceof Recovery
                        || (link instanceof Stage stage && stage.timeout() == null);
                if (!fusable) {
                    break;
                }
                if (selected == null) {
                    selected = new ArrayList<>();
                    selected.add(first);
                }
                selected.add(link);
                next++;
            }
            resume = next;
            run = selected == null ? null : selected.toArray(Link[]::new);
        }
        // Manual boss→worker→boss handoff: the CompletableFuture machinery
        // (async task, dependent future, composition nodes) buys nothing
        // here — no composition, no timeout — and costs several
        // allocations per dispatch. Two plain closures do the round trip.
        Link[] selectedRun = run;
        int resumeAt = resume;
        engine.workersExecutorService.execute(() -> {
            Object outcome;
            try {
                outcome = selectedRun == null || selectedRun.length == 1
                        ? applyStage(first, value)
                        : applyRun(selectedRun, value);
            } catch (Throwable error) {
                resumeOnBoss(() -> recover(resumeAt, DefaultNioEngine.unwrap(error)));
                return;
            }
            Object nextValue = outcome;
            resumeOnBoss(() -> {
                if (nextValue == DefaultNioEngine.FILTERED) {
                    complete(DefaultNioEngine.FILTERED);
                } else {
                    advance(resumeAt, nextValue);
                }
            });
        });
    }

    // Orchestration may only resume on the boss. A rejection means the
    // engine-owned boss executor was shut down mid-flight: the execution
    // can only end exceptionally (never silently, never on this thread).
    private void resumeOnBoss(Runnable continuation) {
        try {
            boss.execute(continuation);
        } catch (RejectedExecutionException rejected) {
            fail(rejected);
        }
    }

    /**
     * The fused run, applied on the worker as one plain call chain.
     *
     * <p>Cancellation is checked HERE too, and it has to be: a fused run is
     * several links with no boss boundary between them, so the check in
     * advance() would not fire until the whole run had finished — and the
     * most ordinary chain there is (handle → handle → charge) is exactly one
     * fused run. Without this, "the card is not charged" would be false in
     * the common case, which is the entire feature.
     *
     * <p>Same cooperative rule as everywhere: the stage already running is
     * not interrupted. The one after it is simply never invoked, and the run
     * hands the sentinel back to the boss.
     */
    private Object applyRun(Link[] run, Object value) {
        Object current = value;
        for (int i = 0; i < run.length; i++) {
            if (cancelled) {
                return DefaultNioEngine.CANCELLED;
            }
            try {
                if (run[i] instanceof Stage stage) {
                    current = applyStage(stage, current);
                } else if (run[i] instanceof Filter filter && !filter.predicate().test(current)) {
                    return DefaultNioEngine.FILTERED;
                }
                // Recovery: skipped on the happy path
            } catch (Throwable error) {
                // Positional semantics inside the run: look forward for the
                // next Recovery; a throwing recovery keeps scanning with the
                // new failure; with none left, escape the run (the boss then
                // scans the rest of the chain from the run's end).
                //
                // Unwrapped, exactly as the dispatched path does before it
                // calls recover(): what a recovery sees must not depend on
                // whether it happened to fuse with the stage that failed.
                Throwable pending = DefaultNioEngine.unwrap(error);
                int next = i + 1;
                boolean recovered = false;
                while (next < run.length) {
                    if (run[next] instanceof Recovery recovery) {
                        try {
                            current = recovery.function().apply(pending);
                            if (metrics != null) {
                                meter(() -> metrics.recoveryApplied(recovery.name()));
                            }
                            recovered = true;
                            break;
                        } catch (Throwable failure) {
                            pending = DefaultNioEngine.unwrap(failure);
                        }
                    }
                    next++;
                }
                if (!recovered) {
                    throw new CompletionException(pending);
                }
                i = next;
            }
        }
        return current;
    }

    // Hands this execution to the batch group and stops advancing; the
    // group's flush calls the continuation, which hops back to THIS
    // execution's boss to resume (or recover) — affinity preserved.
    private void joinBatch(Batch batch, int resume, Object value) {
        engine.batchGroupFor(batch)
                .add(value, (element, error) -> resumeOnBoss(() -> {
                    if (error != null) {
                        recover(resume, error);
                    } else {
                        advance(resume, element);
                    }
                }));
    }

    /**
     * Parallel split-join, as a countdown instead of a CompletableFuture
     * tree: one result slot per branch (written only by that branch — no
     * sharing), an {@link AtomicInteger} the branches count down, and a
     * first-failure {@link AtomicReference} the branches CAS. The branch
     * that decrements the counter to zero runs {@code join} and hops to the
     * boss once — no {@code allOf} tree, no dependent futures, and no
     * dedicated virtual thread for the join.
     *
     * <p>The counter's read-modify-write is the memory barrier: each branch
     * writes its slot (and, on failure, CASes the throwable) BEFORE it
     * decrements, so the thread that observes zero sees every slot and the
     * winning failure. A failing branch skips the join and takes the
     * recovery path.
     */
    private void dispatchFanOut(FanOut fanOut, int resume, Object value) {
        new FanOutJoin(fanOut, resume, value).dispatch();
    }

    /**
     * The shared state of one in-flight fan-out: a result slot per branch
     * (each written only by its branch), the countdown, and the first-failure
     * winner. The counter's read-modify-write is the memory barrier — every
     * branch fills its slot (or CASes the throwable) BEFORE it counts down, so
     * the branch that reads zero sees every slot and the winning failure.
     */
    private final class FanOutJoin {

        private final FanOut fanOut;
        private final List<Function<Object, Object>> branches;
        private final int resume;
        private final Object value;
        private final Object[] results;
        private final AtomicInteger remaining;
        private final AtomicReference<Throwable> failure = new AtomicReference<>();

        private FanOutJoin(FanOut fanOut, int resume, Object value) {
            this.fanOut = fanOut;
            this.branches = fanOut.branches();
            this.resume = resume;
            this.value = value;
            this.results = new Object[branches.size()];
            this.remaining = new AtomicInteger(branches.size());
        }

        /** Submit every branch to a worker; a rejected submission counts down exceptionally. */
        private void dispatch() {
            boolean async = fanOut.async();
            for (int i = 0; i < branches.size(); i++) {
                int slot = i;
                Runnable task = async ? () -> invokeAsyncBranch(slot) : () -> runBranch(slot);
                try {
                    engine.workersExecutorService.execute(task);
                } catch (RejectedExecutionException rejected) {
                    // Workers gone mid-shutdown: this branch cannot run, but
                    // the fan-out must still end. Record the failure and count
                    // down so the last branch out finishes (exceptionally).
                    fail(rejected);
                    branchDone();
                }
            }
        }

        /** One sync branch on its worker: fill the slot (or CAS the failure), then count down. */
        private void runBranch(int slot) {
            try {
                succeed(slot, branches.get(slot).apply(value));
            } catch (Throwable error) {
                fail(error);
            }
            branchDone();
        }

        /**
         * One async branch: the worker INVOKES the branch (building the stage
         * is user code) and is released; the countdown fires when the stage
         * completes, on whatever thread completes it — no parked worker.
         */
        @SuppressWarnings("unchecked")
        private void invokeAsyncBranch(int slot) {
            CompletionStage<Object> stage;
            try {
                stage = (CompletionStage<Object>) branches.get(slot).apply(value);
                if (stage == null) {
                    throw new IllegalStateException("Async fan-out '" + fanOut.name()
                            + "' branch returned a null CompletionStage");
                }
            } catch (Throwable error) {
                fail(error);
                branchDoneAsync();
                return;
            }
            stage.whenComplete((outcome, error) -> {
                if (error != null) {
                    fail(error);
                } else {
                    succeed(slot, outcome);
                }
                branchDoneAsync();
            });
        }

        private void succeed(int slot, Object outcome) {
            results[slot] = outcome;
        }

        private void fail(Throwable error) {
            failure.compareAndSet(null, DefaultNioEngine.unwrap(error));
        }

        /** Sync branch done, on its worker: the last one runs the join here. */
        private void branchDone() {
            if (remaining.decrementAndGet() == 0) {
                finish();
            }
        }

        /**
         * Async branch done, on the completion thread: the last one dispatches
         * the join to a worker so it never runs on a foreign loop; if the
         * workers are gone, it finishes inline (exceptionally) rather than
         * leaving the execution hung.
         */
        private void branchDoneAsync() {
            if (remaining.decrementAndGet() != 0) {
                return;
            }
            try {
                engine.workersExecutorService.execute(this::finish);
            } catch (RejectedExecutionException rejected) {
                fail(rejected);
                finish();
            }
        }

        /**
         * The terminal, on the last branch's worker: a failure skips the join
         * and takes the recovery path; otherwise the join combines the slots
         * in declaration order and the value resumes on the boss. Either way,
         * exactly one hop to the boss.
         */
        private void finish() {
            Throwable error = failure.get();
            if (error != null) {
                resumeOnBoss(() -> recover(resume, error));
                return;
            }
            Object combined;
            try {
                combined = fanOut.join().apply(Arrays.asList(results));
            } catch (Throwable joinError) {
                resumeOnBoss(() -> recover(resume, DefaultNioEngine.unwrap(joinError)));
                return;
            }
            resumeOnBoss(() -> advance(resume, combined));
        }
    }

    private void dispatchWithTimeout(Stage stage, int resume, Object value) {
        attemptWithTimeout(stage, resume, value, 1);
    }

    /**
     * Timeout + retry composition: the budget applies to EACH attempt (a
     * hung attempt is cut externally, which an inline loop could not do),
     * the backoff is scheduled without parking anyone, and once attempts
     * are exhausted the last failure flows to the recovery path.
     *
     * The budget is armed on the shared TimerWheel — O(1) schedule and
     * cancel — instead of orTimeout's lock-guarded priority queue; the
     * attempt future's internal CAS arbitrates the completion/timeout
     * race exactly as before. Retry backoff keeps delayedExecutor: it
     * only runs after a failure (cold) and a sub-tick backoff would
     * degrade to wheel granularity.
     */
    private void attemptWithTimeout(Stage stage, int resume, Object value, int attempt) {
        CompletableFuture<Object> attemptResult = new CompletableFuture<>();
        engine.workersExecutorService.execute(() -> {
            try {
                attemptResult.complete(timedApply(stage, value));
            } catch (Throwable error) {
                attemptResult.completeExceptionally(error);
            }
        });
        TimerWheel.Timeout budget = TimerWheel.shared().schedule(stage.timeout().toNanos(),
                () -> attemptResult.completeExceptionally(
                        new TimeoutException("Stage '" + stage.name() + "' exceeded " + stage.timeout())));
        attemptResult.whenCompleteAsync((nextValue, error) -> {
            budget.cancel();
            if (error == null) {
                advance(resume, nextValue);
                return;
            }
            Retry retry = stage.retry();
            if (retry != null && attempt < retry.attempts()) {
                if (metrics != null) {
                    meter(() -> metrics.stageRetried(stage.name()));
                }
                CompletableFuture.delayedExecutor(retry.delayNanos(attempt), TimeUnit.NANOSECONDS, boss)
                        .execute(() -> attemptWithTimeout(stage, resume, value, attempt + 1));
            } else {
                recover(resume, DefaultNioEngine.unwrap(error));
            }
        }, boss);
    }

    /**
     * One attempt of an AsyncStage: the worker invokes the call and goes
     * back to the pool — the whole point of the link. What waits is a
     * CompletableFuture, not a thread.
     *
     * <p>The timeout does what a Stage's cannot: on expiry it <b>cancels</b>
     * the CompletionStage (a Mono's subscription dies with its future; an
     * HttpClient exchange aborts), so the remote call stops instead of
     * merely being ignored. The cancel only happens if the timeout WON the
     * race — the attempt future's CAS arbitrates, exactly as for Stage.
     *
     * <p>Retry re-invokes on a worker after a backoff scheduled on the boss's
     * delayedExecutor: there is no parked worker to park a little longer, so
     * the inline LockSupport loop that Stage uses has nothing to loop on.
     */
    private void attemptAsync(AsyncStage stage, int resume, Object value, int attempt) {
        // Single async stage: the completion settles on the BOSS, which then
        // advances/recovers the rest of the chain.
        attemptAsyncCall(stage, value, boss,
                (invokedAt, nextValue, error) ->
                        settleAsync(stage, resume, value, attempt, invokedAt, nextValue, error));
    }

    /** What a settled async attempt does with its outcome; the invocation time rides along for the metric. */
    @FunctionalInterface
    private interface AsyncSettle {
        void settle(long invokedAt, Object nextValue, Throwable error);
    }

    /**
     * One async attempt, shared by the single-stage path and the fused
     * async-run driver: a worker invokes the call and is released, a
     * TimerWheel budget arms the per-attempt timeout (cancelling the call on
     * expiry), and the completion settles on the given executor — the BOSS
     * for a lone stage, a WORKER for a run (so the next stage's invocation
     * never runs on the completing thread and never on the boss).
     */
    private void attemptAsyncCall(AsyncStage stage, Object value, java.util.concurrent.Executor settleOn,
                                  AsyncSettle onSettled) {
        CompletableFuture<Object> attemptResult = new CompletableFuture<>();
        long invokedAt = metrics != null ? System.nanoTime() : 0;

        try {
            engine.workersExecutorService.execute(() -> invokeAsync(stage, value, attemptResult));
        } catch (RejectedExecutionException rejected) {
            // Workers gone mid-shutdown: the execution must still end.
            attemptResult.completeExceptionally(rejected);
        }

        // The in-flight call is published by the worker into pendingCall and
        // read from two places that may run on any thread: this timeout, and
        // an outside cancel(). Only one of them wins the attempt future, and
        // only the winner cancels — attemptResult's CAS is the arbiter.
        // The winner completes the future INLINE (cheap, keeps the wheel
        // ticking) but hands the subscription-cancel to a worker (RFC 0025):
        // the teardown must not run on the shared timer thread.
        TimerWheel.Timeout budget = stage.timeout() == null ? null
                : TimerWheel.shared().schedule(stage.timeout().toNanos(), () -> {
                    boolean expired = attemptResult.completeExceptionally(new TimeoutException(
                            DefaultNioEngine.ASYNC_STAGE + stage.name() + "' exceeded " + stage.timeout()));
                    if (expired) {
                        cancelOffThread(pendingCall);
                    }
                });

        attemptResult.whenCompleteAsync((nextValue, error) -> {
            if (budget != null) {
                budget.cancel();
            }
            onSettled.settle(invokedAt, nextValue, error);
        }, settleOn);
    }

    /** On the boss: continue, retry, or give the failure to the recovery path. */
    private void settleAsync(AsyncStage stage, int resume, Object value, int attempt,
                             long invokedAt, Object nextValue, Throwable error) {
        // Checked here and not only in advance()/recover(): the retry branch
        // below reaches neither, and a cancelled execution must not fire the
        // remote call one more time.
        if (cancelled) {
            complete(DefaultNioEngine.CANCELLED);
            return;
        }
        if (error == null) {
            if (metrics != null) {
                meterStageCompleted(stage.name(), System.nanoTime() - invokedAt);
            }
            advance(resume, nextValue);
            return;
        }
        Retry retry = stage.retry();
        if (retry == null || attempt >= retry.attempts()) {
            recover(resume, DefaultNioEngine.unwrap(error));
            return;
        }
        if (metrics != null) {
            meter(() -> metrics.stageRetried(stage.name()));
        }
        CompletableFuture.delayedExecutor(retry.delayNanos(attempt), TimeUnit.NANOSECONDS, boss)
                .execute(() -> attemptAsync(stage, resume, value, attempt + 1));
    }

    /**
     * The worker-side driver for a fused run of consecutive AsyncStages
     * (RFC 0013): it runs beside the boss rather than on it, invoking each
     * stage on a worker and continuing on a worker when the stage completes —
     * so the run costs one worker hop per stage instead of two boss hops, and
     * reaches the boss only once (to advance past the run, recover a failure,
     * or complete a cancellation).
     *
     * <p>It is a SECOND execution driver, so it holds the same invariants the
     * boss loop does: {@code cancelled} is read between stages (exactly as
     * {@code applyRun} reads it between fused blocking stages — a fused async
     * run is several links with no boss boundary between them), user code runs
     * only on workers (never the boss, never the completing thread), and its
     * per-stage timeout/retry/metric ride the shared {@link #attemptAsyncCall}.
     * A failing stage recovers from the chain index right after it, exactly as
     * the single-stage path does — the run is an optimization, never a
     * semantic.
     */
    private final class AsyncRun {

        private final AsyncStage[] stages;
        // Chain index of stages[0]; stage at run position p is at start + p,
        // and the link after the whole run is start + stages.length.
        private final int start;

        private AsyncRun(AsyncStage[] stages, int start) {
            this.stages = stages;
            this.start = start;
        }

        /** Started on the boss: hop to a worker once, then drive the whole run there. */
        private void begin(Object value) {
            try {
                engine.workersExecutorService.execute(() -> drive(0, value));
            } catch (RejectedExecutionException rejected) {
                fail(rejected);
            }
        }

        /**
         * The trampoline, on a worker: invoke each stage inline and — when its
         * CompletionStage is ALREADY resolved (a {@code Mono.just}, a completed
         * future) — loop straight to the next without a thread hop, so a run of
         * resolved stages costs one worker run like a fused blocking run does.
         * A stage still pending registers a callback and stops the loop; that
         * callback hops back to a worker to resume (never the completing
         * thread, never the boss). Cancellation is read between stages, exactly
         * as {@code applyRun} reads it between fused blocking stages.
         */
        private void drive(int pos, Object value) {
            Object current = value;
            int i = pos;
            while (i < stages.length) {
                if (cancelled) {
                    resumeOnBoss(() -> complete(DefaultNioEngine.CANCELLED));
                    return;
                }
                AsyncStage stage = stages[i];
                if (stage.timeout() != null || stage.retry() != null) {
                    // Needs the per-attempt timer/backoff machinery — it cannot
                    // run inline. It settles on a worker and resumes the run.
                    attempt(i, current, 1);
                    return;
                }
                Object next = inlineStage(stage, i, current);
                if (next == DefaultNioEngine.HANDED_OFF) {
                    // Pending (a callback will resume) or terminal (recover /
                    // cancel already hopped to the boss): the loop is done.
                    return;
                }
                current = next;
                i++;
            }
            Object finalValue = current;
            resumeOnBoss(() -> advance(start + stages.length, finalValue));
        }

        /**
         * Invoke one no-budget stage on this worker. Returns the next value if
         * its CompletionStage resolved synchronously (loop on), or
         * {@code HANDED_OFF} if it is still pending (a callback will resume the
         * run) or the execution already ended (a recover/cancel hop to the boss).
         */
        private Object inlineStage(AsyncStage stage, int pos, Object value) {
            long invokedAt = metrics != null ? System.nanoTime() : 0;
            CompletionStage<Object> call;
            try {
                call = stage.call().apply(value);
                if (call == null) {
                    throw new IllegalStateException(DefaultNioEngine.ASYNC_STAGE + stage.name() + "' returned a null CompletionStage");
                }
            } catch (Throwable error) {
                resumeOnBoss(() -> recover(start + pos + 1, DefaultNioEngine.unwrap(error)));
                return DefaultNioEngine.HANDED_OFF;
            }
            // Same Dekker handshake as invokeAsync: publish, then re-read
            // cancelled, so an in-flight call is never left running.
            pendingCall = call;
            if (cancelled) {
                cancel(call);
                resumeOnBoss(() -> complete(DefaultNioEngine.CANCELLED));
                return DefaultNioEngine.HANDED_OFF;
            }
            CompletableFuture<Object> future = call.toCompletableFuture();
            if (!future.isDone()) {
                // Genuinely async: resume on a worker when it completes.
                call.whenComplete((outcome, error) -> onPending(stage, pos, invokedAt, outcome, error));
                return DefaultNioEngine.HANDED_OFF;
            }
            Object outcome;
            try {
                outcome = future.join();
            } catch (Throwable error) {
                resumeOnBoss(() -> recover(start + pos + 1, DefaultNioEngine.unwrap(error)));
                return DefaultNioEngine.HANDED_OFF;
            }
            if (metrics != null) {
                meterStageCompleted(stage.name(), System.nanoTime() - invokedAt);
            }
            return outcome;
        }

        /** A pending stage completed on a foreign thread: settle off it, resume the run on a worker. */
        private void onPending(AsyncStage stage, int pos, long invokedAt, Object outcome, Throwable error) {
            if (cancelled) {
                resumeOnBoss(() -> complete(DefaultNioEngine.CANCELLED));
                return;
            }
            if (error != null) {
                resumeOnBoss(() -> recover(start + pos + 1, DefaultNioEngine.unwrap(error)));
                return;
            }
            if (metrics != null) {
                meterStageCompleted(stage.name(), System.nanoTime() - invokedAt);
            }
            try {
                engine.workersExecutorService.execute(() -> drive(pos + 1, outcome));
            } catch (RejectedExecutionException rejected) {
                fail(rejected);
            }
        }

        /** A timeout/retry stage inside the run, on the shared per-attempt machinery. */
        private void attempt(int pos, Object value, int attempt) {
            attemptAsyncCall(stages[pos], value, engine.workersExecutorService,
                    (invokedAt, nextValue, error) -> settle(pos, value, attempt, invokedAt, nextValue, error));
        }

        /** A budgeted stage settled on a WORKER: resume the run inline, retry, or recover. */
        private void settle(int pos, Object value, int attempt, long invokedAt, Object nextValue, Throwable error) {
            if (cancelled) {
                resumeOnBoss(() -> complete(DefaultNioEngine.CANCELLED));
                return;
            }
            AsyncStage stage = stages[pos];
            if (error == null) {
                if (metrics != null) {
                    meterStageCompleted(stage.name(), System.nanoTime() - invokedAt);
                }
                drive(pos + 1, nextValue);   // already on a worker
                return;
            }
            Retry retry = stage.retry();
            if (retry == null || attempt >= retry.attempts()) {
                resumeOnBoss(() -> recover(start + pos + 1, DefaultNioEngine.unwrap(error)));
                return;
            }
            if (metrics != null) {
                meter(() -> metrics.stageRetried(stage.name()));
            }
            // Backoff on delayedExecutor (cold path), re-invoke on a worker:
            // there is no parked worker to park a little longer, and the
            // re-invocation is user code that must stay off the boss.
            CompletableFuture.delayedExecutor(retry.delayNanos(attempt), TimeUnit.NANOSECONDS,
                            engine.workersExecutorService)
                    .execute(() -> attempt(pos, value, attempt + 1));
        }
    }

    /**
     * The worker's whole job: apply the function, publish the stage it
     * returned, hook the completion onto the attempt future — microseconds,
     * then the thread is free. The callback that arms it runs on whatever
     * thread completes the call (Netty, an HttpClient selector) and does no
     * user code: it only completes a future, and the boss picks it up from
     * there.
     */
    private void invokeAsync(AsyncStage stage, Object value,
                             CompletableFuture<Object> attemptResult) {
        CompletionStage<Object> call;
        try {
            call = stage.call().apply(value);
            if (call == null) {
                throw new IllegalStateException(
                        DefaultNioEngine.ASYNC_STAGE + stage.name() + "' returned a null CompletionStage");
            }
        } catch (Throwable error) {
            // A synchronous throw from the call is an ordinary stage failure.
            attemptResult.completeExceptionally(error);
            return;
        }
        pendingCall = call;
        // Both readers of pendingCall may have run BEFORE the line above
        // published it, and then they cancelled nothing: the timeout (which
        // completed the attempt future) and an outside cancel() (which raised
        // the flag). Re-check both here, so the call never outlives the
        // attempt that owns it.
        //
        // Publishing then re-reading two volatiles that the other side writes
        // then reads in the opposite order is what makes this airtight: one of
        // the two sides always sees the other, so the call is cancelled once.
        if (attemptResult.isDone() || cancelled) {
            cancel(call);
            return;
        }
        call.whenComplete((outcome, error) -> {
            if (error != null) {
                attemptResult.completeExceptionally(error);
            } else {
                attemptResult.complete(outcome);
            }
        });
    }

    /**
     * Cancellation, best effort and honest about it: a CompletableFuture
     * (which is what {@code mono.toFuture()} and {@code HttpClient.sendAsync}
     * hand back) cancels the work behind it; a minimal CompletionStage
     * implementation may refuse to produce one, and then the timeout is all
     * we have — the failure still reaches recover(), only the remote call
     * keeps running.
     */
    private static void cancel(CompletionStage<Object> call) {
        if (call == null) {
            return;
        }
        try {
            call.toCompletableFuture().cancel(false);
        } catch (UnsupportedOperationException notCancellable) {
            // Nothing to take back; the execution has already failed.
        }
    }

    /**
     * Cancels the in-flight async call on a WORKER, never on the thread that
     * asked for it. Cancelling a {@code mono.toFuture()} disposes the Reactor
     * subscription, which can run reactor-netty's connection teardown — not
     * cheap, and not something the two threads that reach here may run: the
     * shared {@link TimerWheel} thread (it must keep ticking for every other
     * timeout and batch window in the JVM) and an outside {@code cancel()}
     * caller's thread. The caller passes the CURRENT {@code pendingCall}
     * value, so a retry that later republishes the field cannot redirect this
     * cancel to the wrong call. Workers gone mid-shutdown: cancel inline,
     * there is nothing left to protect. See RFC 0025.
     */
    private void cancelOffThread(CompletionStage<Object> call) {
        if (call == null) {
            return;
        }
        try {
            engine.workersExecutorService.execute(() -> cancel(call));
        } catch (RejectedExecutionException gone) {
            cancel(call);
        }
    }

    /**
     * Runs the stage on a worker honoring its retry policy: failed attempts
     * back off by parking the (virtual) worker thread — cheap — and the
     * last failure escapes to the caller's error path (in-run recovery or
     * recover()). Works unchanged inside fused runs: retrying never breaks
     * fusion. Timeout+retry stages do NOT go through here (a hung attempt
     * cannot be cut inline; see attemptWithTimeout).
     */
    private Object applyStage(Stage stage, Object value) {
        Retry retry = stage.retry();
        if (retry == null) {
            return timedApply(stage, value);
        }
        Throwable last = null;
        for (int attempt = 1; attempt <= retry.attempts(); attempt++) {
            if (attempt > 1) {
                if (metrics != null) {
                    meter(() -> metrics.stageRetried(stage.name()));
                }
                LockSupport.parkNanos(retry.delayNanos(attempt - 1));
            }
            try {
                return timedApply(stage, value);
            } catch (Throwable error) {
                last = error;
            }
        }
        throw new CompletionException(last);
    }

    // Runs the stage function, timing it when metrics are installed.
    // Called on a worker thread — except for boss-inlined sync stages.
    private Object timedApply(Stage stage, Object value) {
        if (metrics == null) {
            return invoke(stage, value);
        }
        long start = System.nanoTime();
        Object next = invoke(stage, value);
        meterStageCompleted(stage.name(), System.nanoTime() - start);
        return next;
    }

    // The single point where stage functions run: contextual stages get
    // the execution's Context here; plain stages pay one instanceof. The
    // view is stateless (it reads/writes the execution's map), so it is
    // allocated per contextual application instead of adding a field
    // that every context-free execution would pay for.
    private Object invoke(Stage stage, Object value) {
        return stage.function() instanceof ContextualFunction(var body)
                ? body.apply(value, new ExecutionContext())
                : stage.function().apply(value);
    }

    private final class ExecutionContext implements Context {

        @Override
        @SuppressWarnings("unchecked")
        public <T> T get(Key<T> key) {
            Map<String, Object> map = context;
            return map == null ? null : (T) map.get(key.name());
        }

        @Override
        public <T> T getOrDefault(Key<T> key, T fallback) {
            T value = get(key);
            return value == null ? fallback : value;
        }

        @Override
        public <T> Context put(Key<T> key, T value) {
            if (context == null) {
                context = new HashMap<>();
            }
            context.put(key.name(), value);
            return this;
        }
    }

    private void recover(int from, Throwable error) {
        // A cancelled execution runs no more user code — and a recovery is
        // user code. This is also the door the cancelled async call comes
        // back through (its CancellationException), which is why the failure
        // must not be reported: nobody cancelled it by accident.
        if (cancelled) {
            complete(DefaultNioEngine.CANCELLED);
            return;
        }
        for (int i = from; i < links.size(); i++) {
            if (links.get(i) instanceof Recovery recovery && passesGuards(recovery)) {
                int next = i + 1;
                CompletableFuture.supplyAsync(() -> applyRecovery(recovery, error), engine.workersExecutorService)
                        .whenCompleteAsync((value, failure) -> {
                            if (failure != null) {
                                recover(next, DefaultNioEngine.unwrap(failure));
                            } else {
                                advance(next, value);
                            }
                        }, boss);
                return;
            }
        }
        fail(error);
    }

    private Object applyRecovery(Recovery recovery, Throwable error) {
        Object value = recovery.function().apply(error);
        if (metrics != null) {
            meter(() -> metrics.recoveryApplied(recovery.name()));
        }
        return value;
    }

    private void runBackground(Background background, Object value) {
        try {
            background.effect().accept(value);
        } catch (Throwable error) {
            engine.notifyError(error);
        }
    }

    // Ids recorded here always fit the bitset: it was sized from the same
    // chain this Decision came from. Only ids past the limit go to the map.
    private void recordDecision(int id, boolean value) {
        if (decisionBits != null) {
            int shift = (id & 31) << 1;
            decisionBits[id >>> 5] = (decisionBits[id >>> 5] & ~(0b11L << shift))
                    | ((value ? 0b11L : 0b01L) << shift);
        } else {
            if (decisionsOverflow == null) {
                decisionsOverflow = new HashMap<>();
            }
            decisionsOverflow.put(id, value);
        }
    }

    // Plain loop on purpose: this runs on the boss for every link of every
    // execution — no streams or allocations in the hot path.
    private boolean passesGuards(Link link) {
        List<Guard> guards = link.guards();
        if (guards == null || guards.isEmpty()) {
            return true;
        }
        for (int i = 0; i < guards.size(); i++) {
            Guard guard = guards.get(i);
            int id = guard.decision();
            if (decisionBits != null) {
                // Out-of-range ids (dangling or negative guards — >>> maps
                // negatives past any length) read as never recorded.
                int word = id >>> 5;
                if (word >= decisionBits.length) {
                    return false;
                }
                long bits = decisionBits[word] >>> ((id & 31) << 1);
                if ((bits & 0b01L) == 0 || ((bits & 0b10L) != 0) != guard.expected()) {
                    return false;
                }
            } else {
                Boolean recorded = decisionsOverflow == null ? null : decisionsOverflow.get(id);
                if (recorded == null || recorded != guard.expected()) {
                    return false;
                }
            }
        }
        return true;
    }
}
