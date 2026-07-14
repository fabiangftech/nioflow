package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Context;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.Segment;
import dev.nioflow.core.model.Background;
import dev.nioflow.core.model.Batch;
import dev.nioflow.core.model.Decision;
import dev.nioflow.core.model.FanOut;
import dev.nioflow.core.model.Filter;
import dev.nioflow.core.model.Fork;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.Link;
import dev.nioflow.core.model.RateLimit;
import dev.nioflow.core.model.Recovery;
import dev.nioflow.core.model.Retry;
import dev.nioflow.core.model.Stage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Chain-building core, shared by the three typed facades: the shared
 * definition (DefaultNioFlow, building over its input type), the per-request
 * builder (ExecutionNioFlow, building over the value's current type) and fork
 * lanes (DefaultLane). X is the type of the value at this point of the chain.
 *
 * <p>Every link is created here and every unchecked cast lives here: the
 * engine is untyped by design (it moves {@code Object}s), so the type
 * parameters exist only to make the facades safe to call. Forks work through
 * views: withGuards() returns the same chain with active Guards, so every link
 * declared inside a lane is conditioned on its Decision and nested forks
 * compose guards automatically.
 */
abstract class AbstractChain<X> {

    abstract NioEngine engine();

    abstract void appendLink(Link link);

    abstract List<Guard> guards();

    abstract AbstractChain<X> withGuards(List<Guard> guards);

    abstract String anonymousName(String prefix);

    void stage(String name, UnaryOperator<X> function) {
        appendLink(new Stage(name, asObjectFunction(function), false, null, null, guards()));
    }

    void stage(String name, UnaryOperator<X> function, Duration timeout) {
        appendLink(new Stage(name, asObjectFunction(function), false, timeout, null, guards()));
    }

    void stage(String name, UnaryOperator<X> function, Retry retry) {
        appendLink(new Stage(name, asObjectFunction(function), false, null, retry, guards()));
    }

    void stage(String name, UnaryOperator<X> function, Duration timeout, Retry retry) {
        appendLink(new Stage(name, asObjectFunction(function), false, timeout, retry, guards()));
    }

    void rateLimitedStage(String name, UnaryOperator<X> function, RateLimit rateLimit) {
        Function<Object, Object> body = asObjectFunction(function);
        // The acquire runs where the stage runs — a virtual worker — so the
        // wait parks cheaply, never blocks the boss, and fuses like any
        // other no-timeout stage.
        appendLink(new Stage(name, value -> {
            rateLimit.acquire();
            return body.apply(value);
        }, false, null, null, guards()));
    }

    void syncStage(String name, UnaryOperator<X> function) {
        appendLink(new Stage(name, asObjectFunction(function), true, null, null, guards()));
    }

    @SuppressWarnings("unchecked")
    void contextualStage(String name, BiFunction<X, Context, X> function) {
        appendLink(new Stage(name,
                new ContextualFunction((BiFunction<Object, Context, Object>) (BiFunction<?, ?, ?>) function),
                false, null, null, guards()));
    }

    @SuppressWarnings("unchecked")
    void backgroundEffect(String name, Consumer<X> effect) {
        appendLink(new Background(name, (Consumer<Object>) effect, guards()));
    }

    @SuppressWarnings("unchecked")
    void filterValues(Predicate<X> predicate) {
        appendLink(new Filter((Predicate<Object>) predicate, guards()));
    }

    /** Re-types the chain: the caller re-parameterizes itself (the engine does not care). */
    <R> void adaptValue(Function<X, R> function) {
        appendLink(new Stage(anonymousName("adapt"), asObjectFunction(function), false, null, null, guards()));
    }

    @SuppressWarnings("unchecked")
    <R, C> void fanOutBranches(String name, List<Function<X, R>> branches, Function<List<R>, C> join) {
        appendLink(new FanOut(name,
                (List<Function<Object, Object>>) (List<?>) List.copyOf(branches),
                (Function<List<Object>, Object>) (Function<?, ?>) join,
                guards()));
    }

    @SuppressWarnings("unchecked")
    <R> void batchValues(String name, int size, Duration window, Function<List<X>, List<R>> bulk) {
        appendLink(new Batch(name, size, window,
                (Function<List<Object>, List<Object>>) (Function<?, ?>) bulk, guards()));
    }

    /** Embeds a segment inline: its links are appended with the current guards. */
    <R> void embed(Segment<X, R> segment) {
        segment.define(new DefaultLane<>(this));
    }

    /**
     * Records the segment OFF the chain and appends it as one detached Fork
     * link. Two guard scopes meet here:
     *
     * <ul>
     * <li>the Fork LINK carries the caller's guards, so a fork declared inside
     *     a lane only spawns for values routed down that branch;</li>
     * <li>the links INSIDE start from no guards (the recorder's), so the
     *     sub-chain is guard-closed — the child execution has its own decision
     *     bitset, and a guard pointing at a parent decision would be dangling
     *     in it.</li>
     * </ul>
     */
    <R> void forkSegment(String name, Segment<X, R> segment) {
        List<Link> recorded = new ArrayList<>();
        segment.define(new DefaultLane<>(new RecordingChain<>(engine(), recorded, this::anonymousName)));
        if (recorded.isEmpty()) {
            throw new IllegalArgumentException("Fork '" + name + "' is empty: the segment declared no links");
        }
        appendLink(new Fork(name, compactDecisions(recorded), guards()));
    }

    /**
     * Renumbers the sub-chain's decisions to 0..n-1. Ids come from the
     * engine-wide counter, which grows forever under per-request forks and
     * would push the child's decision bitset into its overflow map for no
     * reason: inside a fork they are private (the chain is guard-closed), so
     * they can be compacted. Links with neither guards nor a decision id keep
     * their instance — Batch in particular, whose identity keys its in-flight
     * group.
     */
    private static List<Link> compactDecisions(List<Link> links) {
        Map<Integer, Integer> remap = new HashMap<>();
        for (Link link : links) {
            if (link instanceof Decision decision) {
                remap.put(decision.id(), remap.size());
            }
        }
        if (remap.isEmpty()) {
            return List.copyOf(links);
        }
        List<Link> compacted = new ArrayList<>(links.size());
        for (Link link : links) {
            compacted.add(remapped(link, remap));
        }
        return List.copyOf(compacted);
    }

    private static Link remapped(Link link, Map<Integer, Integer> remap) {
        List<Guard> guards = remapGuards(link.guards(), remap);
        boolean same = guards == link.guards();
        return switch (link) {
            case Decision decision -> new Decision(decision.predicate(), remap.get(decision.id()), guards);
            case Stage stage -> same ? stage : new Stage(stage.name(), stage.function(), stage.sync(),
                    stage.timeout(), stage.retry(), guards);
            case Filter filter -> same ? filter : new Filter(filter.predicate(), guards);
            case Recovery recovery -> same ? recovery
                    : new Recovery(recovery.name(), recovery.function(), guards);
            case Background background -> same ? background
                    : new Background(background.name(), background.effect(), guards);
            case FanOut fanOut -> same ? fanOut
                    : new FanOut(fanOut.name(), fanOut.branches(), fanOut.join(), guards);
            case Batch batch -> same ? batch
                    : new Batch(batch.name(), batch.size(), batch.window(), batch.bulk(), guards);
            // A nested fork's own chain was already compacted in ITS scope.
            case Fork fork -> same ? fork : new Fork(fork.name(), fork.chain(), guards);
        };
    }

    private static List<Guard> remapGuards(List<Guard> guards, Map<Integer, Integer> remap) {
        if (guards == null || guards.isEmpty()) {
            return guards;
        }
        List<Guard> next = new ArrayList<>(guards.size());
        for (Guard guard : guards) {
            Integer id = remap.get(guard.decision());
            if (id == null) {
                // Unreachable through the fluent API (the recorder starts with
                // no guards, so a sub-chain guard can only name a sub-chain
                // decision). Loud here rather than silently mis-routing: after
                // compaction the stale id could collide with a compacted one.
                throw new IllegalStateException("Fork sub-chain is guarded by decision " + guard.decision()
                        + ", which is not declared inside the fork");
            }
            next.add(new Guard(id, guard.expected()));
        }
        return List.copyOf(next);
    }

    /**
     * Same inline embedding, plus the appended span is remembered (by link
     * identity) so spliceRegion can swap it atomically later.
     */
    <R> void embed(String region, Segment<X, R> segment) {
        List<Link> before = engine().chain();
        segment.define(new DefaultLane<>(this));
        List<Link> after = engine().chain();
        if (after.size() == before.size()) {
            throw new IllegalArgumentException("Region '" + region + "' is empty: the segment appended no links");
        }
        engine().rememberRegion(region, after.get(before.size()), after.get(after.size() - 1));
    }

    @SuppressWarnings("unchecked")
    void recovery(String name, Function<Throwable, X> function) {
        appendLink(new Recovery(name, (Function<Throwable, Object>) function, guards()));
    }

    @SuppressWarnings("unchecked")
    int appendDecision(Predicate<X> predicate) {
        int decision = engine().nextDecision();
        appendLink(new Decision((Predicate<Object>) predicate, decision, guards()));
        return decision;
    }

    static List<Guard> withGuard(List<Guard> guards, Guard extra) {
        List<Guard> next = new ArrayList<>(guards);
        next.add(extra);
        return List.copyOf(next);
    }

    static List<Guard> withGuards(List<Guard> guards, List<Guard> extras) {
        List<Guard> next = new ArrayList<>(guards);
        next.addAll(extras);
        return List.copyOf(next);
    }

    @SuppressWarnings("unchecked")
    static Function<Object, Object> asObjectFunction(Function<?, ?> function) {
        return (Function<Object, Object>) function;
    }
}
