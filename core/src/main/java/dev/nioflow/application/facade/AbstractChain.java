package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Context;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.Segment;
import dev.nioflow.core.model.Background;
import dev.nioflow.core.model.Batch;
import dev.nioflow.core.model.Decision;
import dev.nioflow.core.model.FanOut;
import dev.nioflow.core.model.Filter;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.Link;
import dev.nioflow.core.model.RateLimit;
import dev.nioflow.core.model.Recovery;
import dev.nioflow.core.model.Retry;
import dev.nioflow.core.model.Stage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

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

    void stage(String name, Function<X, X> function) {
        appendLink(new Stage(name, asObjectFunction(function), false, null, null, guards()));
    }

    void stage(String name, Function<X, X> function, Duration timeout) {
        appendLink(new Stage(name, asObjectFunction(function), false, timeout, null, guards()));
    }

    void stage(String name, Function<X, X> function, Retry retry) {
        appendLink(new Stage(name, asObjectFunction(function), false, null, retry, guards()));
    }

    void stage(String name, Function<X, X> function, Duration timeout, Retry retry) {
        appendLink(new Stage(name, asObjectFunction(function), false, timeout, retry, guards()));
    }

    void rateLimitedStage(String name, Function<X, X> function, RateLimit rateLimit) {
        Function<Object, Object> body = asObjectFunction(function);
        // The acquire runs where the stage runs — a virtual worker — so the
        // wait parks cheaply, never blocks the boss, and fuses like any
        // other no-timeout stage.
        appendLink(new Stage(name, value -> {
            rateLimit.acquire();
            return body.apply(value);
        }, false, null, null, guards()));
    }

    void syncStage(String name, Function<X, X> function) {
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
