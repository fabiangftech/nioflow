package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.NioFlowMetrics;
import dev.nioflow.core.facade.NioFlowTracer;
import dev.nioflow.core.facade.Resilience;
import dev.nioflow.core.model.Diagnostics;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * The auto-scoped facade behind {@link DefaultNioFlow#autoScoped()}: every fluent
 * chain started on it opens its own {@link NioFlow#scoped() scope}, so callers get
 * per-call isolation without asking for it — the shape for one shared bean serving
 * many call sites, each declaring its own stages:
 *
 * <pre>{@code
 * flow.just("Hello")
 *     .handle("greeting", s -> s + ", World!")
 *     .join();                      // this chain's own scope, isolated per call
 * }</pre>
 *
 * <p>The first call on the facade returns the fresh scope, and the rest of the
 * chain — stages, forks, {@code join()}/{@code call} — lives on it. There is no
 * shared chain to operate on, so the shared-pipeline operations ({@code join},
 * {@code seal}, {@code release}, structural edits) throw on the facade itself
 * with a message that explains the model, rather than silently doing the wrong
 * thing. Global concerns keep working on the facade: {@code onComplete}/
 * {@code onError}/{@code metrics}/{@code trace} observe every chain's values, and
 * {@code close} stops the one shared engine.
 */
final class AutoScopedNioFlow<T> implements NioFlow<T> {

    private final DefaultNioFlow<T> backing;

    /** @param backing owns the engine every per-call scope rides */
    AutoScopedNioFlow(DefaultNioFlow<T> backing) {
        this.backing = backing;
    }

    /** A fresh scope: where every chain started on this facade actually lives. */
    private NioFlow<T> scope() {
        return backing.scoped();
    }

    @Override
    public NioFlow<T> just(T input) {
        return scope().just(input);
    }

    @Override
    public NioFlow<T> just(T input, Map<String, Object> context) {
        return scope().just(input, context);
    }

    @Override
    public NioFlow<T> justAll(Iterable<T> inputs) {
        return scope().justAll(inputs);
    }

    @Override
    public <R> CompletableFuture<R> call(T input) {
        return scope().call(input);
    }

    @Override
    public <R> CompletableFuture<R> call(T input, Map<String, Object> context) {
        return scope().call(input, context);
    }

    @Override
    public <R> CompletableFuture<R> call(T input, Duration timeout) {
        return scope().call(input, timeout);
    }

    @Override
    public <R> CompletableFuture<R> call(T input, Map<String, Object> context, Duration timeout) {
        return scope().call(input, context, timeout);
    }

    @Override
    public NioFlow<T> handle(Function<T, T> function) {
        return scope().handle(function);
    }

    @Override
    public NioFlow<T> handle(String name, Function<T, T> function) {
        return scope().handle(name, function);
    }

    @Override
    public NioFlow<T> handle(Function<T, T> function, Resilience<T> resilience) {
        return scope().handle(function, resilience);
    }

    @Override
    public NioFlow<T> submit(Function<T, T> function) {
        return scope().submit(function);
    }

    @Override
    public NioFlow<T> submit(String name, Function<T, T> function) {
        return scope().submit(name, function);
    }

    @Override
    public NioFlow<T> submit(Function<T, T> function, Duration timeout) {
        return scope().submit(function, timeout);
    }

    @Override
    public NioFlow<T> submit(Function<T, T> function, Resilience<T> resilience) {
        return scope().submit(function, resilience);
    }

    @Override
    public NioFlow<T> batch(int size, Duration maxWait, Function<List<T>, List<T>> function) {
        return scope().batch(size, maxWait, function);
    }

    @Override
    public <N> NioFlow<N> adapt(Function<T, N> function) {
        return scope().adapt(function);
    }

    @Override
    public <N> NioFlow<N> fanOut(Function<T, List<N>> function) {
        return scope().fanOut(function);
    }

    @Override
    public NioFlow<T> filter(Predicate<T> predicate) {
        return scope().filter(predicate);
    }

    @Override
    public Condition<T> when(Predicate<T> predicate) {
        return scope().when(predicate);
    }

    @Override
    public Cases<T> match() {
        return scope().match();
    }

    @Override
    public NioFlow<T> onErrorResume(Function<Throwable, T> fallback) {
        return scope().onErrorResume(fallback);
    }

    @Override
    public NioFlow<T> scoped() {
        return backing.scoped();
    }

    @Override
    public NioFlow<T> onError(Consumer<Throwable> handler) {
        backing.onError(handler); // global: observes every chain's values
        return this;
    }

    @Override
    public NioFlow<T> onComplete(Consumer<T> handler) {
        backing.onComplete(handler); // global: observes every chain's values
        return this;
    }

    @Override
    public NioFlow<T> metrics(NioFlowMetrics metrics) {
        backing.metrics(metrics);
        return this;
    }

    @Override
    public NioFlow<T> trace(NioFlowTracer tracer) {
        backing.trace(tracer);
        return this;
    }

    @Override
    public Diagnostics diagnostics() {
        return backing.diagnostics();
    }

    @Override
    public String toString() {
        return backing.toString();
    }

    @Override
    public T join() {
        throw sharedChain("join()");
    }

    @Override
    public T join(Duration timeout) {
        throw sharedChain("join(timeout)");
    }

    @Override
    public NioFlow<T> seal() {
        throw sharedChain("seal()");
    }

    @Override
    public NioFlow<T> release() {
        throw sharedChain("release()");
    }

    @Override
    public NioFlow<T> remove(String name) {
        throw sharedChain("remove()");
    }

    @Override
    public NioFlow<T> replace(String name, UnaryOperator<NioFlow<T>> segment) {
        throw sharedChain("replace()");
    }

    @Override
    public NioFlow<T> insertBefore(String anchor, UnaryOperator<NioFlow<T>> segment) {
        throw sharedChain("insertBefore()");
    }

    @Override
    public NioFlow<T> insertAfter(String anchor, UnaryOperator<NioFlow<T>> segment) {
        throw sharedChain("insertAfter()");
    }

    @Override
    public void close() {
        backing.close();
    }

    @Override
    public void close(Duration gracePeriod) {
        backing.close(gracePeriod);
    }

    /** Loud instead of silently wrong: the facade has no shared chain to act on. */
    private static IllegalStateException sharedChain(String operation) {
        return new IllegalStateException(operation + " needs a shared chain, and an auto-scoped "
                + "nio-flow has none: every chain started on it is its own scope. Keep chaining "
                + "instead — flow.just(x).handle(...).join() — or build a shared pipeline with "
                + "new DefaultNioFlow<>()");
    }
}
