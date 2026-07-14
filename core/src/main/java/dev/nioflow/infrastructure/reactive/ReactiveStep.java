package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Context;
import dev.nioflow.core.facade.NioStep;
import dev.nioflow.core.facade.Segment;
import dev.nioflow.core.model.RateLimit;
import dev.nioflow.core.model.Retry;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * The per-request pipeline, with Mono/Flux steps and a Mono terminal.
 * See {@link ReactiveFlow} for why the reactive steps are ordinary stages.
 *
 * <p>A pipeline inherits the {@link ReactiveFlow#defaultBudget} of the flow whose
 * {@code just()} opened it: every reactive step here that declares no budget of
 * its own gets that one.
 */
public interface ReactiveStep<T, O> extends NioStep<T, O> {

    // ── the reactive steps ───────────────────────────────────────────────

    /**
     * A stage whose work IS a Mono: the virtual worker parks on it — forever, if
     * the Mono never completes and no budget (here or as the flow's default)
     * bounds it. See {@link ReactiveFlow#defaultBudget}.
     */
    ReactiveStep<T, O> handleMono(String name, Function<T, Mono<T>> call);

    /** Same, with the budget on the MONO — see ReactiveFlow#handleMono. */
    ReactiveStep<T, O> handleMono(String name, Function<T, Mono<T>> call, Duration budget);

    /**
     * Retry over the whole reactive call: each attempt re-subscribes the Mono,
     * backing off on the virtual worker. Composes in the documented layers —
     * rate limit gates admission, the budget bounds each attempt, retry spans
     * the attempts, recover() is the final net.
     */
    ReactiveStep<T, O> handleMono(String name, Function<T, Mono<T>> call, Retry retry);

    ReactiveStep<T, O> handleMono(String name, Function<T, Mono<T>> call, Duration budget, Retry retry);

    /** Re-types THROUGH a Mono: T -> Mono&lt;R&gt; -> the chain continues at R. */
    <R> ReactiveStep<R, O> adaptMono(Function<T, Mono<R>> call);

    /** Re-types through a Mono with a budget on it; see #handleMono. */
    <R> ReactiveStep<R, O> adaptMono(Function<T, Mono<R>> call, Duration budget);

    /**
     * Collects a Flux into the List the chain carries. BUFFERS THE WHOLE
     * STREAM: for bounded results only — a nioflow value is one object, and a
     * large export belongs in a Flux, not in a pipeline.
     */
    <R> ReactiveStep<List<R>, O> adaptFlux(Function<T, Flux<R>> call);

    /** Parallel split-join over reactive branches, each on its own worker. */
    <R, C> ReactiveStep<C, O> fanOutMono(String name, List<Function<T, Mono<R>>> branches,
                                         Function<List<R>, C> join);

    /**
     * The terminal. LAZY: the pipeline runs on subscription, once per
     * subscription — so an unsubscribed Mono runs nothing, and .retry() /
     * .repeat() re-run the whole pipeline. A filter() cut arrives as an empty
     * Mono (switchIfEmpty is your 404); a terminal failure as onError.
     */
    Mono<T> executeMono();

    // ── every NioStep step, re-declared covariantly ──────────────────────

    @Override
    ReactiveStep<T, O> handle(UnaryOperator<T> function);

    @Override
    ReactiveStep<T, O> handle(String name, UnaryOperator<T> function);

    @Override
    ReactiveStep<T, O> handle(String name, UnaryOperator<T> function, Duration timeout);

    @Override
    ReactiveStep<T, O> handle(String name, UnaryOperator<T> function, Retry retry);

    @Override
    ReactiveStep<T, O> handle(String name, UnaryOperator<T> function, Duration timeout, Retry retry);

    @Override
    ReactiveStep<T, O> handle(String name, UnaryOperator<T> function, RateLimit rateLimit);

    @Override
    ReactiveStep<T, O> handleContextual(BiFunction<T, Context, T> function);

    @Override
    ReactiveStep<T, O> handleContextual(String name, BiFunction<T, Context, T> function);

    @Override
    ReactiveStep<T, O> handleSync(UnaryOperator<T> function);

    @Override
    ReactiveStep<T, O> handleSync(String name, UnaryOperator<T> function);

    @Override
    ReactiveStep<T, O> background(Consumer<T> effect);

    @Override
    ReactiveStep<T, O> background(String name, Consumer<T> effect);

    @Override
    <R> ReactiveStep<R, O> adapt(Function<T, R> function);

    @Override
    ReactiveStep<T, O> filter(Predicate<T> predicate);

    @Override
    <R, C> ReactiveStep<C, O> fanOut(List<Function<T, R>> branches, Function<List<R>, C> join);

    @Override
    <R, C> ReactiveStep<C, O> fanOut(String name, List<Function<T, R>> branches, Function<List<R>, C> join);

    @Override
    <R> ReactiveStep<R, O> batch(int size, Duration window, Function<List<T>, List<R>> bulk);

    @Override
    <R> ReactiveStep<R, O> batch(String name, int size, Duration window, Function<List<T>, List<R>> bulk);

    @Override
    <R> ReactiveStep<T, O> fork(Segment<T, R> sub);

    @Override
    <R> ReactiveStep<T, O> fork(String name, Segment<T, R> sub);

    @Override
    <R> ReactiveStep<R, O> use(Segment<T, R> segment);

    @Override
    ReactiveStep<T, O> recover(Function<Throwable, T> function);

    @Override
    ReactiveStep<T, O> recover(String name, Function<Throwable, T> function);

    @Override
    ReactiveStep<T, O> onComplete(Consumer<T> callback);

    @Override
    ReactiveStep<T, O> onError(Consumer<Throwable> callback);

    @Override
    ReactiveStep<T, O> key(Object key);

    @Override
    <V> ReactiveStep<T, O> with(Context.Key<V> key, V value);

    @Override
    ReactiveStepCondition<T, O> when(Predicate<T> predicate);

    @Override
    ReactiveStepCases<T, O> match();
}
