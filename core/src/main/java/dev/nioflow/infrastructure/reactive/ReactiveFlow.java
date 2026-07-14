package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Context;
import dev.nioflow.core.facade.NioFlow;
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
 * The shared definition, with Mono/Flux steps — a {@link NioFlow}, not a
 * wrapper around one. Every inherited step is re-declared covariantly so a
 * chain never silently drops back to the base type and loses its reactive
 * steps halfway through.
 *
 * <p>A reactive stage is NOT a new kind of link: {@code handleMono} appends the
 * same {@code Stage} the other steps append, whose function parks a virtual
 * worker on the Mono. So it fuses, retries, rate-limits, lands in a lane and
 * reports its metrics exactly like every other stage — the engine never learns
 * what a Mono is.
 *
 * <p>Reactor is a compileOnly dependency of core: this interface only loads if
 * the consumer brings reactor-core, exactly like the OpenTelemetry and
 * Resilience4j adapters. That is why {@code handleMono} lives here and not on
 * {@link NioFlow}.
 */
public interface ReactiveFlow<I, O> extends NioFlow<I, O> {

    // ── the reactive steps ───────────────────────────────────────────────

    /** A stage whose work IS a Mono: the virtual worker parks on it. */
    ReactiveFlow<I, O> handleMono(String name, Function<I, Mono<I>> call);

    /**
     * Same, with the budget on the MONO. This is NOT
     * {@code handle(name, fn, timeout)}: a stage timeout abandons the parked
     * worker but cannot cancel what it waits on, so the HTTP request stays
     * alive on the connection pool. {@code mono.timeout(d)} cancels the
     * subscription, and reactor-netty releases the connection. For a reactive
     * call, this is the timeout you want.
     */
    ReactiveFlow<I, O> handleMono(String name, Function<I, Mono<I>> call, Duration budget);

    /**
     * Retry over the whole reactive call: each attempt re-subscribes the Mono,
     * backing off on the virtual worker. Composes in the documented layers —
     * rate limit gates admission, the budget bounds each attempt, retry spans
     * the attempts, recover() is the final net.
     */
    ReactiveFlow<I, O> handleMono(String name, Function<I, Mono<I>> call, Retry retry);

    ReactiveFlow<I, O> handleMono(String name, Function<I, Mono<I>> call, Duration budget, Retry retry);

    /** Parallel split-join over reactive branches; the join gives back an I. */
    <R> ReactiveFlow<I, O> fanOutMono(String name, List<Function<I, Mono<R>>> branches,
                                      Function<List<R>, I> join);

    // ── a Flux through the flow ──────────────────────────────────────────

    /**
     * Applies the flow to every element of a Flux, at most {@code concurrency}
     * in flight — flatMap, so the output is UNORDERED. Backpressure IS the
     * concurrency argument: it is the number of executions in flight. Reactor's
     * operator does the request(n) accounting; we do not implement a Publisher.
     */
    <R> Function<Flux<I>, Flux<R>> pipe(int concurrency,
                                        BiFunction<I, ReactiveStep<I, O>, ReactiveStep<R, O>> pipeline);

    /** Same, but the output order matches the input order (flatMapSequential). */
    <R> Function<Flux<I>, Flux<R>> pipeOrdered(int concurrency,
                                               BiFunction<I, ReactiveStep<I, O>, ReactiveStep<R, O>> pipeline);

    // ── every NioFlow step, re-declared covariantly ──────────────────────

    @Override
    ReactiveStep<I, O> just(I input);

    @Override
    ReactiveFlow<I, O> justAll(Iterable<I> inputs);

    @Override
    ReactiveFlow<I, O> handle(UnaryOperator<I> function);

    @Override
    ReactiveFlow<I, O> handle(String name, UnaryOperator<I> function);

    @Override
    ReactiveFlow<I, O> handle(String name, UnaryOperator<I> function, Duration timeout);

    @Override
    ReactiveFlow<I, O> handle(String name, UnaryOperator<I> function, Retry retry);

    @Override
    ReactiveFlow<I, O> handle(String name, UnaryOperator<I> function, Duration timeout, Retry retry);

    @Override
    ReactiveFlow<I, O> handle(String name, UnaryOperator<I> function, RateLimit rateLimit);

    @Override
    ReactiveFlow<I, O> handleContextual(BiFunction<I, Context, I> function);

    @Override
    ReactiveFlow<I, O> handleContextual(String name, BiFunction<I, Context, I> function);

    @Override
    ReactiveFlow<I, O> handleSync(UnaryOperator<I> function);

    @Override
    ReactiveFlow<I, O> handleSync(String name, UnaryOperator<I> function);

    @Override
    ReactiveFlow<I, O> background(Consumer<I> effect);

    @Override
    ReactiveFlow<I, O> background(String name, Consumer<I> effect);

    @Override
    ReactiveFlow<I, O> filter(Predicate<I> predicate);

    @Override
    <R> ReactiveFlow<I, O> fanOut(List<Function<I, R>> branches, Function<List<R>, I> join);

    @Override
    <R> ReactiveFlow<I, O> fanOut(String name, List<Function<I, R>> branches, Function<List<R>, I> join);

    @Override
    ReactiveFlow<I, O> batch(int size, Duration window, UnaryOperator<List<I>> bulk);

    @Override
    ReactiveFlow<I, O> batch(String name, int size, Duration window, UnaryOperator<List<I>> bulk);

    @Override
    <R> ReactiveFlow<I, O> fork(Segment<I, R> sub);

    @Override
    <R> ReactiveFlow<I, O> fork(String name, Segment<I, R> sub);

    @Override
    ReactiveFlow<I, O> use(Segment<I, I> segment);

    @Override
    ReactiveFlow<I, O> use(String region, Segment<I, I> segment);

    @Override
    ReactiveFlow<I, O> recover(Function<Throwable, I> function);

    @Override
    ReactiveFlow<I, O> recover(String name, Function<Throwable, I> function);

    @Override
    ReactiveFlow<I, O> onComplete(Consumer<O> callback);

    @Override
    ReactiveFlow<I, O> onError(Consumer<Throwable> callback);

    @Override
    ReactiveCondition<I, O> when(Predicate<I> predicate);

    @Override
    ReactiveCases<I, O> match();
}
