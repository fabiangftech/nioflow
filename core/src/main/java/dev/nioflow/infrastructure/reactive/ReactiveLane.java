package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Context;
import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.Segment;
import dev.nioflow.core.model.RateLimit;
import dev.nioflow.core.model.Retry;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * A fork lane with the reactive steps.
 *
 * <p>Reach one with {@link Reactive#lane(Lane)} inside a when()/match() lambda:
 * the branch contracts hard-code {@code UnaryOperator<Lane<T>>}, and Java will
 * not let a reactive variant both BE a Condition and hand out a reactive lane
 * (same erasure — it is a name clash, not an override). That single unwrap is
 * the whole price.
 *
 * <p>The lane a reactive flow hands to that lambda already carries the flow's
 * {@link ReactiveFlow#defaultBudget}, so a {@code handleMono} inside a branch or
 * a fork is protected exactly like one on the main line.
 */
public interface ReactiveLane<T> extends Lane<T> {

    ReactiveLane<T> handleMono(String name, Function<T, Mono<T>> call);

    ReactiveLane<T> handleMono(String name, Function<T, Mono<T>> call, Duration budget);

    /**
     * Retry over the whole reactive call: each attempt re-subscribes the Mono,
     * backing off on the virtual worker. Composes in the documented layers —
     * rate limit gates admission, the budget bounds each attempt, retry spans
     * the attempts, recover() is the final net.
     */
    ReactiveLane<T> handleMono(String name, Function<T, Mono<T>> call, Retry retry);

    ReactiveLane<T> handleMono(String name, Function<T, Mono<T>> call, Duration budget, Retry retry);

    /**
     * The reactive stage that does NOT park a worker; see
     * {@link ReactiveFlow#handleMonoAsync}. A remote call inside a branch or a
     * fork body is still a remote call — it deserves the same choice.
     */
    ReactiveLane<T> handleMonoAsync(String name, Function<T, Mono<T>> call);

    ReactiveLane<T> handleMonoAsync(String name, Function<T, Mono<T>> call, Duration budget);

    /** Re-types through a Mono without parking a worker; see #handleMonoAsync. */
    <R> ReactiveLane<R> adaptMonoAsync(Function<T, Mono<R>> call);

    <R> ReactiveLane<R> adaptMonoAsync(Function<T, Mono<R>> call, Duration budget);

    <R> ReactiveLane<R> adaptMono(Function<T, Mono<R>> call);

    /**
     * Re-types through a Mono with the budget ON THE MONO — see
     * {@link ReactiveFlow#handleMono(String, Function, Duration)}. A remote call
     * inside a branch or a fork body is still a remote call: it needs a budget
     * as much as one on the main line does.
     */
    <R> ReactiveLane<R> adaptMono(Function<T, Mono<R>> call, Duration budget);

    /**
     * Collects a Flux into the List the lane carries, WITH NO CAP: see
     * {@link ReactiveStep#adaptFlux(Function)}. Prefer the bounded overload — a
     * branch or a fork body is no safer a place to buffer an unbounded stream
     * than the main line is.
     */
    <R> ReactiveLane<List<R>> adaptFlux(Function<T, Flux<R>> call);

    /** The same collect, bounded — see {@link ReactiveStep#adaptFlux(Function, int)}. */
    <R> ReactiveLane<List<R>> adaptFlux(Function<T, Flux<R>> call, int maxItems);

    <R, C> ReactiveLane<C> fanOutMono(String name, List<Function<T, Mono<R>>> branches,
                                      Function<List<R>, C> join);

    // ── every Lane step, re-declared covariantly ─────────────────────────

    @Override
    ReactiveLane<T> handle(UnaryOperator<T> function);

    @Override
    ReactiveLane<T> handle(String name, UnaryOperator<T> function);

    @Override
    ReactiveLane<T> handle(String name, UnaryOperator<T> function, Duration timeout);

    @Override
    ReactiveLane<T> handle(String name, UnaryOperator<T> function, Retry retry);

    @Override
    ReactiveLane<T> handle(String name, UnaryOperator<T> function, Duration timeout, Retry retry);

    @Override
    ReactiveLane<T> handle(String name, UnaryOperator<T> function, RateLimit rateLimit);

    @Override
    ReactiveLane<T> handleAsync(String name, Function<T, CompletionStage<T>> call);

    @Override
    ReactiveLane<T> handleAsync(String name, Function<T, CompletionStage<T>> call, Duration timeout);

    @Override
    ReactiveLane<T> handleAsync(String name, Function<T, CompletionStage<T>> call, Retry retry);

    @Override
    ReactiveLane<T> handleAsync(String name, Function<T, CompletionStage<T>> call,
                                Duration timeout, Retry retry);

    @Override
    <R> ReactiveLane<R> adaptAsync(Function<T, CompletionStage<R>> call);

    @Override
    <R> ReactiveLane<R> adaptAsync(Function<T, CompletionStage<R>> call, Duration timeout);

    @Override
    ReactiveLane<T> handleContextual(BiFunction<T, Context, T> function);

    @Override
    ReactiveLane<T> handleContextual(String name, BiFunction<T, Context, T> function);

    @Override
    ReactiveLane<T> handleSync(UnaryOperator<T> function);

    @Override
    ReactiveLane<T> handleSync(String name, UnaryOperator<T> function);

    @Override
    ReactiveLane<T> background(Consumer<T> effect);

    @Override
    ReactiveLane<T> background(String name, Consumer<T> effect);

    @Override
    <R> ReactiveLane<R> adapt(Function<T, R> function);

    @Override
    <R, C> ReactiveLane<C> fanOut(List<Function<T, R>> branches, Function<List<R>, C> join);

    @Override
    <R, C> ReactiveLane<C> fanOut(String name, List<Function<T, R>> branches, Function<List<R>, C> join);

    @Override
    <R> ReactiveLane<R> batch(int size, Duration window, Function<List<T>, List<R>> bulk);

    @Override
    <R> ReactiveLane<R> batch(String name, int size, Duration window, Function<List<T>, List<R>> bulk);

    @Override
    <R> ReactiveLane<R> use(Segment<T, R> segment);

    @Override
    <R> ReactiveLane<R> use(String region, Segment<T, R> segment);

    @Override
    <R> ReactiveLane<T> fork(Segment<T, R> sub);

    @Override
    <R> ReactiveLane<T> fork(String name, Segment<T, R> sub);

    @Override
    ReactiveLane<T> filter(Predicate<T> predicate);

    @Override
    ReactiveLane<T> recover(Function<Throwable, T> function);

    @Override
    ReactiveLane<T> recover(String name, Function<Throwable, T> function);

    @Override
    ReactiveLaneCondition<T> when(Predicate<T> predicate);

    @Override
    ReactiveLaneCases<T> match();
}
