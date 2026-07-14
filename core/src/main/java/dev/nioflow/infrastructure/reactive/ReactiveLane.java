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

    <R> ReactiveLane<R> adaptMono(Function<T, Mono<R>> call);

    /**
     * Re-types through a Mono with the budget ON THE MONO — see
     * {@link ReactiveFlow#handleMono(String, Function, Duration)}. A remote call
     * inside a branch or a fork body is still a remote call: it needs a budget
     * as much as one on the main line does.
     */
    <R> ReactiveLane<R> adaptMono(Function<T, Mono<R>> call, Duration budget);

    <R> ReactiveLane<List<R>> adaptFlux(Function<T, Flux<R>> call);

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
