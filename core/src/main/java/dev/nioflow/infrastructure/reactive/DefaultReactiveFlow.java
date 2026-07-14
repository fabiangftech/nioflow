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
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Delegating mirror of a shared definition. Every step forwards to the wrapped
 * NioFlow — so the links, the guards, the validation and the engine are the
 * ones that were already there — and the result is re-wrapped so the chain keeps
 * its reactive type. The wrapping happens at BUILD time only.
 *
 * <p>{@link ReactiveConfig} — the default budget and the propagated context keys
 * — is the state the wrapper carries: it rides along every re-wrap, into
 * {@code just()}'s pipeline, into a branch's lane and into a fork's segment, so
 * what the flow declared once holds everywhere the chain goes.
 *
 * <p>AutoCloseable so a Spring bean declared with destroyMethod = "close" still
 * shuts the engine down through the definition that owns it.
 */
class DefaultReactiveFlow<I, O> implements ReactiveFlow<I, O>, AutoCloseable {

    final NioFlow<I, O> delegate;
    final ReactiveConfig config;

    DefaultReactiveFlow(NioFlow<I, O> delegate) {
        this(delegate, ReactiveConfig.NONE);
    }

    DefaultReactiveFlow(NioFlow<I, O> delegate, ReactiveConfig config) {
        this.delegate = delegate;
        this.config = config;
    }

    // A step that returns the flow itself stays this wrapper; one that returns
    // a different builder (a branch's main line) gets its own — with the config.
    private ReactiveFlow<I, O> wrap(NioFlow<I, O> result) {
        return result == delegate ? this : new DefaultReactiveFlow<>(result, config);
    }

    @Override
    public ReactiveFlow<I, O> defaultBudget(Duration budget) {
        return new DefaultReactiveFlow<>(delegate, config.withBudget(budget));
    }

    @Override
    public ReactiveFlow<I, O> propagate(Context.Key<?>... keys) {
        return new DefaultReactiveFlow<>(delegate, config.withKeys(keys));
    }

    // ── the reactive steps: ordinary stages whose function parks on a Mono ──

    @Override
    public ReactiveFlow<I, O> handleMono(String name, Function<I, Mono<I>> call) {
        return handleMono(name, call, config.budget());
    }

    @Override
    public ReactiveFlow<I, O> handleMono(String name, Function<I, Mono<I>> call, Duration budget) {
        return wrap(delegate.handle(name, value -> Blocking.await(Blocking.budgeted(call.apply(value), budget))));
    }

    @Override
    public ReactiveFlow<I, O> handleMono(String name, Function<I, Mono<I>> call, Retry retry) {
        return handleMono(name, call, config.budget(), retry);
    }

    @Override
    public ReactiveFlow<I, O> handleMono(String name, Function<I, Mono<I>> call, Duration budget, Retry retry) {
        return wrap(delegate.handle(name,
                value -> Blocking.await(Blocking.budgeted(call.apply(value), budget)), retry));
    }

    @Override
    public <R> ReactiveFlow<I, O> fanOutMono(String name, List<Function<I, Mono<R>>> branches,
                                             Function<List<R>, I> join) {
        return wrap(delegate.fanOut(name, Blocking.branches(branches, config.budget()), join));
    }

    // ── a Flux through the flow: Reactor's operators do the backpressure ──

    @Override
    public <R> Function<Flux<I>, Flux<R>> pipe(
            int concurrency, BiFunction<I, ReactiveStep<I, O>, ReactiveStep<R, O>> pipeline) {
        checkConcurrency(concurrency);
        return flux -> flux.flatMap(input -> pipeline.apply(input, just(input)).executeMono(), concurrency);
    }

    @Override
    public <R> Function<Flux<I>, Flux<R>> pipe(
            int concurrency, int prefetch, BiFunction<I, ReactiveStep<I, O>, ReactiveStep<R, O>> pipeline) {
        checkConcurrency(concurrency);
        checkPrefetch(prefetch);
        return flux -> flux.flatMap(
                input -> pipeline.apply(input, just(input)).executeMono(), concurrency, prefetch);
    }

    @Override
    public <R> Function<Flux<I>, Flux<R>> pipeOrdered(
            int concurrency, BiFunction<I, ReactiveStep<I, O>, ReactiveStep<R, O>> pipeline) {
        checkConcurrency(concurrency);
        return flux -> flux.flatMapSequential(
                input -> pipeline.apply(input, just(input)).executeMono(), concurrency);
    }

    @Override
    public <R> Function<Flux<I>, Flux<R>> pipeOrdered(
            int concurrency, int prefetch, BiFunction<I, ReactiveStep<I, O>, ReactiveStep<R, O>> pipeline) {
        checkConcurrency(concurrency);
        checkPrefetch(prefetch);
        return flux -> flux.flatMapSequential(
                input -> pipeline.apply(input, just(input)).executeMono(), concurrency, prefetch);
    }

    @Override
    public <R> Function<Flux<I>, Flux<R>> pipeResilient(
            int concurrency, BiFunction<I, ReactiveStep<I, O>, ReactiveStep<R, O>> pipeline,
            BiConsumer<I, Throwable> onElementError) {
        checkConcurrency(concurrency);
        if (onElementError == null) {
            throw new IllegalArgumentException("pipeResilient needs an element-error handler:"
                    + " dropping an element silently is exactly what it exists to prevent");
        }
        return flux -> flux.flatMap(input -> pipeline.apply(input, just(input)).executeMono()
                .onErrorResume(error -> {
                    onElementError.accept(input, error);
                    return Mono.empty();
                }), concurrency);
    }

    /**
     * Rejected here, not at the first element: Reactor validates concurrency on
     * SUBSCRIBE, so a pipe(0, ...) built at startup would blow up later, inside
     * FluxFlatMap, in a stack trace that names none of the caller's code.
     */
    private static void checkConcurrency(int concurrency) {
        if (concurrency < 1) {
            throw new IllegalArgumentException("pipe concurrency must be at least 1, was " + concurrency
                    + ": it is the number of executions in flight");
        }
    }

    private static void checkPrefetch(int prefetch) {
        if (prefetch < 0) {
            throw new IllegalArgumentException("pipe prefetch must not be negative, was " + prefetch
                    + ": it is how many elements the operator requests upstream ahead of the pipeline");
        }
    }

    // ── everything else: delegate, re-wrap ──

    @Override
    public ReactiveStep<I, O> just(I input) {
        return new DefaultReactiveStep<>(delegate.just(input), config);
    }

    @Override
    public ReactiveFlow<I, O> justAll(Iterable<I> inputs) {
        return wrap(delegate.justAll(inputs));
    }

    @Override
    public ReactiveFlow<I, O> handle(UnaryOperator<I> function) {
        return wrap(delegate.handle(function));
    }

    @Override
    public ReactiveFlow<I, O> handle(String name, UnaryOperator<I> function) {
        return wrap(delegate.handle(name, function));
    }

    @Override
    public ReactiveFlow<I, O> handle(String name, UnaryOperator<I> function, Duration timeout) {
        return wrap(delegate.handle(name, function, timeout));
    }

    @Override
    public ReactiveFlow<I, O> handle(String name, UnaryOperator<I> function, Retry retry) {
        return wrap(delegate.handle(name, function, retry));
    }

    @Override
    public ReactiveFlow<I, O> handle(String name, UnaryOperator<I> function, Duration timeout, Retry retry) {
        return wrap(delegate.handle(name, function, timeout, retry));
    }

    @Override
    public ReactiveFlow<I, O> handle(String name, UnaryOperator<I> function, RateLimit rateLimit) {
        return wrap(delegate.handle(name, function, rateLimit));
    }

    @Override
    public ReactiveFlow<I, O> handleContextual(BiFunction<I, Context, I> function) {
        return wrap(delegate.handleContextual(function));
    }

    @Override
    public ReactiveFlow<I, O> handleContextual(String name, BiFunction<I, Context, I> function) {
        return wrap(delegate.handleContextual(name, function));
    }

    @Override
    public ReactiveFlow<I, O> handleSync(UnaryOperator<I> function) {
        return wrap(delegate.handleSync(function));
    }

    @Override
    public ReactiveFlow<I, O> handleSync(String name, UnaryOperator<I> function) {
        return wrap(delegate.handleSync(name, function));
    }

    @Override
    public ReactiveFlow<I, O> background(Consumer<I> effect) {
        return wrap(delegate.background(effect));
    }

    @Override
    public ReactiveFlow<I, O> background(String name, Consumer<I> effect) {
        return wrap(delegate.background(name, effect));
    }

    @Override
    public ReactiveFlow<I, O> filter(Predicate<I> predicate) {
        return wrap(delegate.filter(predicate));
    }

    @Override
    public <R> ReactiveFlow<I, O> fanOut(List<Function<I, R>> branches, Function<List<R>, I> join) {
        return wrap(delegate.fanOut(branches, join));
    }

    @Override
    public <R> ReactiveFlow<I, O> fanOut(String name, List<Function<I, R>> branches, Function<List<R>, I> join) {
        return wrap(delegate.fanOut(name, branches, join));
    }

    @Override
    public ReactiveFlow<I, O> batch(int size, Duration window, UnaryOperator<List<I>> bulk) {
        return wrap(delegate.batch(size, window, bulk));
    }

    @Override
    public ReactiveFlow<I, O> batch(String name, int size, Duration window, UnaryOperator<List<I>> bulk) {
        return wrap(delegate.batch(name, size, window, bulk));
    }

    @Override
    public <R> ReactiveFlow<I, O> fork(Segment<I, R> sub) {
        return wrap(delegate.fork(Lanes.budgeted(sub, config.budget())));
    }

    @Override
    public <R> ReactiveFlow<I, O> fork(String name, Segment<I, R> sub) {
        return wrap(delegate.fork(name, Lanes.budgeted(sub, config.budget())));
    }

    @Override
    public ReactiveFlow<I, O> use(Segment<I, I> segment) {
        return wrap(delegate.use(Lanes.budgeted(segment, config.budget())));
    }

    @Override
    public ReactiveFlow<I, O> use(String region, Segment<I, I> segment) {
        return wrap(delegate.use(region, Lanes.budgeted(segment, config.budget())));
    }

    @Override
    public ReactiveFlow<I, O> recover(Function<Throwable, I> function) {
        return wrap(delegate.recover(function));
    }

    @Override
    public ReactiveFlow<I, O> recover(String name, Function<Throwable, I> function) {
        return wrap(delegate.recover(name, function));
    }

    @Override
    public ReactiveFlow<I, O> onComplete(Consumer<O> callback) {
        return wrap(delegate.onComplete(callback));
    }

    @Override
    public ReactiveFlow<I, O> onError(Consumer<Throwable> callback) {
        return wrap(delegate.onError(callback));
    }

    @Override
    public ReactiveCondition<I, O> when(Predicate<I> predicate) {
        return new DefaultReactiveCondition<>(delegate.when(predicate), config);
    }

    @Override
    public ReactiveCases<I, O> match() {
        return new DefaultReactiveCases<>(delegate.match(), config);
    }

    @Override
    public void close() throws Exception {
        if (delegate instanceof AutoCloseable closeable) {
            closeable.close();
        }
    }
}
