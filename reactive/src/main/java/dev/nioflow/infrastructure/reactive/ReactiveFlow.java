package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Context;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.NioStep;
import dev.nioflow.core.facade.Pipeline;
import dev.nioflow.core.facade.Segment;
import dev.nioflow.core.model.RateLimit;
import dev.nioflow.core.model.Retry;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
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

    // ── the default budget ───────────────────────────────────────────────

    /**
     * The budget every reactive step of this flow gets when it declares none of
     * its own — {@code handleMono}, {@code adaptMono}, {@code adaptFlux} and
     * {@code fanOutMono}, on this flow, in the pipelines its {@code just()}
     * opens, inside its branches and inside its forks. An explicit per-step
     * budget always wins.
     *
     * <p><b>Declare one if the flow talks to the network.</b> The engine has no
     * cancellation: a Mono that never completes — a hung connection, a broken
     * server, a Sinks.One nobody emits into — leaks for the life of the JVM, and
     * nothing can ever free it. The footprint differs by path but the leak is
     * equally permanent: on the blocking path (the default) it <b>parks a virtual
     * worker</b> forever; on the async path ({@link #preferAsync}, {@code
     * handleMonoAsync}) it <b>pins the execution and leaks the connection</b>,
     * with no parked thread to make it visible. A default budget closes BOTH: the
     * subscription is cancelled (reactor-netty releases the connection) and the
     * failure reaches {@code recover()} as a
     * {@link java.util.concurrent.TimeoutException}, exactly like an explicit
     * {@code handleMono(name, call, budget)}. To make an unbudgeted reactive step
     * a build error rather than a documentation footnote, see {@link
     * #requireBudget()}.
     *
     * <p>Not mandatory, on purpose: a {@code Mono.just(...)} or an in-memory
     * cache lookup does not need one, and a legitimately long step (an export, a
     * slow batch job) would be cut short by a default that was chosen for the
     * fast path — give that one its own, larger budget.
     *
     * <p>Build-time only, and it does not mutate: the returned flow is the same
     * definition, wrapped with the budget.
     */
    ReactiveFlow<I, O> defaultBudget(Duration budget);

    // ── the context bridge ───────────────────────────────────────────────

    /**
     * The keys this flow lifts out of Reactor's subscriber context and into the
     * per-execution {@link Context}, on every {@code executeMono} /
     * {@code executeFlux} — declared once, here, so no controller method has to
     * write the {@code Mono.deferContextual(...).with(...)} dance again.
     *
     * <pre>
     * ReactiveFlow&lt;String, Receipt&gt; orders =
     *         Reactive.&lt;String, Receipt&gt;flow(DefaultNioFlow.from(String.class))
     *                 .propagate(TRACE, TENANT);       // once, in the config
     *
     * orders.just(id)
     *       .handleContextual("charge", (order, ctx) -&gt; psp.charge(order, ctx.get(TRACE)))
     *       .executeMono();                            // TRACE is already there
     * </pre>
     *
     * <p>The keys line up by NAME: {@code Context.Key.of("traceId")} reads the
     * subscriber-context entry {@code "traceId"} (the same correspondence that
     * lets a map handed to {@code engine.call} interoperate). A key the
     * subscriber context does not carry is simply not seeded — no throw, no null
     * entry, and {@code ctx.get} gives back null exactly as it would for a key
     * nobody ever wrote.
     *
     * <p><b>Declared-and-automatic, never discovered-and-automatic.</b> There is
     * no {@code Hooks}, no Micrometer context propagation and no write-back: a
     * reader of the config sees exactly what crosses the boundary, and nothing
     * crosses that a person did not write down. Implicit context that is right
     * 99 % of the time is wrong during the incident you bought it for.
     *
     * <p>Seeded per SUBSCRIPTION, not per assembly — two subscriptions of the
     * same Mono under two different subscriber contexts get their own values —
     * and an explicit {@link NioStep#with} on the pipeline wins over a propagated
     * key of the same name: that one the caller wrote down here.
     *
     * @throws IllegalArgumentException if no key is given (a whitelist of nothing
     *         is a mistake, and silence would never tell you)
     */
    ReactiveFlow<I, O> propagate(Context.Key<?>... keys);

    /**
     * Routes every {@code handleMono}/{@code adaptMono} on this flow — and on the
     * pipelines it builds — to the async, future-holding path instead of parking
     * a virtual worker on the Mono. This is what {@code pipe} does per element by
     * default (the ingestion loop at high concurrency, where a parked worker per
     * element is ~3 KB of stack each); declare it explicitly on a flow whose
     * prebuilt {@link #pipeline(Segment) pipeline} you then run through
     * {@code pipe}, so the prebuilt path holds futures too.
     *
     * <p>Since RFC 0013 an async run fuses like a blocking one, so there is no
     * throughput reason left to park at volume. A single request/response can
     * leave this off — one parked worker for one request is fine, and the block
     * path is simpler. {@code RateLimit} steps are unaffected (they are plain
     * {@code handle}s that park on {@code acquire()} by design, not
     * {@code handleMono}). The budget on the async path <b>cancels</b> the
     * subscription on expiry, where the block path only abandons the worker.
     */
    ReactiveFlow<I, O> preferAsync();

    /**
     * Makes an unbudgeted reactive step a BUILD-TIME error instead of a runtime
     * leak. With this on, every {@code handleMono}/{@code adaptMono}/{@code
     * handleMonoAsync}/{@code adaptMonoAsync}/{@code adaptFlux}/{@code fanOutMono}
     * — on this flow, in the pipelines its {@code just()} opens, inside its
     * branches and inside its forks — that resolves to a null budget (neither a
     * per-step budget nor a {@link #defaultBudget}) is rejected at assembly, where
     * the caller's line number still exists.
     *
     * <p><b>On by default since RFC 0034</b>, on purpose: an unbudgeted reactive
     * step is the documented forever-leak (a parked worker on a hung upstream, a
     * pinned execution and a leaked connection on the async path), so the safe
     * thing is mandatory — the facade cannot tell a remote call from a {@code
     * Mono.just}, so the guarantee is total rather than guessed. This method stays
     * as an explicit affirmation; to WAIVE the requirement for the {@code
     * Mono.just(...)}/in-memory-cache chains that genuinely need no budget, call
     * {@link #allowUnbudgeted()}. The same build-time stance {@code propagate()}
     * and the bounded {@code adaptFlux} already take.
     */
    ReactiveFlow<I, O> requireBudget();

    /**
     * Waives the {@link #requireBudget() default} budget requirement: with this
     * on, an unbudgeted reactive step is allowed instead of a build-time error.
     *
     * <p>The opt-out for a flow whose reactive steps genuinely park on nothing — a
     * {@code Mono.just(...)}, an in-memory cache lookup, a {@code Sinks} you
     * control — where forcing a budget would be friction with no leak to prevent.
     * It is a conscious waiver, not a default: a network-facing flow that reaches
     * for this instead of a {@link #defaultBudget} is turning off the one thing
     * standing between a hung socket and a worker lost for the life of the JVM. A
     * {@code defaultBudget} or a per-step budget is the safe answer; this is for
     * when there is genuinely nothing to bound.
     */
    ReactiveFlow<I, O> allowUnbudgeted();

    // ── the reactive steps ───────────────────────────────────────────────

    /**
     * A stage whose work IS a Mono: the virtual worker parks on it.
     *
     * <p>With no {@link #defaultBudget} declared on the flow, this parks
     * FOREVER on a Mono that never completes. See defaultBudget.
     */
    ReactiveFlow<I, O> handleMono(String name, Function<I, Mono<I>> call);

    /**
     * Same, with the budget on the MONO. This is NOT
     * {@code handle(name, fn, timeout)}: a stage timeout abandons the parked
     * worker but cannot cancel what it waits on, so the HTTP request stays
     * alive on the connection pool. {@code mono.timeout(d)} cancels the
     * subscription, and reactor-netty releases the connection. For a reactive
     * call, this is the timeout you want.
     *
     * <p>Overrides the flow's {@link #defaultBudget}, if it declared one.
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

    /**
     * The reactive stage that does NOT park a worker: the Mono becomes a
     * {@link java.util.concurrent.CompletionStage} ({@code mono.toFuture()}) and
     * the engine's AsyncStage holds a future, not a thread.
     *
     * <p>Against {@link #handleMono}, this is a choice and not an upgrade: a
     * handleMono FUSES (four of them are 2 thread hops) and parks a virtual
     * worker for the whole call; a handleMonoAsync is a dispatch boundary (four
     * are 4 round trips) and parks nothing. Hops are microseconds against a
     * remote call — heap is the axis that moves, so reach for this one at high
     * in-flight concurrency, or when the call must be cancellable.
     */
    ReactiveFlow<I, O> handleMonoAsync(String name, Function<I, Mono<I>> call);

    /**
     * Same, with a budget — and here the budget is the ENGINE's timeout, not
     * {@code mono.timeout(d)}: on expiry the engine cancels the future, and
     * cancelling a {@code mono.toFuture()} cancels the SUBSCRIPTION (reactor-netty
     * releases the connection). The two things {@link #handleMono} had to keep
     * apart — the engine's timeout and the Mono's budget — are one thing here.
     */
    ReactiveFlow<I, O> handleMonoAsync(String name, Function<I, Mono<I>> call, Duration budget);

    /** Parallel split-join over reactive branches; the join gives back an I. */
    <R> ReactiveFlow<I, O> fanOutMono(String name, List<Function<I, Mono<R>>> branches,
                                      Function<List<R>, I> join);

    // ── a Flux through the flow ──────────────────────────────────────────

    /**
     * Applies the flow to every element of a Flux, at most {@code concurrency}
     * in flight — flatMap, so the output is UNORDERED. Backpressure IS the
     * concurrency argument: it is the number of executions in flight. Reactor's
     * operator does the request(n) accounting; we do not implement a Publisher.
     *
     * <p>One failing element FAILS THE WHOLE STREAM (a recover() inside the
     * pipeline is the per-element net). For an ingestion loop — Kafka, SSE, a
     * batch import — that is one poison message stopping the consumer: see
     * {@link #pipeResilient}.
     *
     * @throws IllegalArgumentException if concurrency is below 1 — at build
     *         time, not at the first element
     */
    <R> Function<Flux<I>, Flux<R>> pipe(int concurrency,
                                        BiFunction<I, ReactiveStep<I, O>, ReactiveStep<R, O>> pipeline);

    /**
     * Same, plus Reactor's {@code prefetch}: how many elements the operator
     * requests upstream ahead of the pipeline. Lower it when the source is eager
     * and the pipeline is slow, and the default buffer holds more than you want
     * in memory.
     */
    <R> Function<Flux<I>, Flux<R>> pipe(int concurrency, int prefetch,
                                        BiFunction<I, ReactiveStep<I, O>, ReactiveStep<R, O>> pipeline);

    /** Same as pipe, but the output order matches the input order (flatMapSequential). */
    <R> Function<Flux<I>, Flux<R>> pipeOrdered(int concurrency,
                                               BiFunction<I, ReactiveStep<I, O>, ReactiveStep<R, O>> pipeline);

    /** Ordered, with the prefetch — see {@link #pipe(int, int, BiFunction)}. */
    <R> Function<Flux<I>, Flux<R>> pipeOrdered(int concurrency, int prefetch,
                                               BiFunction<I, ReactiveStep<I, O>, ReactiveStep<R, O>> pipeline);

    /**
     * Like {@link #pipe}, but a failing element does not kill the stream: it is
     * handed to {@code onElementError} and dropped, and the Flux carries on with
     * the rest. This is the ingestion-loop shape — one poison message must not
     * stop the consumer.
     *
     * <p>The handler is a parameter and not an overload on purpose: dropping an
     * element is a decision, so you cannot make it without being handed what you
     * dropped. The engine's own {@code onError} handlers still see the failure
     * (once) — this is the stream's net, not a replacement for the flow's.
     */
    <R> Function<Flux<I>, Flux<R>> pipeResilient(int concurrency,
                                                 BiFunction<I, ReactiveStep<I, O>, ReactiveStep<R, O>> pipeline,
                                                 BiConsumer<I, Throwable> onElementError);

    // ── pipe over a PREBUILT pipeline (RFC 0014): build once, execute per element ──

    /**
     * The {@code pipe} an ingestion loop wants: the pipeline is declared ONCE
     * with {@link #pipeline(Segment)} — recorded, validated and compiled a single
     * time — and each element only executes it, off the plan. On the path built
     * for millions of messages this removes the per-element assembly the
     * {@link #pipe(int, BiFunction)} form pays (a fresh pipeline, every step
     * re-wrapped, the chain copied and interpreted).
     *
     * <p>Use this when the pipeline is the same for every element (the usual
     * case); keep the {@code BiFunction} form for a pipeline that genuinely varies
     * element to element. Same semantics otherwise: unordered, {@code concurrency}
     * in flight, one failing element fails the stream (see {@link #pipeResilient}).
     */
    <R> Function<Flux<I>, Flux<R>> pipe(int concurrency, Pipeline<I, R> pipeline);

    /** Same, plus Reactor's prefetch — see {@link #pipe(int, int, BiFunction)}. */
    <R> Function<Flux<I>, Flux<R>> pipe(int concurrency, int prefetch, Pipeline<I, R> pipeline);

    /** Ordered (flatMapSequential) over a prebuilt pipeline. */
    <R> Function<Flux<I>, Flux<R>> pipeOrdered(int concurrency, Pipeline<I, R> pipeline);

    /** Ordered, with the prefetch, over a prebuilt pipeline. */
    <R> Function<Flux<I>, Flux<R>> pipeOrdered(int concurrency, int prefetch, Pipeline<I, R> pipeline);

    /**
     * Resilient pipe over a prebuilt pipeline: a failing element is handed to
     * {@code onElementError} and dropped, the stream carries on. The handler is
     * mandatory for the same reason as in {@link #pipeResilient(int, BiFunction,
     * BiConsumer)} — dropping an element must be a decision.
     */
    <R> Function<Flux<I>, Flux<R>> pipeResilient(int concurrency, Pipeline<I, R> pipeline,
                                                 BiConsumer<I, Throwable> onElementError);

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
    ReactiveFlow<I, O> handleAsync(String name, Function<I, CompletionStage<I>> call);

    @Override
    ReactiveFlow<I, O> handleAsync(String name, Function<I, CompletionStage<I>> call, Duration timeout);

    @Override
    ReactiveFlow<I, O> handleAsync(String name, Function<I, CompletionStage<I>> call, Retry retry);

    @Override
    ReactiveFlow<I, O> handleAsync(String name, Function<I, CompletionStage<I>> call,
                                   Duration timeout, Retry retry);

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
    <R> ReactiveFlow<I, O> fanOutAsync(List<Function<I, CompletionStage<R>>> branches, Function<List<R>, I> join);

    @Override
    <R> ReactiveFlow<I, O> fanOutAsync(String name, List<Function<I, CompletionStage<R>>> branches,
                                       Function<List<R>, I> join);

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
