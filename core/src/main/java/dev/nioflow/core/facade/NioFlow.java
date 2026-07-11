package dev.nioflow.core.facade;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * End-to-end typed pipeline: I is the input type (what just accepts), T is
 * the value's type at the current point of the chain. adapt() is the only
 * step that changes T; everything else preserves it. Lifecycle (close) is
 * NOT part of this contract: it belongs to the root implementation that owns
 * the engine, never to branches or executions.
 */
public interface NioFlow<I, T> {

    NioFlow<I, T> just(I input);

    NioFlow<I, T> justAll(Iterable<I> inputs);

    NioFlow<I, T> handle(Function<T, T> function);

    NioFlow<I, T> handle(String name, Function<T, T> function);

    /**
     * Stage with a time budget: if the function does not finish within the
     * timeout, the value fails with a TimeoutException — catchable downstream
     * by recover(), like any other stage failure.
     */
    NioFlow<I, T> handle(String name, Function<T, T> function, Duration timeout);

    NioFlow<I, T> background(Consumer<T> effect);

    NioFlow<I, T> background(String name, Consumer<T> effect);

    <R> NioFlow<I, R> adapt(Function<T, R> function);

    /**
     * Parallel split-join: every branch receives the current value and runs
     * concurrently on the workers; join combines the branch results (in
     * declaration order) into the value that continues down the chain. Any
     * branch failure fails the fan-out — recoverable downstream.
     */
    <R, C> NioFlow<I, C> fanOut(List<Function<T, R>> branches, Function<List<R>, C> join);

    <R, C> NioFlow<I, C> fanOut(String name, List<Function<T, R>> branches, Function<List<R>, C> join);

    NioFlow<I, T> filter(Predicate<T> predicate);

    /**
     * Positional error handling: catches failures (exceptions and stage
     * timeouts) from links declared upstream of it, and the flow continues
     * after it with the recovered value. Failures downstream are not caught.
     */
    NioFlow<I, T> recover(Function<Throwable, T> function);

    NioFlow<I, T> recover(String name, Function<Throwable, T> function);

    Condition<I, T> when(Predicate<T> predicate);

    Cases<I, T> match();

    /**
     * Runs the execution and blocks the caller until the result is ready.
     * Equivalent to executeAsync().join().
     */
    T execute();

    /**
     * Runs the execution and returns immediately with the promise of the
     * result — the caller's thread never blocks. Returning this future from
     * a Spring controller yields a non-blocking endpoint. It completes
     * exceptionally with the terminal failure when no recover() caught it.
     */
    CompletableFuture<T> executeAsync();
}
