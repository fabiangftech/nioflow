package dev.nioflow.core.facade;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * One request over a prebuilt {@link Pipeline}: the small handle
 * {@link Pipeline#just(Object)} hands back. It has no building steps — the
 * pipeline's structure is fixed — only what a single request configures and the
 * terminals that run it.
 *
 * <p>{@code R} is the value the pipeline leaves; the terminals return it (or its
 * future). The terminals mirror {@link NioStep}'s exactly, and so does their
 * treatment of a Filter cut (mapped to {@code null} by {@code execute}, reported
 * by {@code executeResult}) and of cancellation.
 */
public interface PipelineRun<R> {

    /**
     * Orders this request by business key, Kafka-partition style: requests
     * sharing a key run strictly one at a time, in submission order, pinned to
     * the same boss; distinct keys keep full parallelism.
     */
    PipelineRun<R> key(Object key);

    /** Seeds the per-execution {@link Context} before the pipeline runs; see NioStep#with. */
    <V> PipelineRun<R> with(Context.Key<V> key, V value);

    /** Scoped to THIS request; fires before execute() returns. */
    PipelineRun<R> onComplete(Consumer<R> callback);

    PipelineRun<R> onError(Consumer<Throwable> callback);

    /** Runs the request and blocks until the result is ready. */
    R execute();

    /** Runs the request and returns immediately with the promise of the result. */
    CompletableFuture<R> executeAsync();

    /** Same, with the context of THIS run — see NioStep#executeAsync(Map). */
    CompletableFuture<R> executeAsync(Map<String, Object> context);

    /** Like execute(), but distinguishes a Filter cut and a cancellation from a value. */
    FlowResult<R> executeResult();

    /** Runs the request and hands back the handle to stop it — see NioStep#executeCancellable. */
    Cancellable<R> executeCancellable();

    /** Same, with the context of THIS run. */
    Cancellable<R> executeCancellable(Map<String, Object> context);
}
