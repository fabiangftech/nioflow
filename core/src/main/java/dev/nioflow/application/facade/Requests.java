package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Cancellable;
import dev.nioflow.core.model.FlowSignal;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

/**
 * The terminal machinery shared by the two per-request runners — the dynamic
 * {@link ExecutionNioFlow} (a {@code just()} pipeline) and the prebuilt
 * {@link DefaultPipelineRun} (a {@link dev.nioflow.core.facade.Pipeline}). Both
 * hand the engine an input and turn the raw result future into the caller's
 * terms; the only thing that differs between them is the chain they dispatch
 * off, so everything from the sentinel mapping onward lives here, once.
 */
final class Requests {

    private Requests() {
    }

    /**
     * The engine's sentinels in the caller's terms. A cancelled execution
     * THROWS rather than yielding null: a null here would be indistinguishable
     * from a Filter cut, and "the client hung up" is not "the value was cut".
     */
    @SuppressWarnings("unchecked")
    static <T> T typed(Object value) {
        if (value == FlowSignal.CANCELLED) {
            throw new CancellationException("Execution was cancelled");
        }
        return value == FlowSignal.FILTERED ? null : (T) value;
    }

    static Throwable unwrap(Throwable error) {
        return error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
    }

    /**
     * The context ONE run starts from: a fresh map every time, because a
     * terminal may be called more than once (a Mono re-subscribing, a retry)
     * and each run must start from what was seeded rather than inherit what the
     * previous run wrote. The run's own entries go in first and the pipeline's
     * seed wins over them; neither present means no map at all.
     */
    static Map<String, Object> mergeContext(Map<String, Object> seed, Map<String, Object> runContext) {
        if (runContext == null || runContext.isEmpty()) {
            return seed == null ? null : new HashMap<>(seed);
        }
        Map<String, Object> context = new HashMap<>(runContext);
        if (seed != null) {
            context.putAll(seed);
        }
        return context;
    }

    /** Hangs the execution-scoped onComplete/onError off the raw future, if any. */
    static CompletableFuture<Object> decorate(CompletableFuture<Object> raw,
                                              Consumer<Object> onComplete, Consumer<Throwable> onError) {
        if (onComplete == null && onError == null) {
            // Pay for what you use: no callbacks, no dependent future.
            return raw;
        }
        // execute()/executeAsync() join/compose on the DEPENDENT future, so
        // execution-scoped callbacks are guaranteed done before the caller
        // observes the result — same ordering the engine handlers give.
        return raw.whenComplete((value, error) -> {
            if (error != null) {
                if (onError != null) {
                    onError.accept(unwrap(error));
                }
            } else if (onComplete != null && value != FlowSignal.CANCELLED) {
                // Same rule the engine's complete handlers follow: a cancelled
                // execution has no value to hand anyone.
                onComplete.accept(value == FlowSignal.FILTERED ? null : value);
            }
        });
    }

    /**
     * The typed cancellable handle: the caller's future, the engine's cancel.
     * Built by hand rather than with thenApply, because the caller must observe
     * a CancellationException, not a CompletionException wrapping one — this is
     * the future a Mono and an HTTP handler hang their cancellation on, and the
     * exception type is the contract.
     */
    @SuppressWarnings("unchecked")
    static <T> Cancellable<T> cancellable(Cancellable<Object> raw,
                                          Consumer<Object> onComplete, Consumer<Throwable> onError) {
        CompletableFuture<T> typed = new CompletableFuture<>();
        decorate(raw.future(), onComplete, onError).whenComplete((value, error) -> {
            if (error != null) {
                typed.completeExceptionally(unwrap(error));
            } else if (value == FlowSignal.CANCELLED) {
                typed.completeExceptionally(new CancellationException("Execution was cancelled"));
            } else {
                typed.complete(value == FlowSignal.FILTERED ? null : (T) value);
            }
        });
        return new Handle<>(typed, raw);
    }

    private record Handle<T>(CompletableFuture<T> future, Cancellable<Object> raw) implements Cancellable<T> {

        @Override
        public void cancel() {
            raw.cancel();
        }
    }
}
