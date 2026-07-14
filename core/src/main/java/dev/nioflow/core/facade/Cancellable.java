package dev.nioflow.core.facade;

import java.util.concurrent.CompletableFuture;

/**
 * A running execution plus the handle to stop it — what
 * {@link NioStep#executeCancellable()} hands back when the caller may need to
 * abandon the request (a disconnected HTTP client, a disposed Mono, a lost
 * websocket).
 *
 * <p>No Reactor in the signature on purpose: an HTTP server on virtual threads
 * has exactly the same problem, and cancellation is a core concern.
 *
 * <p><b>Cancellation is cooperative, not preemptive.</b> {@link #cancel()}
 * stops the chain from advancing — no further stage is invoked — and cancels
 * the in-flight call of an async stage (handleAsync/adaptAsync/handleMonoAsync:
 * the CompletionStage is cancelled, so the subscription dies and the connection
 * is released). It does NOT interrupt a blocking stage already running on a
 * worker: that call runs to its end and its result is discarded. Nor does it
 * touch a fork already spawned — a fork is detached by definition.
 *
 * <p>So the guarantee is precise: the card is not charged because {@code charge}
 * is the stage that never gets invoked.
 */
public interface Cancellable<T> {

    /**
     * The result of the execution. On cancellation it completes exceptionally
     * with a {@link java.util.concurrent.CancellationException} — never with
     * null, which could not be told apart from a Filter cut.
     */
    CompletableFuture<T> future();

    /**
     * Stops the execution. Idempotent: cancelling twice, or cancelling one that
     * already finished, does nothing. Safe from any thread — the terminal is
     * handed to the execution's boss, so the engine's serialization rule holds.
     */
    void cancel();
}
