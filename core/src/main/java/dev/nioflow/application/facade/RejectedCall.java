package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Cancellable;

import java.util.concurrent.CompletableFuture;

/**
 * A call the shut-down engine never started: there is nothing to cancel (RFC
 * 0032, extracted from {@code DefaultNioEngine}). The future is already failed.
 */
record RejectedCall(CompletableFuture<Object> future) implements Cancellable<Object> {

    @Override
    public void cancel() {
        // Nothing ran; the future is already failed.
    }
}
