package dev.nioflow.infrastructure.reactive;

/**
 * A bounded collector was handed more than it agreed to hold — today only
 * {@link ReactiveStep#adaptFlux(java.util.function.Function, int)} and its lane
 * counterpart throw it.
 *
 * <p>It is an ordinary stage failure: it reaches {@code recover()} like any
 * other, and the caller decides. It is its own type precisely so that a
 * {@code recover()} can tell "the stream was bigger than I said it could be"
 * apart from a bug in the user's own code — which is all an
 * IllegalStateException would have said.
 */
public class FlowOverflowException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public FlowOverflowException(String message) {
        super(message);
    }
}
