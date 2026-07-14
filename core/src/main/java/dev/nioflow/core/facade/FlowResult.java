package dev.nioflow.core.facade;

/**
 * Outcome of an execution that distinguishes a deliberate Filter cut from a
 * completed value — including a genuinely null one — and both of those from an
 * execution stopped from the outside. Pattern-match it:
 *
 * <pre>
 * switch (flow.just(input).executeResult()) {
 *     case Completed(var value) -> ...
 *     case Filtered() -> ...
 *     case Cancelled() -> ...
 * }
 * </pre>
 */
public sealed interface FlowResult<T>
        permits FlowResult.Completed, FlowResult.Filtered, FlowResult.Cancelled {

    record Completed<T>(T value) implements FlowResult<T> {
    }

    record Filtered<T>() implements FlowResult<T> {
    }

    /**
     * The execution was cancelled (see {@link Cancellable}). A third kind of
     * nothing: not a value, not a cut — a request whose answer nobody is
     * waiting for any more.
     */
    record Cancelled<T>() implements FlowResult<T> {
    }

    default boolean filtered() {
        return this instanceof Filtered<T>;
    }

    default boolean cancelled() {
        return this instanceof Cancelled<T>;
    }

    default T orElse(T fallback) {
        return this instanceof Completed<T>(T value) ? value : fallback;
    }
}
