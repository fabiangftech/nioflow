package dev.nioflow.core.facade;

/**
 * Outcome of an execution that distinguishes a deliberate Filter cut from a
 * completed value — including a genuinely null one. Pattern-match it:
 *
 * <pre>
 * switch (flow.just(input).executeResult()) {
 *     case Completed(var value) -> ...
 *     case Filtered() -> ...
 * }
 * </pre>
 */
public sealed interface FlowResult<T> permits FlowResult.Completed, FlowResult.Filtered {

    record Completed<T>(T value) implements FlowResult<T> {
    }

    record Filtered<T>() implements FlowResult<T> {
    }

    default boolean filtered() {
        return this instanceof Filtered<T>;
    }

    default T orElse(T fallback) {
        return this instanceof Completed<T>(T value) ? value : fallback;
    }
}
