package dev.nioflow.core.model;

/**
 * Engine-level signals carried by raw call() futures. FILTERED marks an
 * execution deliberately cut by a Filter link: engine exits (await, complete
 * handlers) and flow-level execute()/executeAsync() map it to null; use
 * executeResult() to observe the distinction.
 *
 * <p>CANCELLED marks an execution stopped from the outside (see
 * NioStep.executeCancellable). It travels the same door FILTERED does — the
 * terminal completes the raw future with the sentinel — so cancellation needs
 * no second completion path: the drain slot is released, the metrics fire
 * (executionCancelled) and the key lane is handed on, exactly as for any other
 * ending. What it does NOT do is reach the complete handlers: a cancelled
 * execution has no output value, and those handlers promise an O.
 */
public enum FlowSignal {

    FILTERED,
    CANCELLED
}
