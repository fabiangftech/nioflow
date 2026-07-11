package dev.nioflow.core.model;

/**
 * Engine-level signals carried by raw call() futures. FILTERED marks an
 * execution deliberately cut by a Filter link: engine exits (await, complete
 * handlers) and flow-level execute()/executeAsync() map it to null; use
 * executeResult() to observe the distinction.
 */
public enum FlowSignal {

    FILTERED
}
