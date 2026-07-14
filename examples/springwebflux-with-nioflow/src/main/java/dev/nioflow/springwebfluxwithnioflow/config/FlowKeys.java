package dev.nioflow.springwebfluxwithnioflow.config;

import dev.nioflow.core.facade.Context;

/**
 * The context keys this application bridges, in one place — because they are
 * named in three: the WebFilter that writes them into Reactor's subscriber
 * context, the flow that {@code propagate}s them, and the stages that read them.
 *
 * <p>{@code Context.Key} is name-based, and the name IS the correspondence: the
 * subscriber-context entry {@code "traceId"} becomes the per-execution context
 * entry {@code "traceId"}. Nothing else about the two worlds has to line up.
 */
public final class FlowKeys {

    public static final Context.Key<String> TRACE = Context.Key.of("traceId");

    private FlowKeys() {
    }
}
