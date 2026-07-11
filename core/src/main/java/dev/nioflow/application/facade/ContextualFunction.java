package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Context;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Build-time bridge for context-aware stages: the fluent API wraps the
 * user's BiFunction in this marker so the Stage model keeps its single
 * Function field, and the engine — the only caller — unwraps it and
 * supplies the execution's Context at apply time. Plain stages skip the
 * whole mechanism (one instanceof at the apply point).
 */
record ContextualFunction(BiFunction<Object, Context, Object> body) implements Function<Object, Object> {

    @Override
    public Object apply(Object value) {
        throw new IllegalStateException("Contextual stages run through the engine, which supplies the Context");
    }
}
