package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Branch;
import dev.nioflow.core.facade.Lane;

import java.util.function.UnaryOperator;

/** Chaining after when().then() returns to the main line — still reactive. */
public interface ReactiveBranch<I, O> extends Branch<I, O>, ReactiveFlow<I, O> {

    @Override
    ReactiveFlow<I, O> otherwise(UnaryOperator<Lane<I>> lane);
}
