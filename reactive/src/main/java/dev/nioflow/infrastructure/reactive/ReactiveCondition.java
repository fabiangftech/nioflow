package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Condition;
import dev.nioflow.core.facade.Lane;

import java.util.function.UnaryOperator;

/**
 * when() on a ReactiveFlow. The lane lambda still receives a plain Lane — the
 * erasure clash documented on {@link ReactiveLane} — so unwrap it with
 * {@link Reactive#lane(Lane)}. What this buys is the RETURN type: chaining
 * after the fork stays reactive.
 */
public interface ReactiveCondition<I, O> extends Condition<I, O> {

    @Override
    ReactiveBranch<I, O> then(UnaryOperator<Lane<I>> lane);
}
