package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Condition;
import dev.nioflow.core.facade.Lane;

import java.util.function.UnaryOperator;

class DefaultReactiveCondition<I, O> implements ReactiveCondition<I, O> {

    private final Condition<I, O> delegate;

    DefaultReactiveCondition(Condition<I, O> delegate) {
        this.delegate = delegate;
    }

    @Override
    public ReactiveBranch<I, O> then(UnaryOperator<Lane<I>> lane) {
        return new DefaultReactiveBranch<>(delegate.then(lane));
    }
}
