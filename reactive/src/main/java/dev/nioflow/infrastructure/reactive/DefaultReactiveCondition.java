package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Condition;
import dev.nioflow.core.facade.Lane;

import java.util.function.UnaryOperator;

class DefaultReactiveCondition<I, O> implements ReactiveCondition<I, O> {

    private final Condition<I, O> delegate;
    private final ReactiveConfig config;

    DefaultReactiveCondition(Condition<I, O> delegate, ReactiveConfig config) {
        this.delegate = delegate;
        this.config = config;
    }

    @Override
    public ReactiveBranch<I, O> then(UnaryOperator<Lane<I>> lane) {
        return new DefaultReactiveBranch<>(delegate.then(Lanes.budgeted(lane, config.budget(), config.preferAsync())), config);
    }
}
