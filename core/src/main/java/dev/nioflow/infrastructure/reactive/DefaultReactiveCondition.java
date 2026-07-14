package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Condition;
import dev.nioflow.core.facade.Lane;

import java.time.Duration;
import java.util.function.UnaryOperator;

class DefaultReactiveCondition<I, O> implements ReactiveCondition<I, O> {

    private final Condition<I, O> delegate;
    private final Duration budget;

    DefaultReactiveCondition(Condition<I, O> delegate, Duration budget) {
        this.delegate = delegate;
        this.budget = budget;
    }

    @Override
    public ReactiveBranch<I, O> then(UnaryOperator<Lane<I>> lane) {
        return new DefaultReactiveBranch<>(delegate.then(Lanes.budgeted(lane, budget)), budget);
    }
}
