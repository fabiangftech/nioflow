package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.StepCondition;

import java.time.Duration;
import java.util.function.UnaryOperator;

class DefaultReactiveStepCondition<T, O> implements ReactiveStepCondition<T, O> {

    private final StepCondition<T, O> delegate;
    private final Duration budget;

    DefaultReactiveStepCondition(StepCondition<T, O> delegate, Duration budget) {
        this.delegate = delegate;
        this.budget = budget;
    }

    @Override
    public ReactiveStepBranch<T, O> then(UnaryOperator<Lane<T>> lane) {
        return new DefaultReactiveStepBranch<>(delegate.then(Lanes.budgeted(lane, budget)), budget);
    }
}
