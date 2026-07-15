package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.StepCondition;

import java.util.function.UnaryOperator;

class DefaultReactiveStepCondition<T, O> implements ReactiveStepCondition<T, O> {

    private final StepCondition<T, O> delegate;
    private final ReactiveConfig config;

    DefaultReactiveStepCondition(StepCondition<T, O> delegate, ReactiveConfig config) {
        this.delegate = delegate;
        this.config = config;
    }

    @Override
    public ReactiveStepBranch<T, O> then(UnaryOperator<Lane<T>> lane) {
        return new DefaultReactiveStepBranch<>(delegate.then(Lanes.budgeted(lane, config.budget(), config.preferAsync())), config);
    }
}
