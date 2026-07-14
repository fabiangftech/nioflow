package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.StepCondition;

import java.util.function.UnaryOperator;

class DefaultReactiveStepCondition<T, O> implements ReactiveStepCondition<T, O> {

    private final StepCondition<T, O> delegate;

    DefaultReactiveStepCondition(StepCondition<T, O> delegate) {
        this.delegate = delegate;
    }

    @Override
    public ReactiveStepBranch<T, O> then(UnaryOperator<Lane<T>> lane) {
        return new DefaultReactiveStepBranch<>(delegate.then(lane));
    }
}
