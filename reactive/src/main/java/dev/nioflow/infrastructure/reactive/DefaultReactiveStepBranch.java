package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.StepBranch;

import java.util.function.UnaryOperator;

class DefaultReactiveStepBranch<T, O> extends DefaultReactiveStep<T, O> implements ReactiveStepBranch<T, O> {

    private final StepBranch<T, O> branch;

    DefaultReactiveStepBranch(StepBranch<T, O> branch, ReactiveConfig config) {
        super(branch, config);
        this.branch = branch;
    }

    @Override
    public ReactiveStep<T, O> otherwise(UnaryOperator<Lane<T>> lane) {
        return new DefaultReactiveStep<>(branch.otherwise(Lanes.budgeted(lane, config)), config);
    }
}
