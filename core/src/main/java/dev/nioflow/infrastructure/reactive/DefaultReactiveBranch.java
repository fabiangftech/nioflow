package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Branch;
import dev.nioflow.core.facade.Lane;

import java.time.Duration;
import java.util.function.UnaryOperator;

class DefaultReactiveBranch<I, O> extends DefaultReactiveFlow<I, O> implements ReactiveBranch<I, O> {

    private final Branch<I, O> branch;

    DefaultReactiveBranch(Branch<I, O> branch, Duration budget) {
        super(branch, budget);
        this.branch = branch;
    }

    @Override
    public ReactiveFlow<I, O> otherwise(UnaryOperator<Lane<I>> lane) {
        return new DefaultReactiveFlow<>(branch.otherwise(Lanes.budgeted(lane, budget)), budget);
    }
}
