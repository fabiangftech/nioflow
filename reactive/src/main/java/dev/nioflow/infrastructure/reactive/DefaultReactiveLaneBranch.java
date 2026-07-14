package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.LaneBranch;

import java.time.Duration;
import java.util.function.UnaryOperator;

class DefaultReactiveLaneBranch<T> extends DefaultReactiveLane<T> implements ReactiveLaneBranch<T> {

    private final LaneBranch<T> branch;

    DefaultReactiveLaneBranch(LaneBranch<T> branch, Duration budget) {
        super(branch, budget);
        this.branch = branch;
    }

    @Override
    public ReactiveLane<T> otherwise(UnaryOperator<Lane<T>> lane) {
        return new DefaultReactiveLane<>(branch.otherwise(Lanes.budgeted(lane, budget)), budget);
    }
}
