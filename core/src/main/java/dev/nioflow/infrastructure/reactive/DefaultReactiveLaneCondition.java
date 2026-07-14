package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.LaneCondition;

import java.time.Duration;
import java.util.function.UnaryOperator;

class DefaultReactiveLaneCondition<T> implements ReactiveLaneCondition<T> {

    private final LaneCondition<T> delegate;
    private final Duration budget;

    DefaultReactiveLaneCondition(LaneCondition<T> delegate, Duration budget) {
        this.delegate = delegate;
        this.budget = budget;
    }

    @Override
    public ReactiveLaneBranch<T> then(UnaryOperator<Lane<T>> lane) {
        return new DefaultReactiveLaneBranch<>(delegate.then(Lanes.budgeted(lane, budget)), budget);
    }
}
