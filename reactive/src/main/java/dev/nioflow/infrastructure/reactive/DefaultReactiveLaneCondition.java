package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.LaneCondition;

import java.util.function.UnaryOperator;

class DefaultReactiveLaneCondition<T> implements ReactiveLaneCondition<T> {

    private final LaneCondition<T> delegate;
    private final ReactiveConfig config;

    DefaultReactiveLaneCondition(LaneCondition<T> delegate, ReactiveConfig config) {
        this.delegate = delegate;
        this.config = config;
    }

    @Override
    public ReactiveLaneBranch<T> then(UnaryOperator<Lane<T>> lane) {
        return new DefaultReactiveLaneBranch<>(delegate.then(Lanes.budgeted(lane, config)), config);
    }
}
