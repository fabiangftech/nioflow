package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.LaneCondition;

import java.util.function.UnaryOperator;

class DefaultReactiveLaneCondition<T> implements ReactiveLaneCondition<T> {

    private final LaneCondition<T> delegate;

    DefaultReactiveLaneCondition(LaneCondition<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public ReactiveLaneBranch<T> then(UnaryOperator<Lane<T>> lane) {
        return new DefaultReactiveLaneBranch<>(delegate.then(lane));
    }
}
