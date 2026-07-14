package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.LaneCondition;

import java.util.function.UnaryOperator;

/** Nested when() inside a lane; see ReactiveCondition. */
public interface ReactiveLaneCondition<T> extends LaneCondition<T> {

    @Override
    ReactiveLaneBranch<T> then(UnaryOperator<Lane<T>> lane);
}
