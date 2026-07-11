package dev.nioflow.core.facade;

import java.util.function.UnaryOperator;

public interface LaneCondition<T> {

    LaneBranch<T> then(UnaryOperator<Lane<T>> lane);
}
