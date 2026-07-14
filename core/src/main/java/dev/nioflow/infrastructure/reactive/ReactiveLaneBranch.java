package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.LaneBranch;

import java.util.function.UnaryOperator;

public interface ReactiveLaneBranch<T> extends LaneBranch<T>, ReactiveLane<T> {

    @Override
    ReactiveLane<T> otherwise(UnaryOperator<Lane<T>> lane);
}
