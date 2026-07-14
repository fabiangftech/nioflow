package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.LaneCases;

import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public interface ReactiveLaneCases<T> extends LaneCases<T>, ReactiveLane<T> {

    @Override
    ReactiveLaneCases<T> is(Predicate<T> predicate, UnaryOperator<Lane<T>> lane);

    @Override
    ReactiveLane<T> otherwise(UnaryOperator<Lane<T>> lane);
}
