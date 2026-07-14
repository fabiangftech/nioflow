package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.StepCases;

import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public interface ReactiveStepCases<T, O> extends StepCases<T, O>, ReactiveStep<T, O> {

    @Override
    ReactiveStepCases<T, O> is(Predicate<T> predicate, UnaryOperator<Lane<T>> lane);

    @Override
    ReactiveStep<T, O> otherwise(UnaryOperator<Lane<T>> lane);
}
