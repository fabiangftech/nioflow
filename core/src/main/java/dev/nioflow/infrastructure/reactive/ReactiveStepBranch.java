package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.StepBranch;

import java.util.function.UnaryOperator;

public interface ReactiveStepBranch<T, O> extends StepBranch<T, O>, ReactiveStep<T, O> {

    @Override
    ReactiveStep<T, O> otherwise(UnaryOperator<Lane<T>> lane);
}
