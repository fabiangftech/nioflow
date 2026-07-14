package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.StepCondition;

import java.util.function.UnaryOperator;

/** Per-request when(); see ReactiveCondition. */
public interface ReactiveStepCondition<T, O> extends StepCondition<T, O> {

    @Override
    ReactiveStepBranch<T, O> then(UnaryOperator<Lane<T>> lane);
}
