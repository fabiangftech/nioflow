package dev.nioflow.core.facade;

import java.util.function.UnaryOperator;

/** when() inside a per-request pipeline: lanes run over the value's current type. */
public interface StepCondition<T, O> {

    StepBranch<T, O> then(UnaryOperator<Lane<T>> lane);
}
