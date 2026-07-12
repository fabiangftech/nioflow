package dev.nioflow.core.facade;

import java.util.function.UnaryOperator;

public interface StepBranch<T, O> extends NioStep<T, O> {

    NioStep<T, O> otherwise(UnaryOperator<Lane<T>> lane);
}
