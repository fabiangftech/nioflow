package dev.nioflow.core.facade;

import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/** match() inside a per-request pipeline: first-match-wins over the current type. */
public interface StepCases<T, O> extends NioStep<T, O> {

    StepCases<T, O> is(Predicate<T> predicate, UnaryOperator<Lane<T>> lane);

    NioStep<T, O> otherwise(UnaryOperator<Lane<T>> lane);
}
