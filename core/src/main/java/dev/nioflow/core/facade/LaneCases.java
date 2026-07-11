package dev.nioflow.core.facade;

import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public interface LaneCases<T> extends Lane<T> {

    LaneCases<T> is(Predicate<T> predicate, UnaryOperator<Lane<T>> lane);

    Lane<T> otherwise(UnaryOperator<Lane<T>> lane);
}
