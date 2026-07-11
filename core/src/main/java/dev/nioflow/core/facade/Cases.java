package dev.nioflow.core.facade;

import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public interface Cases<I, T> extends NioFlow<I, T> {

    Cases<I, T> is(Predicate<T> predicate, UnaryOperator<Lane<T>> lane);

    NioFlow<I, T> otherwise(UnaryOperator<Lane<T>> lane);
}
