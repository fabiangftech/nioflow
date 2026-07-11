package dev.nioflow.core.facade;

import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public interface Cases<T> extends NioFlow<T> {

    Cases<T> is(Predicate<T> predicate, UnaryOperator<NioFlow<T>> lane);

    NioFlow<T> otherwise(UnaryOperator<NioFlow<T>> lane);
}
