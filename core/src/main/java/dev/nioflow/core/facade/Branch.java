package dev.nioflow.core.facade;

import java.util.function.UnaryOperator;

public interface Branch<T> extends NioFlow<T> {

    NioFlow<T> otherwise(UnaryOperator<NioFlow<T>> lane);
}
