package dev.nioflow.core.facade;

import java.util.function.UnaryOperator;

public interface Branch<I, T> extends NioFlow<I, T> {

    NioFlow<I, T> otherwise(UnaryOperator<NioFlow<I, T>> lane);
}
