package dev.nioflow.core.facade;

import java.util.function.UnaryOperator;

public interface Branch<I, O> extends NioFlow<I, O> {

    NioFlow<I, O> otherwise(UnaryOperator<Lane<I>> lane);
}
