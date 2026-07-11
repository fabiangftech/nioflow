package dev.nioflow.core.facade;

import java.util.function.UnaryOperator;

public interface Condition<I, T> {

    Branch<I, T> then(UnaryOperator<NioFlow<I, T>> lane);
}
