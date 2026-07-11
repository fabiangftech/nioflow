package dev.nioflow.core.facade;

import java.util.function.UnaryOperator;

public interface Condition<T> {

    Branch<T> then(UnaryOperator<NioFlow<T>> lane);
}
