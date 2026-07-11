package dev.nioflow.core.facade;

import java.util.function.UnaryOperator;

public interface LaneBranch<T> extends Lane<T> {

    Lane<T> otherwise(UnaryOperator<Lane<T>> lane);
}
