package dev.nioflow.core.facade;

import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public interface Cases extends NioFlow {

    <T> Cases is(Predicate<T> predicate, UnaryOperator<NioFlow> lane);

    NioFlow otherwise(UnaryOperator<NioFlow> lane);
}
