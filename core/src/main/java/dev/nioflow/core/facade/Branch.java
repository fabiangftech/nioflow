package dev.nioflow.core.facade;

import java.util.function.UnaryOperator;

public interface Branch extends NioFlow {

    <T> NioFlow otherwise(UnaryOperator<NioFlow> lane);
}
