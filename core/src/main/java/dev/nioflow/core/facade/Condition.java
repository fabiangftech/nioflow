package dev.nioflow.core.facade;

import java.util.function.UnaryOperator;

/** when() on the shared definition: its lanes are type-preserving over I. */
public interface Condition<I, O> {

    Branch<I, O> then(UnaryOperator<Lane<I>> lane);
}
