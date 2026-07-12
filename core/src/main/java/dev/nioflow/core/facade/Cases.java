package dev.nioflow.core.facade;

import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/** match() on the shared definition: first-match-wins, lanes type-preserving over I. */
public interface Cases<I, O> extends NioFlow<I, O> {

    Cases<I, O> is(Predicate<I> predicate, UnaryOperator<Lane<I>> lane);

    NioFlow<I, O> otherwise(UnaryOperator<Lane<I>> lane);
}
