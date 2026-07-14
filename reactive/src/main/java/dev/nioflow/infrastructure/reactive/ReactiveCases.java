package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Cases;
import dev.nioflow.core.facade.Lane;

import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/** match() on a ReactiveFlow: first-match-wins, reactive on the way out. */
public interface ReactiveCases<I, O> extends Cases<I, O>, ReactiveFlow<I, O> {

    @Override
    ReactiveCases<I, O> is(Predicate<I> predicate, UnaryOperator<Lane<I>> lane);

    @Override
    ReactiveFlow<I, O> otherwise(UnaryOperator<Lane<I>> lane);
}
