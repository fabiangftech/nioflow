package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Cases;
import dev.nioflow.core.facade.Lane;

import java.time.Duration;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

class DefaultReactiveCases<I, O> extends DefaultReactiveFlow<I, O> implements ReactiveCases<I, O> {

    private final Cases<I, O> cases;

    DefaultReactiveCases(Cases<I, O> cases, Duration budget) {
        super(cases, budget);
        this.cases = cases;
    }

    @Override
    public ReactiveCases<I, O> is(Predicate<I> predicate, UnaryOperator<Lane<I>> lane) {
        return new DefaultReactiveCases<>(cases.is(predicate, Lanes.budgeted(lane, budget)), budget);
    }

    @Override
    public ReactiveFlow<I, O> otherwise(UnaryOperator<Lane<I>> lane) {
        return new DefaultReactiveFlow<>(cases.otherwise(Lanes.budgeted(lane, budget)), budget);
    }
}
