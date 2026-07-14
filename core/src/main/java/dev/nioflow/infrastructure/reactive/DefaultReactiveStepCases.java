package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.StepCases;

import java.time.Duration;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

class DefaultReactiveStepCases<T, O> extends DefaultReactiveStep<T, O> implements ReactiveStepCases<T, O> {

    private final StepCases<T, O> cases;

    DefaultReactiveStepCases(StepCases<T, O> cases, Duration budget) {
        super(cases, budget);
        this.cases = cases;
    }

    @Override
    public ReactiveStepCases<T, O> is(Predicate<T> predicate, UnaryOperator<Lane<T>> lane) {
        return new DefaultReactiveStepCases<>(cases.is(predicate, Lanes.budgeted(lane, budget)), budget);
    }

    @Override
    public ReactiveStep<T, O> otherwise(UnaryOperator<Lane<T>> lane) {
        return new DefaultReactiveStep<>(cases.otherwise(Lanes.budgeted(lane, budget)), budget);
    }
}
