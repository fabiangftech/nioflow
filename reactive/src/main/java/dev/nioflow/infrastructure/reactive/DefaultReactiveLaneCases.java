package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.LaneCases;

import java.util.function.Predicate;
import java.util.function.UnaryOperator;

class DefaultReactiveLaneCases<T> extends DefaultReactiveLane<T> implements ReactiveLaneCases<T> {

    private final LaneCases<T> cases;

    DefaultReactiveLaneCases(LaneCases<T> cases, ReactiveConfig config) {
        super(cases, config);
        this.cases = cases;
    }

    @Override
    public ReactiveLaneCases<T> is(Predicate<T> predicate, UnaryOperator<Lane<T>> lane) {
        return new DefaultReactiveLaneCases<>(cases.is(predicate, Lanes.budgeted(lane, config)), config);
    }

    @Override
    public ReactiveLane<T> otherwise(UnaryOperator<Lane<T>> lane) {
        return new DefaultReactiveLane<>(cases.otherwise(Lanes.budgeted(lane, config)), config);
    }
}
