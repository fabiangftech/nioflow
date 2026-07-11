package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Branch;
import dev.nioflow.core.facade.Condition;
import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.model.Guard;

import java.util.function.UnaryOperator;

final class DefaultCondition<I, T> implements Condition<I, T> {

    private final AbstractNioFlow<I, T> flow;
    private final int decision;

    DefaultCondition(AbstractNioFlow<I, T> flow, int decision) {
        this.flow = flow;
        this.decision = decision;
    }

    @Override
    public Branch<I, T> then(UnaryOperator<Lane<T>> lane) {
        lane.apply(new DefaultLane<>(flow.withGuards(
                AbstractNioFlow.withGuard(flow.guards(), new Guard(decision, true)))));
        return new DefaultBranch<>(flow, decision);
    }
}
