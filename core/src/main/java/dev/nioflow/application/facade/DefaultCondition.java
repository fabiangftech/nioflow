package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Branch;
import dev.nioflow.core.facade.Condition;
import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.model.Guard;

import java.util.function.UnaryOperator;

final class DefaultCondition<I, O> implements Condition<I, O> {

    private final DefaultNioFlow<I, O> flow;
    private final int decision;

    DefaultCondition(DefaultNioFlow<I, O> flow, int decision) {
        this.flow = flow;
        this.decision = decision;
    }

    @Override
    public Branch<I, O> then(UnaryOperator<Lane<I>> lane) {
        lane.apply(new DefaultLane<>(flow.withGuards(
                AbstractChain.withGuard(flow.guards(), new Guard(decision, true)))));
        return new DefaultBranch<>(flow, decision);
    }
}
