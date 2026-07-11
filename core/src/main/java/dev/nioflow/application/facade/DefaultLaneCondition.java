package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.LaneBranch;
import dev.nioflow.core.facade.LaneCondition;
import dev.nioflow.core.model.Guard;

import java.util.function.UnaryOperator;

final class DefaultLaneCondition<T> implements LaneCondition<T> {

    private final AbstractNioFlow<?, T> view;
    private final int decision;

    DefaultLaneCondition(AbstractNioFlow<?, T> view, int decision) {
        this.view = view;
        this.decision = decision;
    }

    @Override
    public LaneBranch<T> then(UnaryOperator<Lane<T>> lane) {
        lane.apply(new DefaultLane<>(view.withGuards(
                AbstractNioFlow.withGuard(view.guards(), new Guard(decision, true)))));
        return new DefaultLaneBranch<>(view, decision);
    }
}
