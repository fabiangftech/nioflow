package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.StepBranch;
import dev.nioflow.core.facade.StepCondition;
import dev.nioflow.core.model.Guard;

import java.util.function.UnaryOperator;

final class DefaultStepCondition<T, O> implements StepCondition<T, O> {

    private final ExecutionNioFlow<T, O> step;
    private final int decision;

    DefaultStepCondition(ExecutionNioFlow<T, O> step, int decision) {
        this.step = step;
        this.decision = decision;
    }

    @Override
    public StepBranch<T, O> then(UnaryOperator<Lane<T>> lane) {
        lane.apply(new DefaultLane<>(step.withGuards(
                AbstractChain.withGuard(step.guards(), new Guard(decision, true)))));
        return new DefaultStepBranch<>(step, decision);
    }
}
