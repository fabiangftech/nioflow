package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.NioStep;
import dev.nioflow.core.facade.StepBranch;
import dev.nioflow.core.model.Guard;

import java.util.function.UnaryOperator;

/** Per-request when().then(): see DefaultBranch. */
final class DefaultStepBranch<T, O> extends NioStepDelegate<T, O> implements StepBranch<T, O> {

    private final ExecutionNioFlow<T, O> step;
    private final int decision;

    DefaultStepBranch(ExecutionNioFlow<T, O> step, int decision) {
        this.step = step;
        this.decision = decision;
    }

    @Override
    ExecutionNioFlow<T, O> step() {
        return step;
    }

    @Override
    public NioStep<T, O> otherwise(UnaryOperator<Lane<T>> lane) {
        lane.apply(new DefaultLane<>(step.withGuards(
                AbstractChain.withGuard(step.guards(), new Guard(decision, false)))));
        return step;
    }
}
