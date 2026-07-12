package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.NioStep;
import dev.nioflow.core.facade.StepCases;
import dev.nioflow.core.model.Guard;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/** Per-request match(), first-match-wins: see DefaultCases. */
final class DefaultStepCases<T, O> extends NioStepDelegate<T, O> implements StepCases<T, O> {

    private final ExecutionNioFlow<T, O> step;
    private final List<Guard> priorCasesFalse = new ArrayList<>();

    DefaultStepCases(ExecutionNioFlow<T, O> step) {
        this.step = step;
    }

    @Override
    ExecutionNioFlow<T, O> step() {
        return step;
    }

    @Override
    public StepCases<T, O> is(Predicate<T> predicate, UnaryOperator<Lane<T>> lane) {
        AbstractChain<T> evaluation = step.withGuards(
                AbstractChain.withGuards(step.guards(), priorCasesFalse));
        int decision = evaluation.appendDecision(predicate);
        lane.apply(new DefaultLane<>(evaluation.withGuards(
                AbstractChain.withGuard(evaluation.guards(), new Guard(decision, true)))));
        priorCasesFalse.add(new Guard(decision, false));
        return this;
    }

    @Override
    public NioStep<T, O> otherwise(UnaryOperator<Lane<T>> lane) {
        lane.apply(new DefaultLane<>(step.withGuards(
                AbstractChain.withGuards(step.guards(), priorCasesFalse))));
        return step;
    }
}
