package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Cases;
import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.Guard;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * match() with first-match-wins semantics: case k's Decision only evaluates
 * if previous cases were false (it carries their guards in false), its lane
 * additionally requires its own decision true, and otherwise() requires all
 * false. Chaining after (with or without otherwise) returns to the main line.
 */
final class DefaultCases<I, O> extends NioFlowDelegate<I, O> implements Cases<I, O> {

    private final DefaultNioFlow<I, O> flow;
    private final List<Guard> priorCasesFalse = new ArrayList<>();

    DefaultCases(DefaultNioFlow<I, O> flow) {
        this.flow = flow;
    }

    @Override
    DefaultNioFlow<I, O> flow() {
        return flow;
    }

    @Override
    public Cases<I, O> is(Predicate<I> predicate, UnaryOperator<Lane<I>> lane) {
        AbstractChain<I> evaluation = flow.withGuards(
                AbstractChain.withGuards(flow.guards(), priorCasesFalse));
        int decision = evaluation.appendDecision(predicate);
        lane.apply(new DefaultLane<>(evaluation.withGuards(
                AbstractChain.withGuard(evaluation.guards(), new Guard(decision, true)))));
        priorCasesFalse.add(new Guard(decision, false));
        return this;
    }

    @Override
    public NioFlow<I, O> otherwise(UnaryOperator<Lane<I>> lane) {
        lane.apply(new DefaultLane<>(flow.withGuards(
                AbstractChain.withGuards(flow.guards(), priorCasesFalse))));
        return flow;
    }
}
