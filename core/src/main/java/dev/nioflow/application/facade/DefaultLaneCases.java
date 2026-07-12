package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.LaneCases;
import dev.nioflow.core.model.Guard;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Nested match() inside a lane, first-match-wins (same semantics as the
 * flow-level Cases): case k evaluates only if previous cases were false,
 * its sub-lane additionally requires its own decision, and otherwise()
 * requires all false. Chaining continues on the enclosing lane's guards.
 */
final class DefaultLaneCases<T> extends DefaultLane<T> implements LaneCases<T> {

    private final List<Guard> priorCasesFalse = new ArrayList<>();

    DefaultLaneCases(AbstractChain<T> view) {
        super(view);
    }

    @Override
    public LaneCases<T> is(Predicate<T> predicate, UnaryOperator<Lane<T>> lane) {
        AbstractChain<T> evaluation = view.withGuards(
                AbstractChain.withGuards(view.guards(), priorCasesFalse));
        int decision = evaluation.appendDecision(predicate);
        lane.apply(new DefaultLane<>(evaluation.withGuards(
                AbstractChain.withGuard(evaluation.guards(), new Guard(decision, true)))));
        priorCasesFalse.add(new Guard(decision, false));
        return this;
    }

    @Override
    public Lane<T> otherwise(UnaryOperator<Lane<T>> lane) {
        lane.apply(new DefaultLane<>(view.withGuards(
                AbstractChain.withGuards(view.guards(), priorCasesFalse))));
        return new DefaultLane<>(view);
    }
}
