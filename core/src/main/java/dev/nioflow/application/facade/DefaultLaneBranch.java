package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.LaneBranch;
import dev.nioflow.core.model.Guard;

import java.util.function.UnaryOperator;

/**
 * Branch opened by a nested when().then() inside a lane: otherwise() declares
 * the opposite sub-lane; chaining continues on the enclosing lane's guards.
 */
final class DefaultLaneBranch<T> extends DefaultLane<T> implements LaneBranch<T> {

    private final int decision;

    DefaultLaneBranch(AbstractChain<T> view, int decision) {
        super(view);
        this.decision = decision;
    }

    @Override
    public Lane<T> otherwise(UnaryOperator<Lane<T>> lane) {
        lane.apply(new DefaultLane<>(view.withGuards(
                AbstractChain.withGuard(view.guards(), new Guard(decision, false)))));
        return new DefaultLane<>(view);
    }
}
