package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Branch;
import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.Guard;

import java.util.function.UnaryOperator;

/**
 * Branch opened by when().then(): otherwise() declares the opposite lane and
 * the rest of the API delegates to the original flow — chaining after a fork
 * returns to the main line (without the fork's guards).
 */
final class DefaultBranch<I, O> extends NioFlowDelegate<I, O> implements Branch<I, O> {

    private final DefaultNioFlow<I, O> flow;
    private final int decision;

    DefaultBranch(DefaultNioFlow<I, O> flow, int decision) {
        this.flow = flow;
        this.decision = decision;
    }

    @Override
    DefaultNioFlow<I, O> flow() {
        return flow;
    }

    @Override
    public NioFlow<I, O> otherwise(UnaryOperator<Lane<I>> lane) {
        lane.apply(new DefaultLane<>(flow.withGuards(
                AbstractChain.withGuard(flow.guards(), new Guard(decision, false)))));
        return flow;
    }
}
