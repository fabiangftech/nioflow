package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.NioFlow;

/**
 * Entry point to the reactive facade.
 *
 * <p>There is no engine here and no second implementation: {@link #flow} wraps
 * an ordinary {@link NioFlow} in a {@link ReactiveFlow}, which delegates every
 * step to it and appends the very same links. The reactive steps are plain
 * stages whose function parks a virtual worker on a Mono — the engine never
 * learns what a Mono is.
 *
 * <pre>
 * ReactiveFlow&lt;Order, Receipt&gt; orders =
 *         Reactive.flow(DefaultNioFlow.from(Order.class));
 * </pre>
 */
public final class Reactive {

    private Reactive() {
    }

    /** Wraps a shared definition so its pipelines can take Monos and end in one. */
    public static <I, O> ReactiveFlow<I, O> flow(NioFlow<I, O> flow) {
        return flow instanceof ReactiveFlow<I, O> reactive ? reactive : new DefaultReactiveFlow<>(flow);
    }

    /**
     * Unwraps the lane a when()/match() lambda receives, so reactive steps can
     * be declared inside a branch.
     *
     * <p>The one helper the design could not get rid of. The branch contracts
     * hard-code {@code UnaryOperator<Lane<T>>}, and a reactive variant would
     * have the same erasure — a name clash, not an override — so Java will not
     * let a ReactiveCondition both BE a Condition and hand out a ReactiveLane.
     */
    public static <T> ReactiveLane<T> lane(Lane<T> lane) {
        return lane instanceof ReactiveLane<T> reactive ? reactive : new DefaultReactiveLane<>(lane);
    }
}
