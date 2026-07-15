package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.Segment;

import java.time.Duration;
import java.util.function.UnaryOperator;

/**
 * Carries the flow's default budget into the places core hands out a lane: a
 * when()/match() lambda and a segment body (use, fork).
 *
 * <p>Core builds a plain {@code DefaultLane} there, so without this the lambda
 * would receive an unbudgeted lane and every {@code handleMono} declared inside
 * a branch or a fork would be back to blocking forever. Wrapping it here means
 * {@link Reactive#lane(Lane)} finds a ReactiveLane already — its instanceof
 * check hands it straight back, budget and all.
 *
 * <p>Core ignores what a lane lambda returns (the links are appended as they are
 * declared, by side effect on the guarded view), so handing back a reactive lane
 * where it expects a plain one changes nothing for it.
 */
final class Lanes {

    private Lanes() {
    }

    static <T> UnaryOperator<Lane<T>> budgeted(UnaryOperator<Lane<T>> lane, Duration budget, boolean preferAsync) {
        return budget == null && !preferAsync
                ? lane
                : core -> lane.apply(new DefaultReactiveLane<>(core, budget, preferAsync));
    }

    static <T, R> Segment<T, R> budgeted(Segment<T, R> segment, Duration budget, boolean preferAsync) {
        return budget == null && !preferAsync
                ? segment
                : core -> segment.define(new DefaultReactiveLane<>(core, budget, preferAsync));
    }
}
