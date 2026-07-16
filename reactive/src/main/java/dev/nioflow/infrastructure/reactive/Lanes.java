package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.Segment;

import java.util.function.UnaryOperator;

/**
 * Carries the flow's {@link ReactiveConfig} into the places core hands out a
 * lane: a when()/match() lambda and a segment body (use, fork).
 *
 * <p>Core builds a plain {@code DefaultLane} there, so without this the lambda
 * would receive an unbudgeted lane and every {@code handleMono} declared inside
 * a branch or a fork would be back to blocking forever (and requireBudget() would
 * not reach it). Wrapping it here means {@link Reactive#lane(Lane)} finds a
 * ReactiveLane already — its instanceof check hands it straight back, config and
 * all.
 *
 * <p>Core ignores what a lane lambda returns (the links are appended as they are
 * declared, by side effect on the guarded view), so handing back a reactive lane
 * where it expects a plain one changes nothing for it.
 */
final class Lanes {

    private Lanes() {
    }

    // Skip the wrap only when propagating the config would give the lane the
    // SAME behaviour it defaults to unwrapped — i.e. the config matches
    // ReactiveConfig.NONE, which an unwrapped lane inherits via Reactive.lane.
    // Since RFC 0034 that default has requireBudget ON, so a plain flow's lane
    // already enforces it; what must NOT be skipped is an allowUnbudgeted() flow
    // (requireBudget OFF, differing from the default) — its waiver has to reach
    // the lane, or a handleMono inside a branch/fork would wrongly demand a budget
    // the flow deliberately waived. A default budget or preferAsync must propagate
    // for the same reason.
    private static boolean inert(ReactiveConfig config) {
        return config.budget() == null && !config.preferAsync() && config.requireBudget();
    }

    static <T> UnaryOperator<Lane<T>> budgeted(UnaryOperator<Lane<T>> lane, ReactiveConfig config) {
        return inert(config)
                ? lane
                : core -> lane.apply(new DefaultReactiveLane<>(core, config));
    }

    static <T, R> Segment<T, R> budgeted(Segment<T, R> segment, ReactiveConfig config) {
        return inert(config)
                ? segment
                : core -> segment.define(new DefaultReactiveLane<>(core, config));
    }
}
