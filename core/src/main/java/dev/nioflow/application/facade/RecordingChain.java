package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.Link;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

/**
 * Off-chain recorder: builds a segment's links into a local list instead of the
 * live chain, while still drawing decision ids from the REAL engine (a scratch
 * engine would restart at 0 and collide with the live chain's guards) and
 * minting anonymous names through the owner's counter (a fresh counter would
 * mint duplicate anchors). Build-only: it never executes.
 *
 * <p>Used by replaceRegion (records a segment to swap in) and by fork (records
 * the detached sub-chain). It starts with NO guards, which is what makes a
 * fork's sub-chain guard-closed even when the fork is declared inside a lane.
 */
final class RecordingChain<X> extends AbstractChain<X> {

    private final NioEngine engine;
    private final List<Link> recorded;
    private final UnaryOperator<String> naming;
    private final List<Guard> guards;

    RecordingChain(NioEngine engine, List<Link> recorded, AtomicInteger anonymousLinks) {
        this(engine, recorded, prefix -> prefix + "-" + anonymousLinks.getAndIncrement(), List.of());
    }

    RecordingChain(NioEngine engine, List<Link> recorded, UnaryOperator<String> naming) {
        this(engine, recorded, naming, List.of());
    }

    private RecordingChain(NioEngine engine, List<Link> recorded, UnaryOperator<String> naming,
                           List<Guard> guards) {
        this.engine = engine;
        this.recorded = recorded;
        this.naming = naming;
        this.guards = guards;
    }

    @Override
    NioEngine engine() {
        return engine;
    }

    @Override
    void appendLink(Link link) {
        recorded.add(link);
    }

    /** Records off-chain (a replaceRegion segment, a fork's sub-chain). */
    @Override
    boolean buildsSharedChain() {
        return false;
    }

    @Override
    List<Guard> guards() {
        return guards;
    }

    @Override
    AbstractChain<X> withGuards(List<Guard> guards) {
        return new RecordingChain<>(engine, recorded, naming, guards);
    }

    @Override
    String anonymousName(String prefix) {
        return naming.apply(prefix);
    }
}
