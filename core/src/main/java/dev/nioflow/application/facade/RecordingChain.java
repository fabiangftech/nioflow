package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.Link;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Off-chain recorder for replaceRegion: builds a segment's links into a local
 * list instead of the live chain, while still drawing decision ids from the
 * REAL engine (a scratch engine would restart at 0 and collide with the live
 * chain's guards) and sharing the root's anonymous-name counter (a fresh
 * counter would mint duplicate anchors). Build-only: it never executes.
 */
final class RecordingChain<X> extends AbstractChain<X> {

    private final NioEngine engine;
    private final List<Link> recorded;
    private final AtomicInteger anonymousLinks;
    private final List<Guard> guards;

    RecordingChain(NioEngine engine, List<Link> recorded, AtomicInteger anonymousLinks) {
        this(engine, recorded, anonymousLinks, List.of());
    }

    private RecordingChain(NioEngine engine, List<Link> recorded, AtomicInteger anonymousLinks,
                           List<Guard> guards) {
        this.engine = engine;
        this.recorded = recorded;
        this.anonymousLinks = anonymousLinks;
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

    @Override
    List<Guard> guards() {
        return guards;
    }

    @Override
    AbstractChain<X> withGuards(List<Guard> guards) {
        return new RecordingChain<>(engine, recorded, anonymousLinks, guards);
    }

    @Override
    String anonymousName(String prefix) {
        return prefix + "-" + anonymousLinks.getAndIncrement();
    }
}
