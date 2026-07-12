package dev.nioflow.application.facade;

import dev.nioflow.core.facade.FlowResult;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.Link;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Off-chain recorder for replaceRegion: builds a segment's links into a
 * local list instead of the live chain, while still drawing decision ids
 * from the REAL engine (a scratch engine would restart at 0 and collide
 * with the live chain's guards) and sharing the root's anonymous-name
 * counter (a fresh counter would mint duplicate anchors). Build-only:
 * it can never execute.
 */
final class RecordingNioFlow<I, T> extends AbstractNioFlow<I, T> {

    private final NioEngine engine;
    private final List<Link> recorded;
    private final AtomicInteger anonymousLinks;
    private final List<Guard> guards;

    RecordingNioFlow(NioEngine engine, List<Link> recorded, AtomicInteger anonymousLinks) {
        this(engine, recorded, anonymousLinks, List.of());
    }

    private RecordingNioFlow(NioEngine engine, List<Link> recorded, AtomicInteger anonymousLinks,
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
    AbstractNioFlow<I, T> withGuards(List<Guard> guards) {
        return new RecordingNioFlow<>(engine, recorded, anonymousLinks, guards);
    }

    @Override
    String anonymousName(String prefix) {
        return prefix + "-" + anonymousLinks.getAndIncrement();
    }

    @Override
    public <R> NioFlow<I, R> use(String region, dev.nioflow.core.facade.Segment<T, R> segment) {
        throw new UnsupportedOperationException("A recorded replacement cannot register nested regions");
    }

    @Override
    public NioFlow<I, T> just(I input) {
        throw new UnsupportedOperationException("Recorder flows only build links");
    }

    @Override
    public NioFlow<I, T> justAll(Iterable<I> inputs) {
        throw new UnsupportedOperationException("Recorder flows only build links");
    }

    @Override
    public NioFlow<I, T> key(Object key) {
        throw new UnsupportedOperationException("Recorder flows only build links");
    }

    @Override
    public NioFlow<I, T> onComplete(Consumer<T> callback) {
        throw new UnsupportedOperationException("Recorder flows only build links");
    }

    @Override
    public NioFlow<I, T> onError(Consumer<Throwable> callback) {
        throw new UnsupportedOperationException("Recorder flows only build links");
    }

    @Override
    public T execute() {
        throw new UnsupportedOperationException("Recorder flows only build links");
    }

    @Override
    public CompletableFuture<T> executeAsync() {
        throw new UnsupportedOperationException("Recorder flows only build links");
    }

    @Override
    public FlowResult<T> executeResult() {
        throw new UnsupportedOperationException("Recorder flows only build links");
    }
}
