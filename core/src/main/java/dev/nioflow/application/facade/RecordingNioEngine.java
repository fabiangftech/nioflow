package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlowMetrics;
import dev.nioflow.core.facade.NioFlowTracer;
import dev.nioflow.core.model.Diagnostics;
import dev.nioflow.core.model.Link;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * The engine behind a segment under construction for a structural edit: it only
 * collects the links the segment declares, so the segment builder can use the full
 * fluent API — stages, filters, forks, recoveries — without touching the live chain.
 * Decision ids are delegated to the live engine, keeping them unique across versions.
 *
 * <p>Anything besides declaring links (injecting, joining, sealing, registering
 * handlers) is rejected: a segment describes chain structure, nothing else.
 */
final class RecordingNioEngine implements NioEngine {

    private final NioEngine decisions;
    private final List<Link> links = new ArrayList<>();

    /**
     * @param decisions the live engine, asked for fork decision ids so ids stay
     *                  unique engine-wide across chain versions
     */
    RecordingNioEngine(NioEngine decisions) {
        this.decisions = decisions;
    }

    /** The links the segment declared, in declaration order. */
    List<Link> links() {
        return List.copyOf(links);
    }

    @Override
    public void append(Link link) {
        links.add(link);
    }

    @Override
    public int nextDecision() {
        return decisions.nextDecision();
    }

    @Override
    public void inject(Object input) {
        throw reject();
    }

    @Override
    public void inject(Object input, Map<String, Object> context) {
        throw reject();
    }

    @Override
    public CompletableFuture<Object> call(Object input, Map<String, Object> context) {
        throw reject();
    }

    @Override
    public CompletableFuture<Object> call(Object input, Map<String, Object> context, List<Link> chain) {
        throw reject();
    }

    @Override
    public List<Link> chain() {
        throw reject();
    }

    @Override
    public void release() {
        throw reject();
    }

    @Override
    public void seal() {
        throw reject();
    }

    @Override
    public void splice(String anchor, Splice position, List<Link> links) {
        throw reject();
    }

    @Override
    public void addErrorHandler(Consumer<Throwable> handler) {
        throw reject();
    }

    @Override
    public void addCompleteHandler(Consumer<Object> handler) {
        throw reject();
    }

    @Override
    public void metrics(NioFlowMetrics metrics) {
        throw reject();
    }

    @Override
    public void trace(NioFlowTracer tracer) {
        throw reject();
    }

    @Override
    public Diagnostics diagnostics() {
        throw reject();
    }

    @Override
    public Object await() {
        throw reject();
    }

    @Override
    public Object await(Duration timeout) {
        throw reject();
    }

    @Override
    public void shutdown(Duration gracePeriod) {
        throw reject();
    }

    private static IllegalStateException reject() {
        return new IllegalStateException(
                "a segment builder can only declare links (handle, submit, filter, batch, "
                        + "fanOut, adapt, when, match, onErrorResume)");
    }
}
