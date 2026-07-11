package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.model.Link;
import dev.nioflow.core.model.Splice;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class DefaultNioEngine implements NioEngine {

    private static final String NIO_FLOW_BOSS = "nio-flow-boss-";
    private final ExecutorService bossExecutorService;
    private final ExecutorService workersExecutorService;

    public DefaultNioEngine() {
        this(
                Executors.newSingleThreadExecutor(Thread.ofPlatform().name(NIO_FLOW_BOSS, 0)
                        .factory()),
                Executors.newVirtualThreadPerTaskExecutor()
        );
    }

    public DefaultNioEngine(ExecutorService bossExecutorService,
                            ExecutorService workersExecutorService) {
        this.bossExecutorService = bossExecutorService;
        this.workersExecutorService = workersExecutorService;
    }

    @Override
    public void inject(Object input) {

    }

    @Override
    public void inject(Object input, Map<String, Object> context) {

    }

    @Override
    public CompletableFuture<Object> call(Object input, Map<String, Object> context) {
        return null;
    }

    @Override
    public CompletableFuture<Object> call(Object input, Map<String, Object> context, List<Link> chain) {
        return null;
    }

    @Override
    public List<Link> chain() {
        return List.of();
    }

    @Override
    public void append(Link link) {

    }

    @Override
    public void seal() {

    }

    @Override
    public void release() {

    }

    @Override
    public void splice(String anchor, Splice position, List<Link> links) {

    }

    @Override
    public int nextDecision() {
        return 0;
    }

    @Override
    public void addErrorHandler(Consumer<Throwable> handler) {

    }

    @Override
    public void addCompleteHandler(Consumer<Object> handler) {

    }

    @Override
    public Object await() {
        return null;
    }

    @Override
    public Object await(Duration timeout) {
        return null;
    }

    @Override
    public void shutdown(Duration gracePeriod) {

    }
}
