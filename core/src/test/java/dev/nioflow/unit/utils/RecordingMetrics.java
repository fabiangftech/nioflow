package dev.nioflow.unit.utils;

import dev.nioflow.core.facade.NioFlowMetrics;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test double for the metrics port: records every event for assertions.
 */
public class RecordingMetrics implements NioFlowMetrics {

    public final AtomicInteger injected = new AtomicInteger();
    public final AtomicInteger completed = new AtomicInteger();
    public final AtomicInteger failed = new AtomicInteger();
    public final AtomicInteger dropped = new AtomicInteger();
    public final List<Integer> fanOuts = new CopyOnWriteArrayList<>();
    public final List<String> stages = new CopyOnWriteArrayList<>();

    @Override
    public void injected() {
        injected.incrementAndGet();
    }

    @Override
    public void completed() {
        completed.incrementAndGet();
    }

    @Override
    public void failed(Throwable error) {
        failed.incrementAndGet();
    }

    @Override
    public void dropped() {
        dropped.incrementAndGet();
    }

    @Override
    public void fannedOut(int children) {
        fanOuts.add(children);
    }

    @Override
    public void stage(String name, boolean async, long elapsedNanos, boolean success) {
        stages.add((name == null ? "unnamed" : name)
                   + ":" + (async ? "submit" : "handle")
                   + ":" + (success ? "success" : "error")
                   + ":" + (elapsedNanos >= 0 ? "timed" : "negative"));
    }
}
