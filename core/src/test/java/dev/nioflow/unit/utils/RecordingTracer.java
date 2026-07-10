package dev.nioflow.unit.utils;

import dev.nioflow.core.facade.NioFlowTracer;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** Test double for the trace port: records every transition as a readable line. */
public class RecordingTracer implements NioFlowTracer {

   public final List<String> events = new CopyOnWriteArrayList<>();

    @Override
    public void injected(long value, Object payload) {
        events.add(value + ":injected:" + payload);
    }

    @Override
    public void stage(long value, String name, boolean async, Object in, Object out, Throwable error) {
        events.add(value + ":stage:" + (name == null ? (async ? "submit" : "handle") : name)
                + ":" + in + "->" + (error == null ? out : "error"));
    }

    @Override
    public void lane(long value, int decision, boolean outcome) {
        events.add(value + ":lane:" + outcome);
    }

    @Override
    public void dropped(long value, Object payload) {
        events.add(value + ":dropped:" + payload);
    }

    @Override
    public void fannedOut(long value, int children) {
        events.add(value + ":fannedOut:" + children);
    }

    @Override
    public void recovered(long value, Throwable error, Object fallback) {
        events.add(value + ":recovered:" + fallback);
    }

    @Override
    public void failed(long value, Throwable error) {
        events.add(value + ":failed:" + error.getMessage());
    }

    @Override
    public void completed(long value, Object result) {
        events.add(value + ":completed:" + result);
    }
}
