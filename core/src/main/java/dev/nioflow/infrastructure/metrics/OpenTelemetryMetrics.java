package dev.nioflow.infrastructure.metrics;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.Meter;
import dev.nioflow.core.facade.NioFlowMetrics;

/**
 * OpenTelemetry adapter for the {@link NioFlowMetrics} port. Optional
 * infrastructure: the library only compiles against opentelemetry-api
 * ({@code compileOnly}), so using this class requires {@code io.opentelemetry}
 * on your classpath. Nothing else in the library touches it — replace or drop
 * the adapter and the core stays intact.
 *
 * <p>Instruments: {@code dev.nioflow.values.injected/completed/failed/dropped}
 * counters, an {@code dev.nioflow.values.in_flight} up-down counter, and an
 * {@code dev.nioflow.stage.duration} histogram (ms) with stage name, kind
 * (handle/submit) and outcome attributes.
 */
public final class OpenTelemetryMetrics implements NioFlowMetrics {

    private static final AttributeKey<String> STAGE = AttributeKey.stringKey("nio-flow.stage");
    private static final AttributeKey<String> KIND = AttributeKey.stringKey("nio-flow.stage.kind");
    private static final AttributeKey<String> OUTCOME = AttributeKey.stringKey("nio-flow.stage.outcome");
    private static final AttributeKey<String> ERROR_TYPE = AttributeKey.stringKey("error.type");

    private final LongCounter injected;
    private final LongCounter completed;
    private final LongCounter failed;
    private final LongCounter dropped;
    private final LongUpDownCounter inFlight;
    private final DoubleHistogram stageDuration;

    private OpenTelemetryMetrics(Meter meter) {
        this.injected = meter.counterBuilder("dev.nioflow.values.injected")
                .setDescription("Values admitted into the nio-flow").build();
        this.completed = meter.counterBuilder("dev.nioflow.values.completed")
                .setDescription("Values that reached the end of the chain").build();
        this.failed = meter.counterBuilder("dev.nioflow.values.failed")
                .setDescription("Values that failed after exhausting recoveries").build();
        this.dropped = meter.counterBuilder("dev.nioflow.values.dropped")
                .setDescription("Values deliberately dropped by filters").build();
        this.inFlight = meter.upDownCounterBuilder("dev.nioflow.values.in_flight")
                .setDescription("Values currently flowing").build();
        this.stageDuration = meter.histogramBuilder("dev.nioflow.stage.duration")
                .setDescription("Stage execution time").setUnit("ms").build();
    }

    public static OpenTelemetryMetrics of(Meter meter) {
        return new OpenTelemetryMetrics(meter);
    }

    @Override
    public void injected() {
        injected.add(1);
        inFlight.add(1);
    }

    @Override
    public void completed() {
        completed.add(1);
        inFlight.add(-1);
    }

    @Override
    public void failed(Throwable error) {
        failed.add(1, Attributes.of(ERROR_TYPE, error.getClass().getSimpleName()));
        inFlight.add(-1);
    }

    @Override
    public void dropped() {
        dropped.add(1);
        inFlight.add(-1);
    }

    @Override
    public void fannedOut(int children) {
        inFlight.add(children - 1L);
    }

    @Override
    public void stage(String name, boolean async, long elapsedNanos, boolean success) {
        stageDuration.record(elapsedNanos / 1_000_000.0, Attributes.of(
                STAGE, name == null ? "unnamed" : name,
                KIND, async ? "submit" : "handle",
                OUTCOME, success ? "success" : "error"));
    }
}
