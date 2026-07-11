package dev.nioflow.infrastructure;

import dev.nioflow.core.facade.NioFlowMetrics;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * OpenTelemetry adapter for the metrics SPI. The otel API is a compileOnly
 * dependency of core: this class only loads if the consumer brings
 * opentelemetry-api to the runtime classpath.
 *
 * Exposes: nioflow.execution.duration and nioflow.stage.duration histograms
 * (microseconds; stages tagged with nioflow.stage), completion/failure/filter
 * counters, recovery counter (tagged with nioflow.recovery), dropped-values
 * counter and a nioflow.queue.depth gauge.
 */
public final class OpenTelemetryMetrics implements NioFlowMetrics {

    private static final AttributeKey<String> STAGE = AttributeKey.stringKey("nioflow.stage");
    private static final AttributeKey<String> RECOVERY = AttributeKey.stringKey("nioflow.recovery");

    private final LongHistogram executionMicros;
    private final LongHistogram stageMicros;
    private final LongCounter completed;
    private final LongCounter failed;
    private final LongCounter filtered;
    private final LongCounter recoveries;
    private final LongCounter dropped;
    private final AtomicInteger queueDepth = new AtomicInteger();
    // Attribute instances cached per name: no allocations on the hot path.
    private final Map<String, Attributes> stageAttributes = new ConcurrentHashMap<>();
    private final Map<String, Attributes> recoveryAttributes = new ConcurrentHashMap<>();

    public OpenTelemetryMetrics(Meter meter) {
        this.executionMicros = meter.histogramBuilder("nioflow.execution.duration")
                .setUnit("us").ofLongs().build();
        this.stageMicros = meter.histogramBuilder("nioflow.stage.duration")
                .setUnit("us").ofLongs().build();
        this.completed = meter.counterBuilder("nioflow.executions.completed").build();
        this.failed = meter.counterBuilder("nioflow.executions.failed").build();
        this.filtered = meter.counterBuilder("nioflow.executions.filtered").build();
        this.recoveries = meter.counterBuilder("nioflow.recoveries.applied").build();
        this.dropped = meter.counterBuilder("nioflow.values.dropped").build();
        meter.gaugeBuilder("nioflow.queue.depth").ofLongs()
                .buildWithCallback(gauge -> gauge.record(queueDepth.get()));
    }

    @Override
    public void executionCompleted(long nanos) {
        completed.add(1);
        executionMicros.record(nanos / 1_000);
    }

    @Override
    public void executionFailed(Throwable error, long nanos) {
        failed.add(1);
        executionMicros.record(nanos / 1_000);
    }

    @Override
    public void executionFiltered(long nanos) {
        filtered.add(1);
        executionMicros.record(nanos / 1_000);
    }

    @Override
    public void stageCompleted(String stage, long nanos) {
        stageMicros.record(nanos / 1_000,
                stageAttributes.computeIfAbsent(stage, name -> Attributes.of(STAGE, name)));
    }

    @Override
    public void recoveryApplied(String recovery) {
        recoveries.add(1,
                recoveryAttributes.computeIfAbsent(recovery, name -> Attributes.of(RECOVERY, name)));
    }

    @Override
    public void valueDropped() {
        dropped.add(1);
    }

    @Override
    public void queueDepth(int pending) {
        queueDepth.set(pending);
    }
}
