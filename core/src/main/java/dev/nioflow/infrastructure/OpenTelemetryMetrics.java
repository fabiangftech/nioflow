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
 * counter and a nioflow.queue.depth gauge. Detached sub-flows report apart from
 * the request they came from (they are not on its critical path):
 * nioflow.fork.duration, nioflow.forks.started/failed and a
 * nioflow.fork.in_flight gauge, all tagged with nioflow.fork.
 */
public final class OpenTelemetryMetrics implements NioFlowMetrics {

    private static final AttributeKey<String> STAGE = AttributeKey.stringKey("nioflow.stage");
    private static final AttributeKey<String> RECOVERY = AttributeKey.stringKey("nioflow.recovery");
    private static final AttributeKey<String> FORK = AttributeKey.stringKey("nioflow.fork");

    private final LongHistogram executionMicros;
    private final LongHistogram stageMicros;
    private final LongHistogram forkMicros;
    private final LongCounter completed;
    private final LongCounter failed;
    private final LongCounter filtered;
    private final LongCounter recoveries;
    private final LongCounter dropped;
    private final LongCounter forksStarted;
    private final LongCounter forksFailed;
    private final AtomicInteger queueDepth = new AtomicInteger();
    private final AtomicInteger forksInFlight = new AtomicInteger();
    // Attribute instances cached per name: no allocations on the hot path.
    private final Map<String, Attributes> stageAttributes = new ConcurrentHashMap<>();
    private final Map<String, Attributes> recoveryAttributes = new ConcurrentHashMap<>();
    private final Map<String, Attributes> forkAttributes = new ConcurrentHashMap<>();

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
        this.forkMicros = meter.histogramBuilder("nioflow.fork.duration")
                .setUnit("us").ofLongs().build();
        this.forksStarted = meter.counterBuilder("nioflow.forks.started").build();
        this.forksFailed = meter.counterBuilder("nioflow.forks.failed").build();
        meter.gaugeBuilder("nioflow.queue.depth").ofLongs()
                .buildWithCallback(gauge -> gauge.record(queueDepth.get()));
        meter.gaugeBuilder("nioflow.fork.in_flight").ofLongs()
                .buildWithCallback(gauge -> gauge.record(forksInFlight.get()));
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
    public void forkStarted(String fork) {
        forksStarted.add(1, attributesFor(fork));
    }

    @Override
    public void forkCompleted(String fork, long nanos) {
        forkMicros.record(nanos / 1_000, attributesFor(fork));
    }

    @Override
    public void forkFailed(String fork, Throwable error, long nanos) {
        forksFailed.add(1, attributesFor(fork));
        forkMicros.record(nanos / 1_000, attributesFor(fork));
    }

    @Override
    public void forksInFlight(int running) {
        forksInFlight.set(running);
    }

    private Attributes attributesFor(String fork) {
        return forkAttributes.computeIfAbsent(fork, name -> Attributes.of(FORK, name));
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
