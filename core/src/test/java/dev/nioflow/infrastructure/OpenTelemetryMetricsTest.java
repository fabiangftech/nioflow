package dev.nioflow.infrastructure;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.OverflowPolicy;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenTelemetryMetricsTest {

    @Test
    void exportsExecutionStageRecoveryAndFilterMetrics() {
        var reader = InMemoryMetricReader.create();
        var meterProvider = SdkMeterProvider.builder().registerMetricReader(reader).build();
        var engine = new DefaultNioEngine();
        engine.metrics(new OpenTelemetryMetrics(meterProvider.get("nioflow-test")));

        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.filter(value -> value >= 0)
                .handle("plus", value -> value + 1)
                .handle("boom-on-two", value -> {
                    if (value == 2) {
                        throw new IllegalStateException("boom");
                    }
                    return value;
                })
                .recover("fallback", error -> -100);

        assertEquals(6, flow.just(5).execute());     // completed
        assertEquals(-100, flow.just(1).execute());  // recovered (1 + 1 == 2 blows up)
        flow.just(-5).execute();                     // filtered

        Map<String, MetricData> metrics = reader.collectAllMetrics().stream()
                .collect(Collectors.toMap(MetricData::getName, Function.identity()));

        assertTrue(metrics.containsKey("nioflow.execution.duration"));
        assertTrue(metrics.containsKey("nioflow.stage.duration"));
        assertEquals(2, sum(metrics.get("nioflow.executions.completed"))); // the recovered one completes normally
        assertEquals(1, sum(metrics.get("nioflow.executions.filtered")));
        assertEquals(1, sum(metrics.get("nioflow.recoveries.applied")));

        engine.shutdown(Duration.ofMillis(100));
        meterProvider.close();
    }

    private static long sum(MetricData metric) {
        return metric.getLongSumData().getPoints().stream()
                .mapToLong(LongPointData::getValue)
                .sum();
    }

    /**
     * The failure, drop and queue-depth instruments: an execution that is not
     * recovered, a value rejected by backpressure, and the pending-results
     * gauge fed by inject/await.
     */
    @Test
    void exportsFailureDropAndQueueDepthMetrics() {
        var reader = InMemoryMetricReader.create();
        var meterProvider = SdkMeterProvider.builder().registerMetricReader(reader).build();
        var engine = new DefaultNioEngine(1, OverflowPolicy.DROP);
        engine.metrics(new OpenTelemetryMetrics(meterProvider.get("nioflow-test")));

        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("boom-on-one", value -> {
            if (value == 1) {
                throw new IllegalStateException("boom");
            }
            return value;
        });

        assertThrows(CompletionException.class, () -> flow.just(1).execute());   // failed, no recovery

        // Capacity 1 and nothing collecting: the permit is held by the first
        // value, so the rest are dropped.
        flow.justAll(List.of(2, 3, 4));
        assertEquals(2, engine.await());

        Map<String, MetricData> metrics = reader.collectAllMetrics().stream()
                .collect(Collectors.toMap(MetricData::getName, Function.identity()));

        assertTrue(sum(metrics, "nioflow.executions.failed") >= 1, () -> "failed: " + metrics.keySet());
        assertTrue(sum(metrics, "nioflow.values.dropped") >= 1, () -> "dropped: " + metrics.keySet());
        assertTrue(metrics.containsKey("nioflow.queue.depth"), () -> "queue depth: " + metrics.keySet());

        engine.shutdown(Duration.ofMillis(200));
    }

    /**
     * Detached sub-flows export apart from the request that spawned them: their
     * latency is not the request's, so it must not pollute
     * nioflow.execution.duration.
     */
    @Test
    void exportsForkMetricsSeparatelyFromTheRequest() {
        var reader = InMemoryMetricReader.create();
        var meterProvider = SdkMeterProvider.builder().registerMetricReader(reader).build();
        var engine = new DefaultNioEngine();
        engine.metrics(new OpenTelemetryMetrics(meterProvider.get("nioflow-test")));

        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.fork("audit", sub -> sub.handle("persist", value -> value))
                .fork("doomed", sub -> sub.handle("boom", value -> {
                    throw new IllegalStateException("boom");
                }))
                .handle("main", value -> value + 1);

        assertEquals(2, flow.just(1).execute());
        assertEquals(0, engine.shutdown(Duration.ofSeconds(2)), "both forks must drain");

        Map<String, MetricData> metrics = reader.collectAllMetrics().stream()
                .collect(Collectors.toMap(MetricData::getName, Function.identity()));

        assertEquals(2, sum(metrics, "nioflow.forks.started"), () -> "started: " + metrics.keySet());
        assertEquals(1, sum(metrics, "nioflow.forks.failed"), () -> "failed: " + metrics.keySet());
        assertTrue(metrics.containsKey("nioflow.fork.duration"), () -> "duration: " + metrics.keySet());
        assertTrue(metrics.containsKey("nioflow.fork.in_flight"), () -> "gauge: " + metrics.keySet());

        // The request is ONE execution — a fork is not one.
        assertEquals(1, sum(metrics, "nioflow.executions.completed"));
        assertEquals(0, sum(metrics, "nioflow.executions.failed"));

        meterProvider.close();
    }

    private static long sum(Map<String, MetricData> metrics, String name) {
        MetricData data = metrics.get(name);
        if (data == null) {
            return 0;
        }
        return data.getLongSumData().getPoints().stream().mapToLong(LongPointData::getValue).sum();
    }
}
