package dev.nioflow.infrastructure;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioFlow;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
                .mapToLong(point -> point.getValue())
                .sum();
    }
}
