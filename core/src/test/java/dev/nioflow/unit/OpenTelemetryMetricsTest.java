package dev.nioflow.unit;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import dev.nioflow.application.facade.NioFlow;
import dev.nioflow.infrastructure.metrics.OpenTelemetryMetrics;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.*;

class OpenTelemetryMetricsTest {

    @Test
    void theAdapterFeedsOpenTelemetryInstruments() {
        InMemoryMetricReader reader = InMemoryMetricReader.create();
        try (SdkMeterProvider provider = SdkMeterProvider.builder().registerMetricReader(reader).build()) {
            Meter meter = provider.get("dev.nioflow.test");

            try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
                nioFlow.metrics(OpenTelemetryMetrics.of(meter))
                        .handle("validate", x -> x + 1)
                        .submit("save", x -> {
                            if (x == 3) {
                                throw new IllegalStateException("boom");
                            }
                            return x * 10;
                        });

                nioFlow.justAll(List.of(1, 5));
                nioFlow.just(2); // becomes 3 after validate and fails in save
                assertThrows(CompletionException.class, nioFlow::join);
            }

            Collection<MetricData> metrics = reader.collectAllMetrics();
            assertEquals(3, longSum(metrics, "dev.nioflow.values.injected"));
            assertEquals(2, longSum(metrics, "dev.nioflow.values.completed"));
            assertEquals(1, longSum(metrics, "dev.nioflow.values.failed"));
            assertEquals(0, longSum(metrics, "dev.nioflow.values.in_flight"), "in-flight must return to zero");
            assertTrue(metrics.stream().anyMatch(m -> m.getName().equals("dev.nioflow.stage.duration")));
        }
    }

    private static long longSum(Collection<MetricData> metrics, String name) {
        return metrics.stream()
                .filter(metric -> metric.getName().equals(name))
                .flatMap(metric -> metric.getLongSumData().getPoints().stream())
                .mapToLong(LongPointData::getValue)
                .sum();
    }
}
