package dev.nioflow.unit;

import dev.nioflow.application.facade.DefaultNioFlow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DefaultNioFlowAdaptTest {

    @Test
    void adaptChangesThePipelineType() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            String result = defaultNioFlow.just(21)
                    .submit(x -> x * 2)
                    .adapt(x -> "value=" + x)
                    .handle(String::toUpperCase)
                    .join();

            assertEquals("VALUE=42", result);
        }
    }

    @Test
    void adaptRoundTripsAcrossAsyncStages() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            int result = defaultNioFlow.just(21)
                    .adapt(String::valueOf)
                    .submit(s -> s + "0")
                    .adapt(Integer::parseInt)
                    .join();

            assertEquals(210, result);
        }
    }

    @Test
    void onCompleteAfterAdaptReceivesTheAdaptedType() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<String> completed = new CopyOnWriteArrayList<>();
            defaultNioFlow.adapt(x -> "v" + x)
                    .onComplete(completed::add);

            defaultNioFlow.just(42);
            defaultNioFlow.join();

            assertEquals(List.of("v42"), completed);
        }
    }
}
