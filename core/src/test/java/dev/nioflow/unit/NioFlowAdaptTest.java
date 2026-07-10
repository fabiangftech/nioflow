package dev.nioflow.unit;

import dev.nioflow.application.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NioFlowAdaptTest {

    @Test
    void adaptChangesThePipelineType() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            String result = nioFlow.just(21)
                    .submit(x -> x * 2)
                    .adapt(x -> "value=" + x)
                    .handle(String::toUpperCase)
                    .join();

            assertEquals("VALUE=42", result);
        }
    }

    @Test
    void adaptRoundTripsAcrossAsyncStages() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            int result = nioFlow.just(21)
                    .adapt(String::valueOf)
                    .submit(s -> s + "0")
                    .adapt(Integer::parseInt)
                    .join();

            assertEquals(210, result);
        }
    }

    @Test
    void onCompleteAfterAdaptReceivesTheAdaptedType() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            List<String> completed = new CopyOnWriteArrayList<>();
            nioFlow.adapt(x -> "v" + x)
                    .onComplete(completed::add);

            nioFlow.just(42);
            nioFlow.join();

            assertEquals(List.of("v42"), completed);
        }
    }
}
