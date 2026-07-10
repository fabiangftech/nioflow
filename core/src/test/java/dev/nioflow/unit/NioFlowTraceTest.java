package dev.nioflow.unit;

import dev.nioflow.application.facade.NioFlow;
import dev.nioflow.unit.utils.RecordingTracer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.*;

class NioFlowTraceTest {

    @Test
    void everyTransitionOfAValueIsTracedInOrder() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            RecordingTracer tracer = new RecordingTracer();
            nioFlow.trace(tracer)
                    .handle("double", x -> x * 2)
                    .when(x -> x > 5)
                    .then(lane -> lane
                            .submit("big", x -> x + 1))
                    .otherwise(lane -> lane
                            .handle(x -> x));

            nioFlow.just(4);
            nioFlow.join();

            assertEquals(List.of(
                    "0:injected:4",
                    "0:stage:double:4->8",
                    "0:lane:true",
                    "0:stage:big:8->9",
                    "0:completed:9"), tracer.events);
        }
    }

    @Test
    void dropsAndFanOutsAreTraced() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            RecordingTracer tracer = new RecordingTracer();
            nioFlow.trace(tracer)
                    .fanOut(x -> List.of(x, x + 1))
                    .filter(x -> x % 2 == 0);

            nioFlow.just(2); // splits into 2 and 3; 3 gets filtered
            nioFlow.join();

            assertTrue(tracer.events.contains("0:fannedOut:2"));
            assertTrue(tracer.events.contains("0:dropped:3"));
            assertTrue(tracer.events.contains("0:completed:2"));
        }
    }

    @Test
    void recoveriesAreTracedInsteadOfFailures() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            RecordingTracer tracer = new RecordingTracer();
            nioFlow.trace(tracer)
                    .submit("flaky", x -> {
                        throw new IllegalStateException("boom");
                    })
                    .onErrorResume(error -> -1);

            nioFlow.just(1);
            nioFlow.join();

            assertTrue(tracer.events.contains("0:stage:flaky:1->error"));
            assertTrue(tracer.events.contains("0:recovered:-1"));
            assertTrue(tracer.events.contains("0:completed:-1"));
            assertTrue(tracer.events.stream().noneMatch(event -> event.contains(":failed:")));
        }
    }

    @Test
    void terminalFailuresAreTraced() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            RecordingTracer tracer = new RecordingTracer();
            nioFlow.trace(tracer)
                    .handle(x -> {
                        throw new IllegalStateException("sync boom");
                    });

            nioFlow.just(1);
            assertThrows(CompletionException.class, nioFlow::join);

            assertTrue(tracer.events.contains("0:failed:sync boom"));
            assertTrue(tracer.events.stream().noneMatch(event -> event.contains(":completed:")));
        }
    }

    @Test
    void eachValueTracesUnderItsOwnSequence() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            RecordingTracer tracer = new RecordingTracer();
            nioFlow.trace(tracer)
                    .handle(x -> x * 10);

            nioFlow.just(1);
            nioFlow.just(2);
            nioFlow.join();

            assertTrue(tracer.events.contains("0:completed:10"));
            assertTrue(tracer.events.contains("1:completed:20"));
        }
    }
}
