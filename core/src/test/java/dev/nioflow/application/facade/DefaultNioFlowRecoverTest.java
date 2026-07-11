package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultNioFlowRecoverTest {

    @Test
    void recoverContinuesTheFlowWithTheRecoveredValue() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        Integer result = flow.just(5)
                .handle("boom", value -> {
                    throw new IllegalStateException("db down");
                })
                .recover("fallback", error -> -1)
                .handle(value -> value * 10)
                .execute();

        assertEquals(-10, result); // fallback (-1) keeps flowing through the tail
    }

    @Test
    void recoverOnlyCatchesUpstreamFailures() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        var result = assertThrows(CompletionException.class, () -> flow.just(5)
                .recover("too-early", error -> -1)
                .handle("boom", value -> {
                    throw new IllegalStateException("db down");
                })
                .execute());

        assertEquals("db down", result.getCause().getMessage());
    }

    @Test
    void recoverInsideALaneOnlyAppliesToThatLane() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);
        flow.when(value -> value % 2 == 0)
                .then(lane -> lane
                        .handle("even-boom", value -> {
                            throw new IllegalStateException("even failed");
                        })
                        .recover("even-fallback", error -> 0))
                .otherwise(lane -> lane
                        .handle("odd-boom", value -> {
                            throw new IllegalStateException("odd failed");
                        }));

        // Even values fail and are recovered by their lane's recovery.
        assertEquals(0, flow.just(4).execute());
        // Odd values fail in a lane with no recovery: the failure propagates.
        var failure = assertThrows(CompletionException.class, () -> flow.just(3).execute());
        assertEquals("odd failed", failure.getCause().getMessage());
    }

    @Test
    void recoverOnSharedDefinitionAppliesToEveryExecution() {
        NioFlow<String, String> flow = DefaultNioFlow.from(String.class);
        flow.handle("risky", value -> {
            if (value.startsWith("bad")) {
                throw new IllegalStateException(value);
            }
            return value;
        }).recover(Throwable::getMessage);

        assertEquals("ok", flow.just("ok").handle(String::toLowerCase).execute());
        assertEquals("bad-input", flow.just("bad-input").handle(String::toLowerCase).execute());
    }
}
