package dev.nioflow.stress;

import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Bug hunt: 10k concurrent executions through one shared branch (when/match).
 * Routing decisions live PER EXECUTION, on the boss; if one ever leaked across
 * requests, some value would come back routed down the wrong lane. Every result
 * must match its own input, exactly.
 */
class ConcurrentBranchRoutingStressTest {

    @Test
    void concurrentExecutionsNeverLeakRoutingDecisions() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);
        flow.when(value -> value % 2 == 0)
                .then(lane -> lane.handle("even", value -> value * 10))
                .otherwise(lane -> lane.handle("odd", value -> value * -1));

        var results = new ConcurrentHashMap<Integer, Integer>();
        try (var requests = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 10_000; i++) {
                int input = i;
                requests.execute(() -> results.put(input, flow.just(input).execute()));
            }
        }

        for (int i = 0; i < 10_000; i++) {
            int expected = i % 2 == 0 ? i * 10 : -i;
            assertEquals(expected, results.get(i), "input " + i + " took the wrong lane");
        }
    }
}
