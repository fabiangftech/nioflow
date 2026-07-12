package dev.nioflow.application.facade;

import dev.nioflow.core.facade.FlowResult;
import dev.nioflow.core.facade.FlowResult.Completed;
import dev.nioflow.core.facade.FlowResult.Filtered;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.NioStep;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultNioFlowResultTest {

    /** The shared definition cuts negatives — type-preserving, as it must be. */
    private static NioFlow<Integer, String> ambiguousFlow() {
        NioFlow<Integer, String> flow = DefaultNioFlow.from(Integer.class);
        flow.filter(value -> value >= 0);
        return flow;
    }

    /** The per-request pipeline re-types it: zero maps to a GENUINE null value. */
    private static NioStep<String, String> ambiguous(NioFlow<Integer, String> flow, int input) {
        return flow.just(input)
                .adapt(value -> value == 0 ? null : "value:" + value);
    }

    @Test
    void executeResultDistinguishesFilteredFromGenuineNull() {
        NioFlow<Integer, String> flow = ambiguousFlow();

        // execute() cannot tell these two apart:
        assertNull(ambiguous(flow, -5).execute()); // filtered
        assertNull(ambiguous(flow, 0).execute());  // genuinely null

        // executeResult() can:
        assertInstanceOf(Filtered.class, ambiguous(flow, -5).executeResult());
        FlowResult<String> genuineNull = ambiguous(flow, 0).executeResult();
        assertInstanceOf(Completed.class, genuineNull);
        assertNull(((Completed<String>) genuineNull).value());
    }

    @Test
    void completedCarriesTheValueAndHelpersWork() {
        NioFlow<Integer, String> flow = ambiguousFlow();

        FlowResult<String> completed = ambiguous(flow, 7).executeResult();
        FlowResult<String> filtered = ambiguous(flow, -1).executeResult();

        assertEquals("value:7", switch (completed) {
            case Completed<String>(String value) -> value;
            case Filtered<String>() -> "cut";
        });
        assertFalse(completed.filtered());
        assertTrue(filtered.filtered());
        assertEquals("value:7", completed.orElse("fallback"));
        assertEquals("fallback", filtered.orElse("fallback"));
    }

    @Test
    void executeAndExecuteAsyncStillMapFilteredToNull() {
        NioFlow<Integer, String> flow = ambiguousFlow();

        assertNull(ambiguous(flow, -5).execute());
        assertNull(ambiguous(flow, -5).executeAsync().join());
        assertEquals("value:3", ambiguous(flow, 3).executeAsync().join());
    }

    // executeResult() without just() used to throw at runtime; it now lives only
    // on NioStep, so calling it on the shared definition is a compile error.
}
