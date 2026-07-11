package dev.nioflow.application.facade;

import dev.nioflow.core.facade.FlowResult;
import dev.nioflow.core.facade.FlowResult.Completed;
import dev.nioflow.core.facade.FlowResult.Filtered;
import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultNioFlowResultTest {

    /** Filter cuts negatives; zero maps to a GENUINE null value. */
    private static NioFlow<Integer, String> ambiguousFlow() {
        return DefaultNioFlow.<Integer>from(Integer.class)
                .filter(value -> value >= 0)
                .adapt(value -> value == 0 ? null : "value:" + value);
    }

    @Test
    void executeResultDistinguishesFilteredFromGenuineNull() {
        NioFlow<Integer, String> flow = ambiguousFlow();

        // execute() cannot tell these two apart:
        assertNull(flow.just(-5).execute()); // filtered
        assertNull(flow.just(0).execute());  // genuinely null

        // executeResult() can:
        assertInstanceOf(Filtered.class, flow.just(-5).executeResult());
        FlowResult<String> genuineNull = flow.just(0).executeResult();
        assertInstanceOf(Completed.class, genuineNull);
        assertNull(((Completed<String>) genuineNull).value());
    }

    @Test
    void completedCarriesTheValueAndHelpersWork() {
        NioFlow<Integer, String> flow = ambiguousFlow();

        FlowResult<String> completed = flow.just(7).executeResult();
        FlowResult<String> filtered = flow.just(-1).executeResult();

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

        assertNull(flow.just(-5).execute());
        assertNull(flow.just(-5).executeAsync().join());
        assertEquals("value:3", flow.just(3).executeAsync().join());
    }

    @Test
    void executeResultWithoutJustIsRejected() {
        NioFlow<String, String> flow = DefaultNioFlow.from(String.class);

        assertThrows(IllegalStateException.class, flow::executeResult);
    }
}
