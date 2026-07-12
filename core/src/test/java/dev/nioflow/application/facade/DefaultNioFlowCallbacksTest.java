package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.NioStep;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * onComplete/onError in the fluent API. On the shared definition they tap
 * the engine handlers (every execution reports); on a just() execution they
 * are scoped to that execution. Both fire BEFORE the caller observes the
 * result, and a throwing callback never hangs or corrupts the flow.
 */
class DefaultNioFlowCallbacksTest extends EngineTestSupport {

    @Test
    void executionScopedCallbacksObserveOnlyTheirExecution() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("double", value -> value * 2);
        var first = new AtomicReference<Integer>();
        var second = new AtomicReference<Integer>();

        assertEquals(6, flow.just(3).onComplete(first::set).execute());
        assertEquals(10, flow.just(5).onComplete(second::set).execute());

        assertEquals(6, first.get());
        assertEquals(10, second.get());
    }

    @Test
    void onCompleteFiresBeforeExecuteReturns() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("identity", value -> value);
        var observed = new AtomicReference<Integer>();

        flow.just(7).onComplete(observed::set).execute();

        assertEquals(7, observed.get());
    }

    @Test
    void onErrorReceivesTheUnwrappedFailureAndExecuteStillThrows() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("boom", value -> {
            throw new IllegalStateException("boom");
        });
        var observed = new AtomicReference<Throwable>();

        NioStep<Integer, Integer> execution = flow.just(1).onError(observed::set);
        assertThrows(CompletionException.class, execution::execute);

        assertInstanceOf(IllegalStateException.class, observed.get());
        assertEquals("boom", observed.get().getMessage());
    }

    @Test
    void filteredExecutionObservesNullLikeExecute() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.filter(value -> value > 10);
        var observed = new AtomicReference<Integer>(-1);

        assertNull(flow.just(3).onComplete(observed::set).execute());

        assertNull(observed.get());
    }

    @Test
    void multipleCallbacksOnTheSameExecutionCompose() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("identity", value -> value);
        List<String> order = new CopyOnWriteArrayList<>();

        flow.just(1)
                .onComplete(value -> order.add("first"))
                .onComplete(value -> order.add("second"))
                .execute();

        assertEquals(List.of("first", "second"), order);
    }

    @Test
    void sharedFlowCallbacksObserveEveryExecution() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("double", value -> value * 2);
        List<Integer> completed = new CopyOnWriteArrayList<>();
        List<Throwable> failed = new CopyOnWriteArrayList<>();
        flow.onComplete(completed::add).onError(failed::add);

        assertEquals(4, flow.just(2).execute());
        assertEquals(8, flow.just(4).execute());

        assertEquals(List.of(4, 8), completed);
        assertTrue(failed.isEmpty());
    }

    @Test
    void sharedFlowOnErrorSeesInjectedFailuresToo() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("boom-on-two", value -> {
            if (value == 2) {
                throw new IllegalStateException("boom");
            }
            return value;
        });
        List<Throwable> failed = new CopyOnWriteArrayList<>();
        flow.onError(failed::add);

        flow.justAll(List.of(1, 2, 3));
        engine.await();
        assertThrows(CompletionException.class, engine::await);
        engine.await();

        assertEquals(1, failed.size());
        assertInstanceOf(IllegalStateException.class, failed.get(0));
    }

    @Test
    void throwingCompleteCallbackIsReportedToOnErrorAndNeverHangsTheCaller() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("identity", value -> value);
        var reported = new AtomicReference<Throwable>();
        flow.onComplete(value -> {
            throw new IllegalStateException("bad handler");
        }).onError(reported::set);

        // The caller still gets its result (with a hanging future this join
        // would time out) and the handler failure surfaces through onError.
        assertEquals(5, flow.just(5).executeAsync().orTimeout(5, TimeUnit.SECONDS).join());
        assertInstanceOf(IllegalStateException.class, reported.get());
        assertEquals("bad handler", reported.get().getMessage());
    }
}
