package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Cancellable;
import dev.nioflow.core.model.Filter;
import dev.nioflow.core.model.FlowSignal;
import dev.nioflow.core.model.Link;
import dev.nioflow.core.model.OverflowPolicy;
import dev.nioflow.core.model.Stage;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * RFC 0031: {@code capacity}/{@code OverflowPolicy} must bound the request/
 * response path ({@code call}/{@code callCancellable}) — the path the whole
 * reactive facade runs on — not only the {@code inject}/{@code await} queue.
 * Every terminal (value, FILTERED, CANCELLED, failure) must free the permit,
 * or a bounded engine would leak admission slots until it wedges. Each future
 * is joined (or the call rejects synchronously), so a leaked permit surfaces as
 * an assertion failure, never a silent hang.
 */
class DefaultNioEngineBackpressureCallTest {

    private static Stage identity() {
        return new Stage("identity", Function.identity(), false, null, null, List.of());
    }

    /** Parks the worker until the gate opens, keeping its call in-flight (permit held). */
    private static Stage gated(CountDownLatch gate) {
        return new Stage("gated", value -> {
            try {
                gate.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return value;
        }, false, null, null, List.of());
    }

    private static Stage boom() {
        return new Stage("boom", value -> {
            throw new IllegalStateException("boom");
        }, false, null, null, List.of());
    }

    @Test
    void unboundedByDefaultAdmitsEveryCall() {
        var engine = new DefaultNioEngine();
        engine.append(identity());

        var calls = new ArrayList<CompletableFuture<Object>>();
        for (int i = 0; i < 200; i++) {
            calls.add(engine.call(i, null));
        }
        for (int i = 0; i < 200; i++) {
            assertEquals(i, calls.get(i).join());
        }
        engine.shutdown(Duration.ofMillis(200));
    }

    @Test
    void failPolicyRejectsCallsBeyondCapacity() {
        var engine = new DefaultNioEngine(1, OverflowPolicy.FAIL);
        var gate = new CountDownLatch(1);
        engine.append(gated(gate));

        CompletableFuture<Object> first = engine.call("a", null); // takes the one permit
        assertThrows(RejectedExecutionException.class, () -> engine.call("b", null));

        gate.countDown();
        assertEquals("a", first.join());
        // The permit frees on the terminal, so a later call is admitted again.
        assertEquals("c", engine.call("c", null).join());
        engine.shutdown(Duration.ofMillis(200));
    }

    @Test
    void dropPolicyFailsTheFutureAndReportsBeyondCapacity() {
        var engine = new DefaultNioEngine(1, OverflowPolicy.DROP);
        var dropped = new AtomicReference<Throwable>();
        engine.addErrorHandler(dropped::set);
        var gate = new CountDownLatch(1);
        engine.append(gated(gate));

        CompletableFuture<Object> first = engine.call("a", null);
        CompletableFuture<Object> second = engine.call("b", null); // dropped: the future fails

        CompletionException ex = assertThrows(CompletionException.class, second::join);
        assertInstanceOf(RejectedExecutionException.class, ex.getCause());
        assertInstanceOf(RejectedExecutionException.class, dropped.get());

        gate.countDown();
        assertEquals("a", first.join());
        engine.shutdown(Duration.ofMillis(200));
    }

    @Test
    void callCancellableAlsoAdmits() {
        var engine = new DefaultNioEngine(1, OverflowPolicy.FAIL);
        var gate = new CountDownLatch(1);
        engine.append(gated(gate));

        List<Link> chain = engine.chain();
        Cancellable<Object> first = engine.callCancellable("a", null, chain, null);
        assertThrows(RejectedExecutionException.class,
                () -> engine.callCancellable("b", null, chain, null));

        gate.countDown();
        assertEquals("a", first.future().join());
        engine.shutdown(Duration.ofMillis(200));
    }

    @Test
    void injectAndCallShareTheSameBound() {
        var engine = new DefaultNioEngine(1, OverflowPolicy.FAIL);
        engine.append(identity());

        engine.inject("a"); // takes the one permit (held until await collects)
        assertThrows(RejectedExecutionException.class, () -> engine.call("b", null));

        assertEquals("a", engine.await()); // frees inject's permit
        assertEquals("c", engine.call("c", null).join()); // now admitted
        engine.shutdown(Duration.ofMillis(200));
    }

    // ── the permit frees on EVERY terminal (RFC 0031) ────────────────────────
    // Each test runs one call to a given terminal on a capacity-1 FAIL engine,
    // then a second call: if the first terminal leaked its permit, the second
    // would throw RejectedExecutionException instead of being admitted.

    @Test
    void permitFreesOnCompletedTerminal() {
        var engine = new DefaultNioEngine(1, OverflowPolicy.FAIL);
        engine.append(identity());

        assertEquals("x", engine.call("x", null).join());
        assertEquals("y", engine.call("y", null).join());
        engine.shutdown(Duration.ofMillis(200));
    }

    @Test
    void permitFreesOnFilteredTerminal() {
        var engine = new DefaultNioEngine(1, OverflowPolicy.FAIL);
        engine.append(new Filter(value -> false, List.of())); // cuts every value

        engine.call("x", null).join(); // terminal is FILTERED
        // Admitted again (call() does not throw RejectedExecutionException): the
        // filtered terminal freed the permit. The value is the FILTERED sentinel.
        assertEquals(FlowSignal.FILTERED, engine.call("y", null).join());
        engine.shutdown(Duration.ofMillis(200));
    }

    @Test
    void permitFreesOnFailedTerminal() {
        var engine = new DefaultNioEngine(1, OverflowPolicy.FAIL);
        engine.append(boom());

        CompletableFuture<Object> first = engine.call("x", null);
        assertThrows(CompletionException.class, first::join);
        // The second call is admitted (this line does not throw REE); it then
        // fails on boom like the first — proving the failed terminal freed the permit.
        CompletableFuture<Object> second = engine.call("y", null);
        assertThrows(CompletionException.class, second::join);
        engine.shutdown(Duration.ofMillis(200));
    }

    @Test
    void permitFreesOnCancelledTerminal() {
        var engine = new DefaultNioEngine(1, OverflowPolicy.FAIL);
        var gate = new CountDownLatch(1);
        engine.append(gated(gate));

        Cancellable<Object> first = engine.callCancellable("a", null, engine.chain(), null);
        first.cancel();
        first.future().join(); // CANCELLED terminal ran → permit released before this returns
        gate.countDown();      // release the (now result-less) worker if it parked

        assertEquals("b", engine.call("b", null).join()); // admitted → permit was freed on cancel
        engine.shutdown(Duration.ofMillis(200));
    }
}
