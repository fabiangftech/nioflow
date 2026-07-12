package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The contract of NioFlow<I, T>: I is what just() takes, T is what execute()
 * returns. A new flow is always <I, I> — only adapt() moves T — and just()
 * rejects an input that is not an I, so an unchecked cast (a raw type, or a
 * framework injecting by generics) fails at the entry point instead of
 * blowing up as a ClassCastException inside a worker.
 */
class DefaultNioFlowTypeContractTest {

    @Test
    void aNewFlowStartsWithTheOutputTypeEqualToTheInputType() {
        // from() can only produce <I, I>: the compiler would reject
        // NioFlow<Integer, String> here, which is exactly the point.
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        assertEquals(7, flow.just(7).execute());
    }

    @Test
    void adaptIsWhatMovesTheOutputType() {
        // The flow promises Integer in, String out. The per-request pipeline is
        // what keeps that promise, and the compiler checks every step of it:
        // drop the adapt and this method no longer returns a String.
        NioFlow<Integer, String> flow = DefaultNioFlow.from(Integer.class);

        String result = flow.just(7)
                .handle("double", value -> value * 2)   // still an Integer here
                .adapt(value -> "n=" + value)           // Integer -> String
                .execute();

        assertEquals("n=14", result);
    }

    @Test
    void justRejectsAnInputThatIsNotAnInstanceOfI() {
        // Simulates what a framework does when it injects a NioFlow<?, ?> bean
        // into a differently-typed field: the cast is unchecked, so the wrong
        // value reaches just().
        NioFlow<?, ?> erased = DefaultNioFlow.from(Integer.class);
        @SuppressWarnings("unchecked")
        NioFlow<Object, Object> lying = (NioFlow<Object, Object>) erased;

        IllegalArgumentException failure =
                assertThrows(IllegalArgumentException.class, () -> lying.just("not an int"));

        assertTrue(failure.getMessage().contains("java.lang.Integer"), failure.getMessage());
        assertTrue(failure.getMessage().contains("java.lang.String"), failure.getMessage());
    }

    @Test
    void justAllRejectsAWrongInputToo() {
        NioFlow<?, ?> erased = DefaultNioFlow.from(String.class);
        @SuppressWarnings("unchecked")
        NioFlow<Object, Object> lying = (NioFlow<Object, Object>) erased;

        assertThrows(IllegalArgumentException.class, () -> lying.justAll(List.of("ok", 42)));
    }

    @Test
    void primitiveTokensValidateAgainstTheirWrapper() {
        // from(int.class) hands back DefaultNioFlow<Integer, Integer>; the check
        // must box, because int.class.isInstance(5) is false.
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(int.class);

        assertEquals(10, flow.just(5).handle(value -> value * 2).execute());
    }

    @Test
    void aNullInputIsAllowed() {
        NioFlow<String, String> flow = DefaultNioFlow.from(String.class);

        assertNull(flow.just(null).execute());
    }

    @Test
    void theInputTypeIsRequired() {
        assertThrows(IllegalArgumentException.class, () -> DefaultNioFlow.from(null));
    }

    @Test
    void subtypesOfTheInputTypeAreAccepted() {
        NioFlow<Number, Number> flow = DefaultNioFlow.from(Number.class);

        assertEquals(3, flow.just(3).execute());
        assertEquals(2.5, flow.just(2.5).execute());
    }

    /**
     * create() is the escape hatch for the places where I cannot be named: no
     * Class token means no entry-point check, so anything goes in and a
     * mismatch only surfaces inside the stage that touches the value.
     */
    @Test
    void createWithoutATokenAcceptsAnyInput() {
        DefaultNioFlow<Object, Object> flow = DefaultNioFlow.create();
        flow.handle("tag", value -> value + "!");

        // Nothing to check against, so an Integer goes through a flow another
        // caller may believe is a String flow — that is what the token buys you.
        assertEquals("a!", flow.just("a").execute());
        assertEquals("7!", flow.just(7).execute());
    }

    @Test
    void createOverAGivenEngineSharesThatEngine() {
        DefaultNioEngine engine = new DefaultNioEngine();
        DefaultNioFlow<Object, Object> flow = DefaultNioFlow.create(engine);
        flow.handle("tag", value -> value + "!");

        assertEquals("a!", flow.just("a").execute());
        assertEquals(1, engine.chain().size());
        engine.shutdown(Duration.ofMillis(100));
    }

    /** close() is the lifecycle hook of the root flow: it shuts its engine down. */
    @Test
    void closeShutsTheEngineDownAndRejectsFurtherWork() {
        DefaultNioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);
        flow.handle("double", value -> value * 2);
        assertEquals(4, flow.just(2).execute());

        flow.close();

        assertThrows(CompletionException.class, () -> flow.just(2).execute());
    }

    /** Every primitive token validates against its wrapper, not against the primitive. */
    @Test
    void everyPrimitiveTokenValidatesAgainstItsWrapper() {
        assertEquals(true, DefaultNioFlow.from(boolean.class).just(true).execute());
        assertEquals((byte) 1, DefaultNioFlow.from(byte.class).just((byte) 1).execute());
        assertEquals('c', DefaultNioFlow.from(char.class).just('c').execute());
        assertEquals((short) 2, DefaultNioFlow.from(short.class).just((short) 2).execute());
        assertEquals(3, DefaultNioFlow.from(int.class).just(3).execute());
        assertEquals(4L, DefaultNioFlow.from(long.class).just(4L).execute());
        assertEquals(5.0f, DefaultNioFlow.from(float.class).just(5.0f).execute());
        assertEquals(6.0, DefaultNioFlow.from(double.class).just(6.0).execute());

        // void.class has no wrapper to validate against: it degrades to Object,
        // so the entry check accepts anything instead of rejecting everything.
        NioFlow<?, ?> erased = DefaultNioFlow.from(void.class);
        @SuppressWarnings("unchecked")
        NioFlow<Object, Object> voidFlow = (NioFlow<Object, Object>) erased;

        assertEquals("anything", voidFlow.just("anything").execute());
    }
}
