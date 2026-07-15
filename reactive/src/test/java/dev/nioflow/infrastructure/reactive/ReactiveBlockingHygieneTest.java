package dev.nioflow.infrastructure.reactive;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The contract of {@code Blocking.await} — the one place a reactive stage turns a
 * Mono back into a plain value. What matters, and is easy to get wrong, is the
 * FAILURE path: a Mono's error must reach {@code recover()} as itself, with a
 * checked cause carried through a {@link CompletionException} and an unchecked
 * one rethrown directly (the {@code Exceptions.unwrap} the facade promises).
 */
class ReactiveBlockingHygieneTest {

    private NioEngine engine;
    private ReactiveFlow<Integer, Integer> flow;

    @BeforeEach
    void setUp() {
        engine = new DefaultNioEngine();
        flow = Reactive.flow(DefaultNioFlow.from(Integer.class, engine));
    }

    @AfterEach
    void tearDown() {
        engine.shutdown(Duration.ofSeconds(2));
    }

    @Test
    void aResolvedMonoYieldsItsValue() {
        assertEquals(42, Blocking.await(Mono.just(42)));
        assertEquals(7, Blocking.await(Mono.fromCallable(() -> 7)));
    }

    @Test
    void anEmptyMonoYieldsNull() {
        assertNull(Blocking.await(Mono.empty()));
    }

    @Test
    void anUncheckedFailureIsRethrownAsItself() {
        Mono<Integer> failing = Mono.error(new IllegalStateException("boom"));
        IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> Blocking.await(failing));
        assertEquals("boom", thrown.getMessage());
    }

    @Test
    void aCheckedFailureIsCarriedThroughACompletionException() {
        // The engine's recovery path unwraps a CompletionException, so a checked
        // cause reaches recover() as the real exception, not a Reactor wrapper.
        Mono<Integer> failing = Mono.error(new IOException("io down"));
        CompletionException thrown = assertThrows(CompletionException.class, () -> Blocking.await(failing));
        assertInstanceOf(IOException.class, thrown.getCause());
        assertEquals("io down", thrown.getCause().getMessage());
    }

    @Test
    void handleMonoOverASyncMonoFlowsAndRecovers() {
        Integer ok = flow.just(5)
                .handleMono("plus", value -> Mono.just(value + 1))
                .executeMono()
                .block();
        assertEquals(6, ok);

        Integer recovered = flow.just(5)
                .handleMono("boom", value -> Mono.error(new IllegalStateException("down")))
                .recover(error -> error instanceof IllegalStateException ? -1 : -2)
                .executeMono()
                .block();
        assertEquals(-1, recovered);
    }
}
