package dev.nioflow.infrastructure.reactive;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * RFC 0027 — a value-carrying reactive step that returns {@code Mono.empty()}
 * fails with {@link EmptyMonoException} instead of injecting a silent null, in
 * every position and on both the blocking and the async ({@code preferAsync})
 * path. The blocking mid-chain cases live in {@code ReactiveMonoSemanticsTest};
 * this pins the async parity, the explicit async steps and a lane.
 */
class ReactiveEmptyMonoTest {

    private NioEngine engine;

    @BeforeEach
    void setUp() {
        engine = new DefaultNioEngine();
    }

    @AfterEach
    void tearDown() {
        engine.shutdown(Duration.ofSeconds(1));
    }

    private ReactiveFlow<Integer, Integer> flow() {
        return Reactive.flow(DefaultNioFlow.from(Integer.class, engine));
    }

    @Test
    void thePreferAsyncPathFailsOnEmptyExactlyLikeTheBlockingPath() {
        // Same chain, both routings: an empty handleMono is an EmptyMonoException
        // either way, so switching to preferAsync never silently changes the
        // meaning of "empty".
        assertInstanceOf(EmptyMonoException.class, emptyOutcome(flow()));
        assertInstanceOf(EmptyMonoException.class, emptyOutcome(flow().preferAsync()));
    }

    private static Throwable emptyOutcome(ReactiveFlow<Integer, Integer> flow) {
        AtomicReference<Throwable> recovered = new AtomicReference<>();
        AtomicBoolean nextRan = new AtomicBoolean(false);
        flow.just(1)
                .handleMono("lookup", value -> Mono.empty())
                .handle("next", value -> {
                    nextRan.set(true);
                    return value;
                })
                .recover(error -> {
                    recovered.set(error);
                    return -1;
                })
                .executeMono()
                .block(Duration.ofSeconds(5));
        assertFalse(nextRan.get(), "the next stage must not run on an empty value");
        return recovered.get();
    }

    @Test
    void anEmptyHandleMonoAsyncFails() {
        AtomicReference<Throwable> recovered = new AtomicReference<>();

        Integer result = flow().just(1)
                .handleMonoAsync("lookup", value -> Mono.empty())
                .recover(error -> {
                    recovered.set(error);
                    return -1;
                })
                .executeMono()
                .block(Duration.ofSeconds(5));

        assertEquals(-1, result);
        assertInstanceOf(EmptyMonoException.class, recovered.get());
    }

    @Test
    void anEmptyAdaptMonoAsyncFails() {
        AtomicReference<Throwable> recovered = new AtomicReference<>();

        String result = flow().just(1)
                .adaptMonoAsync(value -> Mono.<String>empty())
                .recover(error -> {
                    recovered.set(error);
                    return "recovered";
                })
                .executeMono()
                .block(Duration.ofSeconds(5));

        assertEquals("recovered", result);
        assertInstanceOf(EmptyMonoException.class, recovered.get());
    }

    @Test
    void anEmptyMonoInsideALaneFails() {
        AtomicReference<Throwable> recovered = new AtomicReference<>();

        Integer result = flow().just(10)
                .when(value -> value > 5)
                .then(lane -> Reactive.lane(lane)
                        .handleMono("lane-lookup", value -> Mono.empty())
                        .recover(error -> {
                            recovered.set(error);
                            return -1;
                        }))
                .executeMono()
                .block(Duration.ofSeconds(5));

        assertEquals(-1, result);
        assertInstanceOf(EmptyMonoException.class, recovered.get());
    }

    @Test
    void aMonoThatEmitsAValueIsUnaffected() {
        // The guard only fires on empty: an ordinary emitting step is untouched.
        Integer result = flow().just(2)
                .handleMono("emits", value -> Mono.just(value * 3))
                .executeMono()
                .block(Duration.ofSeconds(5));

        assertEquals(6, result);
    }
}
