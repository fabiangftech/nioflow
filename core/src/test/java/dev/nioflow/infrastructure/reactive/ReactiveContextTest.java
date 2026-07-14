package dev.nioflow.infrastructure.reactive;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.Context.Key;
import dev.nioflow.core.facade.NioEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The context bridge: keys declared ONCE on the flow, seeded from Reactor's
 * subscriber context on every subscription.
 *
 * <p>The line the design draws is declared-and-automatic, never
 * discovered-and-automatic — so the tests that matter most are the ones about
 * what does NOT cross (an undeclared key) and about WHEN it crosses (per
 * subscription, never at assembly).
 */
class ReactiveContextTest {

    private static final Key<String> TRACE = Key.of("traceId");
    private static final Key<String> TENANT = Key.of("tenant");

    private NioEngine engine;
    private ReactiveFlow<String, String> flow;

    @BeforeEach
    void setUp() {
        engine = new DefaultNioEngine();
        flow = Reactive.<String, String>flow(DefaultNioFlow.from(String.class, engine)).propagate(TRACE, TENANT);
    }

    @AfterEach
    void tearDown() {
        engine.shutdown(Duration.ofSeconds(1));
    }

    /** A stage reads what the caller knew, and no controller wrote the plumbing. */
    @Test
    void aDeclaredKeyCrossesFromTheSubscriberContext() {
        Mono<String> mono = flow.just("order-1")
                .handleContextual("charge", (value, ctx) -> value + " @" + ctx.get(TRACE))
                .executeMono()
                .contextWrite(Context.of("traceId", "abc-123"));

        StepVerifier.create(mono).expectNext("order-1 @abc-123").verifyComplete();
    }

    /**
     * Absence is a DEFINED behavior, not a mystery: the key is not seeded, the
     * stage reads null (no exception, no null entry), and the flow runs on.
     */
    @Test
    void aDeclaredKeyTheSubscriberContextDoesNotCarryIsSimplyNotSeeded() {
        Mono<String> mono = flow.just("order-1")
                .handleContextual("read", (value, ctx) ->
                        value + " trace=" + ctx.get(TRACE) + " tenant=" + ctx.getOrDefault(TENANT, "none"))
                .executeMono()
                .contextWrite(Context.of("traceId", "abc-123"));

        StepVerifier.create(mono).expectNext("order-1 trace=abc-123 tenant=none").verifyComplete();
    }

    /**
     * The whitelist, and the reason this method exists at all: nothing crosses
     * that a person did not write down in the config.
     */
    @Test
    void anUndeclaredKeyDoesNotCross() {
        Key<String> secret = Key.of("authorization");

        Mono<String> mono = flow.just("order-1")
                .handleContextual("read", (value, ctx) -> value + " auth=" + ctx.get(secret))
                .executeMono()
                .contextWrite(Context.of("authorization", "Bearer hunter2"));

        StepVerifier.create(mono).expectNext("order-1 auth=null").verifyComplete();
    }

    /**
     * The eager trap, in the one place it would hurt: seeding at ASSEMBLY would
     * hand every subscriber the trace id of whoever assembled the Mono.
     */
    @Test
    void theContextIsReadPerSubscriptionNotPerAssembly() {
        Mono<String> mono = flow.just("order-1")
                .handleContextual("read", (value, ctx) -> ctx.get(TRACE))
                .executeMono();

        StepVerifier.create(mono.contextWrite(Context.of("traceId", "first")))
                .expectNext("first").verifyComplete();
        StepVerifier.create(mono.contextWrite(Context.of("traceId", "second")))
                .expectNext("second").verifyComplete();
    }

    /** Same reason, other direction: a re-subscription re-reads the context. */
    @Test
    void everyRetryAttemptReSeedsFromTheContext() {
        AtomicInteger attempts = new AtomicInteger();

        Mono<String> mono = flow.just("order-1")
                .handleContextual("flaky", (value, ctx) -> {
                    if (attempts.incrementAndGet() < 3) {
                        throw new IllegalStateException("not yet");
                    }
                    return ctx.get(TRACE) + "#" + attempts.get();
                })
                .executeMono()
                .retry(2)
                .contextWrite(Context.of("traceId", "abc-123"));

        StepVerifier.create(mono).expectNext("abc-123#3").verifyComplete();
    }

    /** with() is what THIS pipeline declared; the bridge only carries. */
    @Test
    void anExplicitWithWinsOverThePropagatedKeyOfTheSameName() {
        Mono<String> mono = flow.just("order-1")
                .with(TRACE, "explicit")
                .handleContextual("read", (value, ctx) -> ctx.get(TRACE))
                .executeMono()
                .contextWrite(Context.of("traceId", "propagated"));

        StepVerifier.create(mono).expectNext("explicit").verifyComplete();
    }

    /** propagate() unused costs exactly zero: no defer, no map, nothing seeded. */
    @Test
    void aFlowThatDeclaresNothingSeedsNothing() {
        ReactiveFlow<String, String> plain = Reactive.flow(DefaultNioFlow.from(String.class, engine));

        Mono<String> mono = plain.just("order-1")
                .handleContextual("read", (value, ctx) -> value + " trace=" + ctx.get(TRACE))
                .executeMono()
                .contextWrite(Context.of("traceId", "abc-123"));

        StepVerifier.create(mono).expectNext("order-1 trace=null").verifyComplete();
    }

    /** The bridge rides into a branch's lane, and into executeFlux's pipeline. */
    @Test
    void theSeededContextReachesLanesAndStreamingTails() {
        Flux<String> flux = flow.just("order-1")
                .when(value -> value.startsWith("order"))
                .then(lane -> lane.handleContextual("in-lane", (value, ctx) -> value + " @" + ctx.get(TRACE)))
                .otherwise(lane -> lane.handle(value -> value))
                .executeFlux(value -> Flux.just(value + "!", value + "?"))
                .contextWrite(Context.of("traceId", "abc-123"));

        StepVerifier.create(flux)
                .expectNext("order-1 @abc-123!", "order-1 @abc-123?")
                .verifyComplete();
    }

    /** A pipe() over a Flux is executions of the same flow: they seed too. */
    @Test
    void everyElementOfAPipeIsSeeded() {
        Flux<String> flux = Flux.just("a", "b")
                .transform(flow.pipeOrdered(2, (id, step) -> step
                        .handleContextual("read", (value, ctx) -> value + "@" + ctx.get(TRACE))))
                .contextWrite(Context.of("traceId", "abc-123"));

        StepVerifier.create(flux).expectNext("a@abc-123", "b@abc-123").verifyComplete();
    }

    /** A whitelist of nothing is a mistake, and silence would never tell you. */
    @Test
    void propagateWithoutKeysIsRejectedAtBuildTime() {
        ReactiveFlow<String, String> plain = Reactive.flow(DefaultNioFlow.from(String.class, engine));

        assertEquals("propagate() needs at least one key: it declares WHAT crosses from the subscriber"
                        + " context, and nothing crosses that is not named",
                assertThrows(IllegalArgumentException.class, plain::propagate).getMessage());
        assertThrows(IllegalArgumentException.class, () -> plain.propagate((Key<?>) null));
    }
}
