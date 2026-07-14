package dev.nioflow.infrastructure.reactive;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The reactive face of the ninth link: {@code handleMonoAsync} is a
 * {@code handleMono} that does not park a worker — {@code mono.toFuture()} into
 * an AsyncStage, and the engine holds a future instead of a thread.
 *
 * <p>What earns it its place is the second test: the ENGINE's timeout cancels
 * the subscription. With {@code handleMono} the two were different mechanisms
 * with different reach (a stage timeout abandons the parked worker; only
 * {@code mono.timeout} could cancel). Here they are one.
 */
class ReactiveAsyncStageTest {

    private NioEngine engine;
    private ReactiveFlow<Integer, Integer> flow;

    @BeforeEach
    void setUp() {
        engine = new DefaultNioEngine();
        flow = Reactive.flow(DefaultNioFlow.from(Integer.class, engine));
    }

    @AfterEach
    void tearDown() {
        engine.shutdown(Duration.ofSeconds(1));
    }

    @Test
    void aMonoBecomesAStageThatHoldsNoThread() {
        Mono<Integer> mono = flow.just(10)
                .handleMonoAsync("double", value -> Mono.just(value * 2))
                .handle("plus-one", value -> value + 1)
                .executeMono();

        StepVerifier.create(mono).expectNext(21).verifyComplete();
    }

    /** The cancellation dividend, and the reason this step exists at all. */
    @Test
    void theEngineTimeoutCancelsTheSubscription() throws Exception {
        CountDownLatch cancelled = new CountDownLatch(1);

        Mono<Integer> mono = flow.just(1)
                .handleMonoAsync("hangs", value -> Mono.<Integer>never().doOnCancel(cancelled::countDown),
                        Duration.ofMillis(50))
                .recover(error -> error instanceof TimeoutException ? -1 : -2)
                .executeMono();

        StepVerifier.create(mono).expectNext(-1).verifyComplete();
        assertTrue(cancelled.await(2, TimeUnit.SECONDS),
                "the engine's timeout must cancel the Mono — reactor-netty releases the connection on cancel");
    }

    /** The flow's defaultBudget covers it too: an unbudgeted remote call is the bug. */
    @Test
    void theDefaultBudgetAppliesToTheAsyncStage() throws Exception {
        CountDownLatch cancelled = new CountDownLatch(1);
        ReactiveFlow<Integer, Integer> budgeted = Reactive.<Integer, Integer>flow(
                DefaultNioFlow.from(Integer.class, engine)).defaultBudget(Duration.ofMillis(50));

        Mono<Integer> mono = budgeted.just(1)
                .handleMonoAsync("hangs", value -> Mono.<Integer>never().doOnCancel(cancelled::countDown))
                .recover(error -> -1)
                .executeMono();

        StepVerifier.create(mono).expectNext(-1).verifyComplete();
        assertTrue(cancelled.await(2, TimeUnit.SECONDS), "the default budget must reach the async stage");
    }

    @Test
    void itFailsThroughRecoverLikeAnyOtherStage() {
        Mono<Integer> mono = flow.just(1)
                .handleMonoAsync("remote", value -> Mono.error(new IllegalStateException("down")))
                .recover(error -> error instanceof IllegalStateException ? -1 : -2)
                .executeMono();

        StepVerifier.create(mono).expectNext(-1).verifyComplete();
    }

    /** It re-types like adaptMono, and inside a lane like every other step. */
    @Test
    void itReTypesAndItRunsInsideALane() {
        AtomicBoolean laneRan = new AtomicBoolean();

        Mono<String> mono = flow.just(2)
                .when(value -> value % 2 == 0)
                .then(lane -> Reactive.lane(lane).handleMonoAsync("even", value -> {
                    laneRan.set(true);
                    return Mono.just(value * 10);
                }))
                .otherwise(lane -> lane.handle(value -> -value))
                .adaptMonoAsync(value -> Mono.just("value=" + value))
                .executeMono();

        StepVerifier.create(mono).expectNext("value=20").verifyComplete();
        assertTrue(laneRan.get(), "the async step must be reachable inside a branch");
    }

    /**
     * Nothing runs until somebody subscribes — the async stage did not change
     * what executeMono() promises.
     */
    @Test
    void itStaysLazyPerSubscription() {
        AtomicBoolean subscribed = new AtomicBoolean();

        Mono<Integer> mono = flow.just(1)
                .handleMonoAsync("remote", value -> Mono.fromCallable(() -> {
                    subscribed.set(true);
                    return value;
                }))
                .executeMono();

        assertTrue(!subscribed.get(), "assembling a Mono must not run the pipeline");
        StepVerifier.create(mono).expectNext(1).verifyComplete();
        assertTrue(subscribed.get());
    }
}
