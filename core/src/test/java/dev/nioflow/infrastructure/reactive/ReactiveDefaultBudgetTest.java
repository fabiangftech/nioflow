package dev.nioflow.infrastructure.reactive;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The default budget — the thing standing between a hung socket and a virtual
 * worker parked for the life of the JVM.
 *
 * <p>{@code Blocking.await} is {@code mono.block()}: with no budget anywhere, a
 * Mono that never completes parks its worker forever, and the engine has no
 * cancellation to take it back. So the tests here are about a wait that ENDS: it
 * ends as a TimeoutException on the recovery path, the subscription is cancelled
 * (which is what releases a real connection), and it ends in a lane and inside a
 * fork too — because a remote call there is still a remote call.
 */
class ReactiveDefaultBudgetTest {

    private NioEngine engine;
    private ReactiveFlow<Integer, Integer> budgeted;

    @BeforeEach
    void setUp() {
        engine = new DefaultNioEngine();
        budgeted = Reactive.<Integer, Integer>flow(DefaultNioFlow.from(Integer.class, engine))
                .defaultBudget(Duration.ofMillis(50));
    }

    @AfterEach
    void tearDown() {
        engine.shutdown(Duration.ofSeconds(2));
    }

    @Test
    void aHungMonoCancelsAndFreesItsWorkerInsteadOfParkingForever() throws InterruptedException {
        // The leak, pinned: without the default budget this Mono never completes,
        // the worker never comes back, and this test hangs. With it, the wait ends
        // — and the subscription is CANCELLED, which is what makes reactor-netty
        // release the connection rather than leave it hanging on the pool.
        CountDownLatch cancelled = new CountDownLatch(1);
        AtomicReference<Throwable> seen = new AtomicReference<>();

        Integer result = budgeted.just(1)
                .handleMono("hung-remote", value -> Mono.<Integer>never().doOnCancel(cancelled::countDown))
                .recover(error -> {
                    seen.set(error);
                    return -1;
                })
                .executeMono()
                .block(Duration.ofSeconds(5));

        assertEquals(-1, result);
        assertInstanceOf(TimeoutException.class, seen.get());
        assertTrue(cancelled.await(2, TimeUnit.SECONDS), "the subscription was never cancelled");

        // And the workers really did come back: the flow keeps serving.
        for (int request = 0; request < 20; request++) {
            assertEquals(request * 2, budgeted.just(request)
                    .handleMono("remote", value -> Mono.just(value * 2))
                    .executeMono()
                    .block(Duration.ofSeconds(5)));
        }
    }

    @Test
    void anExplicitBudgetOverridesTheDefault() {
        // The default is 50ms and this call takes 200ms: only the explicit budget
        // can save it. (The reverse — an explicit budget SHORTER than a generous
        // default — is the same rule, and is what the hung test above relies on.)
        Integer result = budgeted.just(3)
                .handleMono("slow-but-legitimate",
                        value -> Mono.delay(Duration.ofMillis(200)).map(ignored -> value * 5),
                        Duration.ofSeconds(5))
                .executeMono()
                .block(Duration.ofSeconds(10));

        assertEquals(15, result);
    }

    @Test
    void aStepWithNoNetworkInItIsUnaffected() {
        Integer result = budgeted.just(2)
                .handleMono("in-memory", value -> Mono.just(value + 1))
                .adaptMono(value -> Mono.just(value * 10))
                .adaptFlux(value -> Flux.just(value, value))
                .adapt(List::size)
                .executeMono()
                .block(Duration.ofSeconds(5));

        assertEquals(2, result);
    }

    @Test
    void theDefaultBudgetCoversAdaptMonoAndFanOutMonoToo() {
        AtomicReference<Throwable> fromAdapt = new AtomicReference<>();
        AtomicReference<Throwable> fromFanOut = new AtomicReference<>();

        Integer adapted = budgeted.just(1)
                .adaptMono(value -> Mono.<String>never())
                .recover(error -> {
                    fromAdapt.set(error);
                    return "recovered";
                })
                .adapt(String::length)
                .executeMono()
                .block(Duration.ofSeconds(5));

        List<Function<Integer, Mono<Integer>>> branches = List.of(
                Mono::just,
                value -> Mono.never());

        Integer fannedOut = budgeted.just(1)
                .fanOutMono("enrich", branches, List::size)
                .recover(error -> {
                    fromFanOut.set(error);
                    return -1;
                })
                .executeMono()
                .block(Duration.ofSeconds(5));

        assertEquals("recovered".length(), adapted);
        assertInstanceOf(TimeoutException.class, fromAdapt.get());
        assertEquals(-1, fannedOut);
        assertInstanceOf(TimeoutException.class, rootOf(fromFanOut.get()));
    }

    @Test
    void theDefaultBudgetReachesIntoALane() {
        AtomicReference<Throwable> seen = new AtomicReference<>();

        Integer result = budgeted.just(10)
                .when(value -> value > 5)
                .then(lane -> Reactive.lane(lane)
                        .handleMono("hung-remote", value -> Mono.never())
                        .recover(error -> {
                            seen.set(error);
                            return -1;
                        }))
                .executeMono()
                .block(Duration.ofSeconds(5));

        assertEquals(-1, result);
        assertInstanceOf(TimeoutException.class, seen.get());
    }

    @Test
    void theDefaultBudgetReachesIntoAForkBody() throws InterruptedException {
        // A fork's failure never reaches the caller's future — so a hung Mono in
        // there would leak a worker with nobody watching. Its recover() is the only
        // witness, and the budget is what lets it fire at all.
        CountDownLatch recovered = new CountDownLatch(1);
        AtomicReference<Throwable> seen = new AtomicReference<>();

        Integer result = budgeted.just(7)
                .fork("notify", lane -> Reactive.lane(lane)
                        .handleMono("hung-remote", value -> Mono.never())
                        .recover(error -> {
                            seen.set(error);
                            recovered.countDown();
                            return -1;
                        }))
                .executeMono()
                .block(Duration.ofSeconds(5));

        assertEquals(7, result, "the main line never waits for the fork");
        assertTrue(recovered.await(5, TimeUnit.SECONDS), "the fork's Mono was never budgeted: it is still parked");
        assertInstanceOf(TimeoutException.class, seen.get());
    }

    @Test
    void aLaneTakesAnExplicitBudgetOnAdaptMono() {
        // The overload that was missing: a re-typing remote call inside a branch
        // could not take a budget without dropping to handleMono.
        ReactiveFlow<Integer, Integer> plain = Reactive.flow(DefaultNioFlow.from(Integer.class, engine));
        AtomicReference<Throwable> seen = new AtomicReference<>();

        Integer result = plain.just(10)
                .when(value -> value > 5)
                .then(lane -> Reactive.lane(lane)
                        .<String>adaptMono(value -> Mono.never(), Duration.ofMillis(50))
                        .recover(error -> {
                            seen.set(error);
                            return "recovered";
                        })
                        .adapt(String::length))
                .executeMono()
                .block(Duration.ofSeconds(5));

        assertEquals("recovered".length(), result);
        assertInstanceOf(TimeoutException.class, seen.get());
    }

    @Test
    void theSameBudgetedFlowServesConcurrentExecutionsIndependently() {
        AtomicInteger recovered = new AtomicInteger();

        List<Integer> results = Flux.range(1, 8)
                .flatMap(value -> budgeted.just(value)
                        .handleMono("half-hung", input -> input % 2 == 0 ? Mono.never() : Mono.just(input))
                        .recover(error -> {
                            recovered.incrementAndGet();
                            return -1;
                        })
                        .executeMono(), 8)
                .sort()
                .collectList()
                .block(Duration.ofSeconds(10));

        assertEquals(List.of(-1, -1, -1, -1, 1, 3, 5, 7), results);
        assertEquals(4, recovered.get());
    }

    @Test
    void aBudgetThatIsNotAPositiveDurationIsRejectedAtBuildTime() {
        ReactiveFlow<Integer, Integer> flow = Reactive.flow(DefaultNioFlow.from(Integer.class, engine));
        Duration negative = Duration.ofMillis(-1);

        assertThrows(IllegalArgumentException.class, () -> flow.defaultBudget(null));
        assertThrows(IllegalArgumentException.class, () -> flow.defaultBudget(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> flow.defaultBudget(negative));
    }

    private static Throwable rootOf(Throwable error) {
        Throwable cause = error;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }
}
