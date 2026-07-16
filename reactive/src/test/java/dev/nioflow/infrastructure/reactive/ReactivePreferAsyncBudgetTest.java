package dev.nioflow.infrastructure.reactive;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RFC 0028 — closing the {@code preferAsync} no-budget leak.
 *
 * <p>Two properties. First, the load-bearing one that already holds: a
 * {@code defaultBudget} reaches the ASYNC path too, so a hung call there is
 * cancelled (execution and connection freed) instead of pinned forever — the
 * quiet, thread-less twin of the parked-worker leak. Second, the new guard:
 * {@code requireBudget()} turns an unbudgeted reactive step into a BUILD-TIME
 * error, in every position a reactive step can be declared — main line,
 * {@code just()} pipeline, a branch lane, a fork body.
 */
class ReactivePreferAsyncBudgetTest {

    private NioEngine engine;

    @BeforeEach
    void setUp() {
        engine = new DefaultNioEngine();
    }

    @AfterEach
    void tearDown() {
        engine.shutdown(Duration.ofSeconds(2));
    }

    private ReactiveFlow<Integer, Integer> flow() {
        return Reactive.flow(DefaultNioFlow.from(Integer.class, engine));
    }

    // ── the async leak, closed by the default budget ──

    @Test
    void preferAsyncWithADefaultBudgetCancelsAHungAsyncCallInsteadOfLeaking() {
        // preferAsync routes handleMono through the async (future-holding) path.
        // Without a budget reaching that path, a Mono that never completes would
        // pin the Execution and leak the connection — no parked worker to notice.
        // The default budget must reach it: the future is cancelled, the
        // subscription disposed, and recover() sees a TimeoutException.
        CountDownLatch cancelled = new CountDownLatch(1);
        AtomicReference<Throwable> seen = new AtomicReference<>();

        ReactiveFlow<Integer, Integer> flow = flow()
                .preferAsync()
                .defaultBudget(Duration.ofMillis(50));

        Integer result = flow.just(1)
                .handleMono("hung-remote", value -> Mono.<Integer>never().doOnCancel(cancelled::countDown))
                .recover(error -> {
                    seen.set(error);
                    return -1;
                })
                .executeMono()
                .block(Duration.ofSeconds(5));

        assertEquals(-1, result);
        assertInstanceOf(TimeoutException.class, seen.get());
        assertTrue(awaitQuietly(cancelled),
                "the async subscription was never cancelled: the default budget did not reach the async path");
    }

    // ── requireBudget: an unbudgeted reactive step is a build error ──

    @Test
    void requireBudgetRejectsAnUnbudgetedStepOnTheMainLine() {
        ReactiveFlow<Integer, Integer> flow = flow().requireBudget();

        IllegalStateException rejected = assertThrows(IllegalStateException.class,
                () -> flow.handleMono("charge", Mono::just));

        assertTrue(rejected.getMessage().contains("charge"), rejected::getMessage);
        assertTrue(rejected.getMessage().contains("requireBudget"), rejected::getMessage);
    }

    @Test
    void requireBudgetRejectsAnUnbudgetedStepInsideAJustPipeline() {
        ReactiveStep<Integer, Integer> pipeline = flow().requireBudget().just(1);

        IllegalStateException rejected = assertThrows(IllegalStateException.class,
                () -> pipeline.handleMono("lookup", Mono::just));

        assertTrue(rejected.getMessage().contains("lookup"), rejected::getMessage);
    }

    @Test
    void requireBudgetRejectsAnUnbudgetedStepInsideABranchLane() {
        ReactiveStepCondition<Integer, Integer> branch = flow().requireBudget().just(10).when(value -> value > 5);

        IllegalStateException rejected = assertThrows(IllegalStateException.class,
                () -> branch.then(lane -> Reactive.lane(lane).handleMono("lane-remote", Mono::just)));

        assertTrue(rejected.getMessage().contains("lane-remote"), rejected::getMessage);
    }

    @Test
    void requireBudgetRejectsAnUnbudgetedStepInsideAForkBody() {
        ReactiveStep<Integer, Integer> pipeline = flow().requireBudget().just(7);

        IllegalStateException rejected = assertThrows(IllegalStateException.class,
                () -> pipeline.fork("notify", lane -> Reactive.lane(lane).handleMono("fork-remote", Mono::just)));

        assertTrue(rejected.getMessage().contains("fork-remote"), rejected::getMessage);
    }

    @Test
    void requireBudgetAlsoGuardsAdaptMono() {
        ReactiveStep<Integer, Integer> pipeline = flow().requireBudget().just(1);

        assertThrows(IllegalStateException.class, () -> pipeline.adaptMono(value -> Mono.just("x")));
    }

    @Test
    void requireBudgetAlsoGuardsFanOutMono() {
        ReactiveStep<Integer, Integer> pipeline = flow().requireBudget().just(1);
        List<Function<Integer, Mono<Integer>>> branches = List.of(Mono::just);

        assertThrows(IllegalStateException.class, () -> pipeline.fanOutMono("enrich", branches, List::size));
    }

    // ── requireBudget is satisfied by a budget from either source ──

    @Test
    void requireBudgetIsSatisfiedByAnExplicitPerStepBudget() {
        ReactiveFlow<Integer, Integer> flow = flow().requireBudget();

        Integer result = flow.just(2)
                .handleMono("bounded", value -> Mono.just(value * 3), Duration.ofSeconds(1))
                .executeMono()
                .block(Duration.ofSeconds(5));

        assertEquals(6, result);
    }

    @Test
    void requireBudgetIsSatisfiedByTheDefaultBudget() {
        ReactiveFlow<Integer, Integer> flow = flow()
                .requireBudget()
                .defaultBudget(Duration.ofSeconds(1));

        Integer result = flow.just(2)
                .handleMono("bounded-by-default", value -> Mono.just(value * 4))
                .executeMono()
                .block(Duration.ofSeconds(5));

        assertEquals(8, result);
    }

    // ── parity: off by default, an unbudgeted Mono.just is fine ──

    @Test
    void withoutRequireBudgetAnUnbudgetedMonoJustStillWorks() {
        ReactiveFlow<Integer, Integer> flow = flow();   // requireBudget OFF (default)

        Integer result = flow.just(5)
                .handleMono("in-memory", value -> Mono.just(value + 1))
                .executeMono()
                .block(Duration.ofSeconds(5));

        assertEquals(6, result);
    }

    private static boolean awaitQuietly(CountDownLatch latch) {
        try {
            return latch.await(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
