package dev.nioflow.infrastructure.reactive;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.FlowResult;
import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * RFC 0020 — equivalence bug-hunt for the reactive facade. "A reactive stage is
 * not a new link": {@code handleMono} appends the same {@code Stage} a plain
 * {@code handle} does, whose function parks a worker on the Mono. So a chain of
 * {@code handleMono} stages over resolved Monos must compute the identical
 * result to the same logic expressed with plain {@code handle} stages, across a
 * battery of inputs, and a generous budget that never fires must not change it.
 * The oracle is agreement — a divergence is a facade bug with a reproducer.
 */
class ReactiveEquivalenceProbeTest {

    private static final int[] INPUTS = {-8, -1, 0, 1, 2, 3, 7, 13, 20, 49, 100};

    private DefaultNioEngine reactiveEngine;
    private DefaultNioEngine plainEngine;
    private ReactiveFlow<Integer, Integer> reactive;
    private NioFlow<Integer, Integer> plain;

    @BeforeEach
    void setUp() {
        reactiveEngine = new DefaultNioEngine();
        plainEngine = new DefaultNioEngine();
        reactive = Reactive.flow(DefaultNioFlow.from(Integer.class, reactiveEngine));
        plain = DefaultNioFlow.from(Integer.class, plainEngine);
    }

    @AfterEach
    void tearDown() {
        reactiveEngine.shutdown(Duration.ofSeconds(1));
        plainEngine.shutdown(Duration.ofSeconds(1));
    }

    @Test
    void handleMonoChainEqualsPlainHandleChain() {
        for (int input : INPUTS) {
            Integer reactiveResult = reactive.just(input)
                    .handleMono("a", value -> Mono.just(value + 1))
                    .handleMono("b", value -> Mono.just(value * 3))
                    .handleMono("c", value -> Mono.just(value - 7))
                    .executeMono()
                    .block(Duration.ofSeconds(5));
            Integer plainResult = plain.just(input)
                    .handle("a", value -> value + 1)
                    .handle("b", value -> value * 3)
                    .handle("c", value -> value - 7)
                    .execute();

            assertEquals(plainResult, reactiveResult, "handleMono diverged from handle at input " + input);
        }
    }

    @Test
    void budgetedHandleMonoEqualsUnbudgetedWhenTheBudgetNeverFires() {
        // A generous budget forces mono.timeout(...) onto every step (a different
        // code path) but never fires on a resolved Mono — the result must not move.
        ReactiveFlow<Integer, Integer> budgeted = reactive.defaultBudget(Duration.ofSeconds(30));
        for (int input : INPUTS) {
            Integer withBudget = budgeted.just(input)
                    .handleMono("a", value -> Mono.just(value + 1))
                    .handleMono("b", value -> Mono.just(value * 2))
                    .executeMono()
                    .block(Duration.ofSeconds(5));
            Integer withoutBudget = plain.just(input)
                    .handle("a", value -> value + 1)
                    .handle("b", value -> value * 2)
                    .execute();

            assertEquals(withoutBudget, withBudget, "a never-firing budget changed the result at input " + input);
        }
    }

    @Test
    void reactiveFilterCutIsFilteredThroughExecuteResult() {
        FlowResult<Integer> result = reactive.just(5)
                .handleMono("x", value -> Mono.just(value + 1))
                .filter(value -> false)
                .executeResult();

        assertInstanceOf(FlowResult.Filtered.class, result);
    }

    @Test
    void anEmptyMonoFailsWhereAFilterCutIsFiltered() {
        // RFC 0027: an empty value-carrying handleMono is a stage FAILURE — it can
        // no longer masquerade as Completed(null). It stays distinct from a
        // filter() cut, which is Filtered (see the test above): the two notions of
        // "no value" a caller must be able to tell apart, one a bug, one a choice.
        ReactiveStep<Integer, Integer> emptyStep = reactive.just(5).handleMono("lookup", value -> Mono.empty());

        CompletionException thrown = assertThrows(CompletionException.class, emptyStep::executeResult);
        assertInstanceOf(EmptyMonoException.class, thrown.getCause());
    }
}
