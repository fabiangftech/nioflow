package dev.nioflow.unit;

import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

class DefaultNioFlowScopedTest {

    @Test
    void aScopePerCallNeverAccumulatesOnTheSharedChain() {
        try (DefaultNioFlow<String> defaultNioFlow = new DefaultNioFlow<>()) {
            for (int i = 0; i < 10; i++) {
                String greeting = defaultNioFlow.scoped()
                        .just("Hello")
                        .handle("greeting", s -> s + ", World!")
                        .join();
                assertEquals("Hello, World!", greeting);
            }
            assertEquals(0, defaultNioFlow.diagnostics().chain().size(),
                    "the shared chain must stay empty");
        }
    }

    @Test
    void concurrentScopesDeclareTheirOwnChainsWithoutInterfering() {
        try (DefaultNioFlow<String> defaultNioFlow = new DefaultNioFlow<>()) {
            NioFlow<String> first = defaultNioFlow.scoped();
            NioFlow<String> second = defaultNioFlow.scoped();

            first.just("x");
            second.just("y");
            first.handle(s -> s + "A"); // interleaved declarations on purpose
            second.handle(s -> s + "B");

            assertEquals("xA", first.join());
            assertEquals("yB", second.join());
        }
    }

    @Test
    void scopesRunConcurrently() throws Exception {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<CompletableFuture<Integer>> replies = new CopyOnWriteArrayList<>();
            for (int i = 0; i < 20; i++) {
                int value = i;
                replies.add(defaultNioFlow.scoped()
                        .handle(x -> x * 2)
                        .call(value, Duration.ofSeconds(2)));
            }
            for (int i = 0; i < 20; i++) {
                assertEquals(i * 2, replies.get(i).get());
            }
        }
    }

    @Test
    void aScopeStartsFromTheSharedChainDeclaredSoFar() {
        try (DefaultNioFlow<String> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle("shared", s -> s + "-shared");

            String scoped = defaultNioFlow.scoped()
                    .just("v")
                    .handle(s -> s + "-scoped")
                    .join();

            assertEquals("v-shared-scoped", scoped);
            assertEquals("w-shared", defaultNioFlow.just("w").join(),
                    "the shared chain must not see the scope's links");
        }
    }

    @Test
    void aScopedFailureIsThrownOnceThenTheScopeStaysUsable() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            NioFlow<Integer> scope = defaultNioFlow.scoped();
            scope.handle(x -> {
                if (x < 0) {
                    throw new IllegalStateException("negative");
                }
                return x;
            });

            scope.just(-1);
            CompletionException failure = assertThrows(CompletionException.class, scope::join);
            assertInstanceOf(IllegalStateException.class, failure.getCause());

            assertEquals(7, scope.just(7).join(), "the scope must recover after the throw");
        }
    }

    @Test
    void filteredValuesDoNotCountTowardTheScopedJoin() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            NioFlow<Integer> scope = defaultNioFlow.scoped();
            scope.filter(x -> x > 0);

            scope.just(-1).just(5);

            assertEquals(5, scope.join());
        }
    }

    @Test
    void scopeObserversSeeOnlyTheirOwnScope() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> first = new CopyOnWriteArrayList<>();
            List<Integer> second = new CopyOnWriteArrayList<>();

            NioFlow<Integer> one = defaultNioFlow.scoped();
            one.handle(x -> x + 1).onComplete(first::add);
            NioFlow<Integer> two = defaultNioFlow.scoped();
            two.handle(x -> x * 10).onComplete(second::add);

            one.just(1).join();
            two.just(2).join();

            assertEquals(List.of(2), first);
            assertEquals(List.of(20), second);
        }
    }

    @Test
    void forksWorkInsideAScope() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            NioFlow<Integer> scope = defaultNioFlow.scoped();
            scope.when(x -> x > 10)
                    .then(lane -> lane.handle(x -> x + 100))
                    .otherwise(lane -> lane.handle(x -> x - 1))
                    .onComplete(completed::add);

            scope.justAll(List.of(20, 5));
            scope.join();

            assertTrue(completed.containsAll(List.of(120, 4)));
        }
    }

    @Test
    void closingAScopeNeverStopsTheSharedFlow() {
        try (DefaultNioFlow<String> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.scoped().close();

            assertEquals("ok", defaultNioFlow.scoped().just("ok").join(),
                    "the shared engine must survive a scope's close");
        }
    }

    @Test
    void globalConcernsAreRejectedOnAScope() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle("stage", x -> x);
            NioFlow<Integer> scope = defaultNioFlow.scoped();

            assertThrows(IllegalStateException.class,
                    () -> scope.replace("stage", segment -> segment.handle(x -> x)));
            assertThrows(IllegalStateException.class, () -> scope.metrics(null));
            assertThrows(IllegalStateException.class, () -> scope.trace(null));
        }
    }

    @Test
    void aBoundedScopedJoinTimesOutWhileWorkKeepsFlowing() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            NioFlow<Integer> scope = defaultNioFlow.scoped();
            scope.submit(x -> {
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return x;
            });

            scope.just(1);
            assertThrows(CompletionException.class, () -> scope.join(Duration.ofMillis(50)));

            assertEquals(1, scope.join(), "a later unbounded join must still deliver");
        }
    }
}
