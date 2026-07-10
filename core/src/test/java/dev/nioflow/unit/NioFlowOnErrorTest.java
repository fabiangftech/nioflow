package dev.nioflow.unit;

import dev.nioflow.application.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class NioFlowOnErrorTest {

    @Test
    void onErrorReceivesSyncFailuresWithoutBlocking() throws InterruptedException {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            CountDownLatch notified = new CountDownLatch(1);
            AtomicReference<Throwable> captured = new AtomicReference<>();
            nioFlow.just(1)
                    .onError(error -> {
                        captured.set(error);
                        notified.countDown();
                    })
                    .handle(x -> {
                        throw new IllegalStateException("sync boom");
                    });

            assertTrue(notified.await(1, TimeUnit.SECONDS));
            assertEquals("sync boom", captured.get().getMessage());
        }
    }

    @Test
    void onErrorReceivesAsyncFailuresAndSkipsRemainingStages() throws InterruptedException {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            CountDownLatch notified = new CountDownLatch(1);
            AtomicReference<Throwable> captured = new AtomicReference<>();
            List<String> executed = new CopyOnWriteArrayList<>();
            nioFlow.just(1)
                    .submit(x -> {
                        throw new IllegalStateException("async boom");
                    })
                    .handle(x -> {
                        executed.add("after-failure");
                        return x;
                    })
                    .onError(error -> {
                        captured.set(error);
                        notified.countDown();
                    });

            assertTrue(notified.await(1, TimeUnit.SECONDS));
            assertEquals("async boom", captured.get().getMessage());
            assertTrue(executed.isEmpty());

            CompletionException thrown = assertThrows(CompletionException.class, nioFlow::join);
            assertInstanceOf(IllegalStateException.class, thrown.getCause());
        }
    }

    @Test
    void onErrorRegisteredAfterTheFailureIsStillNotified() throws InterruptedException {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            nioFlow.just(1).submit(x -> {
                throw new IllegalStateException("late boom");
            });
            assertThrows(CompletionException.class, nioFlow::join);

            CountDownLatch notified = new CountDownLatch(1);
            nioFlow.onError(error -> notified.countDown());
            assertTrue(notified.await(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void everyHandlerIsNotifiedEvenIfOneThrows() throws InterruptedException {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            CountDownLatch second = new CountDownLatch(1);
            nioFlow.onError(error -> {
                        throw new RuntimeException("misbehaving handler");
                    })
                    .onError(error -> second.countDown());
            nioFlow.handle(x -> {
                throw new IllegalStateException("boom");
            });
            nioFlow.just(1);

            assertTrue(second.await(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void syncFailuresArriveAsTheExactThrownInstance() throws InterruptedException {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            IllegalStateException boom = new IllegalStateException("sync instance");
            AtomicReference<Throwable> captured = new AtomicReference<>();
            CountDownLatch notified = new CountDownLatch(1);
            nioFlow.onError(error -> {
                captured.set(error);
                notified.countDown();
            });
            nioFlow.handle(x -> {
                throw boom;
            });
            nioFlow.just(1);

            assertTrue(notified.await(1, TimeUnit.SECONDS));
            assertSame(boom, captured.get());
        }
    }

    @Test
    void asyncFailuresArriveUnwrappedAsTheExactThrownInstance() throws InterruptedException {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            IllegalStateException boom = new IllegalStateException("async instance");
            AtomicReference<Throwable> captured = new AtomicReference<>();
            CountDownLatch notified = new CountDownLatch(1);
            nioFlow.onError(error -> {
                captured.set(error);
                notified.countDown();
            });
            nioFlow.submit(x -> {
                throw boom;
            });
            nioFlow.just(1);

            assertTrue(notified.await(1, TimeUnit.SECONDS));
            assertSame(boom, captured.get());
        }
    }

    @Test
    void aFailingWhenPredicateFailsTheValue() throws InterruptedException {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            CountDownLatch notified = new CountDownLatch(1);
            AtomicReference<Throwable> captured = new AtomicReference<>();
            nioFlow.onError(error -> {
                captured.set(error);
                notified.countDown();
            });
            nioFlow.when(x -> {
                        throw new IllegalStateException("predicate boom");
                    })
                    .then(lane -> lane
                            .handle(x -> x * 2));
            nioFlow.just(1);

            assertTrue(notified.await(1, TimeUnit.SECONDS));
            assertEquals("predicate boom", captured.get().getMessage());
            assertThrows(CompletionException.class, nioFlow::join);
        }
    }

    @Test
    void aLateHandlerReceivesEveryPastFailure() throws InterruptedException {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            nioFlow.submit(x -> {
                throw new IllegalStateException("boom-" + x);
            });
            nioFlow.just(1);
            nioFlow.just(2);
            assertThrows(CompletionException.class, nioFlow::join);

            CountDownLatch notified = new CountDownLatch(2);
            nioFlow.onError(error -> notified.countDown());
            assertTrue(notified.await(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void joinRethrowsTheFailureEvenWhenOtherValuesSucceeded() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            List<Integer> survived = new CopyOnWriteArrayList<>();
            nioFlow.submit(x -> {
                        if (x == 2) {
                            throw new IllegalStateException("only value 2");
                        }
                        return x;
                    })
                    .handle(x -> {
                        survived.add(x);
                        return x;
                    });
            nioFlow.just(1);
            nioFlow.just(2);
            nioFlow.just(3);

            CompletionException thrown = assertThrows(CompletionException.class, nioFlow::join);
            assertEquals("only value 2", thrown.getCause().getMessage());
            assertTrue(survived.containsAll(List.of(1, 3)));
        }
    }

    @Test
    void theReplayHistoryIsBoundedToTheMostRecentFailures() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            nioFlow.handle(x -> {
                throw new IllegalStateException("boom-" + x);
            });

            for (int i = 0; i < 130; i++) {
                nioFlow.just(i);
                assertThrows(CompletionException.class, nioFlow::join); // sequential: deterministic order
            }

            List<String> replayed = new CopyOnWriteArrayList<>();
            nioFlow.onError(error -> replayed.add(error.getMessage()));

            assertEquals(128, replayed.size(), "the history must cap at 128 failures");
            assertEquals("boom-2", replayed.getFirst(), "the oldest failures must be evicted");
            assertEquals("boom-129", replayed.getLast());
        }
    }

    @Test
    void thePipelineRecoversAfterJoinRethrowsAFailure() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            nioFlow.submit(x -> {
                if (x == 1) {
                    throw new IllegalStateException("first value boom");
                }
                return x * 10;
            });

            nioFlow.just(1);
            assertThrows(CompletionException.class, nioFlow::join);

            // the failure was consumed: new values flow and join succeeds again
            nioFlow.just(2);
            assertEquals(20, nioFlow.join());
        }
    }
}
