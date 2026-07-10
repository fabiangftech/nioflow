package dev.nioflow.unit;

import dev.nioflow.application.facade.NioFlow;
import dev.nioflow.core.model.StageException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class NioFlowNamedStageTest {

    @Test
    void aNamedSyncStageFailureCarriesItsName() throws InterruptedException {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            IllegalStateException boom = new IllegalStateException("bad input");
            AtomicReference<Throwable> captured = new AtomicReference<>();
            CountDownLatch notified = new CountDownLatch(1);
            nioFlow.onError(error -> {
                captured.set(error);
                notified.countDown();
            });
            nioFlow.handle("validate", x -> {
                throw boom;
            });
            nioFlow.just(1);

            assertTrue(notified.await(1, TimeUnit.SECONDS));
            StageException failure = assertInstanceOf(StageException.class, captured.get());
            assertEquals("validate", failure.stage());
            assertSame(boom, failure.getCause());
        }
    }

    @Test
    void aNamedAsyncStageFailureCarriesItsName() throws InterruptedException {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            AtomicReference<Throwable> captured = new AtomicReference<>();
            CountDownLatch notified = new CountDownLatch(1);
            nioFlow.onError(error -> {
                captured.set(error);
                notified.countDown();
            });
            nioFlow.submit("save", x -> {
                throw new IllegalStateException("db down");
            });
            nioFlow.just(1);

            assertTrue(notified.await(1, TimeUnit.SECONDS));
            StageException failure = assertInstanceOf(StageException.class, captured.get());
            assertEquals("save", failure.stage());
            assertTrue(failure.getMessage().contains("save"));
        }
    }

    @Test
    void joinRethrowsTheNamedFailure() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            nioFlow.submit("enrich", x -> {
                throw new IllegalStateException("api down");
            });
            nioFlow.just(1);

            CompletionException thrown = assertThrows(CompletionException.class, nioFlow::join);
            StageException failure = assertInstanceOf(StageException.class, thrown.getCause());
            assertEquals("enrich", failure.stage());
        }
    }

    @Test
    void aRecoveryCanInspectTheFailingStage() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            nioFlow.submit("flaky", x -> {
                        throw new IllegalStateException("boom");
                    })
                    .onErrorResume(error ->
                            error instanceof StageException failure && failure.stage().equals("flaky")
                                    ? -1
                                    : -2);
            nioFlow.just(1);

            assertEquals(-1, nioFlow.join());
        }
    }

    @Test
    void namedStagesComputeNormallyOnSuccess() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            int result = nioFlow.just(5)
                    .handle("double", x -> x * 2)
                    .submit("increment", x -> x + 1)
                    .join();

            assertEquals(11, result);
        }
    }
}
