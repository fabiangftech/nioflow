package dev.nioflow.unit;

import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.model.StageException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class DefaultNioFlowNamedStageTest {

    @Test
    void aNamedSyncStageFailureCarriesItsName() throws InterruptedException {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            IllegalStateException boom = new IllegalStateException("bad input");
            AtomicReference<Throwable> captured = new AtomicReference<>();
            CountDownLatch notified = new CountDownLatch(1);
            defaultNioFlow.onError(error -> {
                captured.set(error);
                notified.countDown();
            });
            defaultNioFlow.handle("validate", x -> {
                throw boom;
            });
            defaultNioFlow.just(1);

            assertTrue(notified.await(1, TimeUnit.SECONDS));
            StageException failure = assertInstanceOf(StageException.class, captured.get());
            assertEquals("validate", failure.stage());
            assertSame(boom, failure.getCause());
        }
    }

    @Test
    void aNamedAsyncStageFailureCarriesItsName() throws InterruptedException {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            AtomicReference<Throwable> captured = new AtomicReference<>();
            CountDownLatch notified = new CountDownLatch(1);
            defaultNioFlow.onError(error -> {
                captured.set(error);
                notified.countDown();
            });
            defaultNioFlow.submit("save", x -> {
                throw new IllegalStateException("db down");
            });
            defaultNioFlow.just(1);

            assertTrue(notified.await(1, TimeUnit.SECONDS));
            StageException failure = assertInstanceOf(StageException.class, captured.get());
            assertEquals("save", failure.stage());
            assertTrue(failure.getMessage().contains("save"));
        }
    }

    @Test
    void joinRethrowsTheNamedFailure() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.submit("enrich", x -> {
                throw new IllegalStateException("api down");
            });
            defaultNioFlow.just(1);

            CompletionException thrown = assertThrows(CompletionException.class, defaultNioFlow::join);
            StageException failure = assertInstanceOf(StageException.class, thrown.getCause());
            assertEquals("enrich", failure.stage());
        }
    }

    @Test
    void aRecoveryCanInspectTheFailingStage() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.submit("flaky", x -> {
                        throw new IllegalStateException("boom");
                    })
                    .onErrorResume(error ->
                            error instanceof StageException failure && failure.stage().equals("flaky")
                                    ? -1
                                    : -2);
            defaultNioFlow.just(1);

            assertEquals(-1, defaultNioFlow.join());
        }
    }

    @Test
    void namedStagesComputeNormallyOnSuccess() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            int result = defaultNioFlow.just(5)
                    .handle("double", x -> x * 2)
                    .submit("increment", x -> x + 1)
                    .join();

            assertEquals(11, result);
        }
    }
}
