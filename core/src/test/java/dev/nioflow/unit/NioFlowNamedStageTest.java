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
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            IllegalStateException boom = new IllegalStateException("bad input");
            AtomicReference<Throwable> captured = new AtomicReference<>();
            CountDownLatch notified = new CountDownLatch(1);
            pipeline.onError(error -> {
                captured.set(error);
                notified.countDown();
            });
            pipeline.handle("validate", x -> {
                throw boom;
            });
            pipeline.just(1);

            assertTrue(notified.await(1, TimeUnit.SECONDS));
            StageException failure = assertInstanceOf(StageException.class, captured.get());
            assertEquals("validate", failure.stage());
            assertSame(boom, failure.getCause());
        }
    }

    @Test
    void aNamedAsyncStageFailureCarriesItsName() throws InterruptedException {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            AtomicReference<Throwable> captured = new AtomicReference<>();
            CountDownLatch notified = new CountDownLatch(1);
            pipeline.onError(error -> {
                captured.set(error);
                notified.countDown();
            });
            pipeline.submit("save", x -> {
                throw new IllegalStateException("db down");
            });
            pipeline.just(1);

            assertTrue(notified.await(1, TimeUnit.SECONDS));
            StageException failure = assertInstanceOf(StageException.class, captured.get());
            assertEquals("save", failure.stage());
            assertTrue(failure.getMessage().contains("save"));
        }
    }

    @Test
    void joinRethrowsTheNamedFailure() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            pipeline.submit("enrich", x -> {
                throw new IllegalStateException("api down");
            });
            pipeline.just(1);

            CompletionException thrown = assertThrows(CompletionException.class, pipeline::join);
            StageException failure = assertInstanceOf(StageException.class, thrown.getCause());
            assertEquals("enrich", failure.stage());
        }
    }

    @Test
    void aRecoveryCanInspectTheFailingStage() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            pipeline.submit("flaky", x -> {
                        throw new IllegalStateException("boom");
                    })
                    .onErrorResume(error ->
                            error instanceof StageException failure && failure.stage().equals("flaky")
                                    ? -1
                                    : -2);
            pipeline.just(1);

            assertEquals(-1, pipeline.join());
        }
    }

    @Test
    void namedStagesComputeNormallyOnSuccess() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            int result = pipeline.just(5)
                    .handle("double", x -> x * 2)
                    .submit("increment", x -> x + 1)
                    .join();

            assertEquals(11, result);
        }
    }
}
