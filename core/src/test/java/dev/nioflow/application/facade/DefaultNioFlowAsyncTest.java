package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultNioFlowAsyncTest {

    @Test
    void executeAsyncReturnsImmediatelyWithoutBlockingTheCaller() {
        var gate = new CountDownLatch(1);
        NioFlow<String, String> flow = DefaultNioFlow.from(String.class);

        CompletableFuture<String> ticket = flow.just("hola")
                .handle("slow", value -> {
                    try {
                        gate.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return value.toUpperCase();
                })
                .executeAsync();

        // The caller got the ticket back while the stage is still blocked.
        assertFalse(ticket.isDone());

        gate.countDown();
        assertEquals("HOLA", ticket.join());
    }

    @Test
    void executeAsyncCompletesExceptionallyOnUnrecoveredFailure() {
        NioFlow<String, String> flow = DefaultNioFlow.from(String.class);

        CompletableFuture<String> ticket = flow.just("hola")
                .handle("boom", value -> {
                    throw new IllegalStateException("db down");
                })
                .executeAsync();

        var failure = assertThrows(CompletionException.class, ticket::join);
        assertEquals("db down", failure.getCause().getMessage());
    }

    @Test
    void executeAsyncCanPipelineManyExecutionsBeforeJoining() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);
        flow.handle("double", value -> value * 2);

        List<CompletableFuture<Integer>> tickets = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            tickets.add(flow.just(i).executeAsync());
        }

        for (int i = 0; i < tickets.size(); i++) {
            assertEquals(i * 2, tickets.get(i).join());
        }
    }

    // executeAsync() without just() used to throw at runtime; with NioFlow (the
    // definition) and NioStep (the per-request pipeline) split apart, it is a
    // compile error.
}
