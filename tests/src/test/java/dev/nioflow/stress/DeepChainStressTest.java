package dev.nioflow.stress;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.core.model.Decision;
import dev.nioflow.core.model.Stage;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Caza-bugs: una chain profunda de links baratos (Decision/Filter/Background)
 * se procesa entera en el hilo boss. Si el avance es recursivo en vez de
 * iterativo, esto revienta el stack del boss (StackOverflowError), la tarea
 * muere y el CompletableFuture del request queda colgado para siempre.
 * El orTimeout convierte ese cuelgue en un fallo de test visible.
 */
class DeepChainStressTest {

    @Test
    void deepChainOfCheapLinksMustNotOverflowTheBossStack() {
        var engine = new DefaultNioEngine();
        try {
            int depth = 50_000;
            for (int i = 0; i < depth; i++) {
                engine.append(new Decision(value -> true, engine.nextDecision(), List.of()));
            }
            engine.append(new Stage("tail", value -> value, false, null, null, List.of()));

            Object result = engine.call(42, new ConcurrentHashMap<>())
                    .orTimeout(20, TimeUnit.SECONDS)
                    .join();

            assertEquals(42, result);
        } finally {
            engine.shutdown(Duration.ofMillis(100));
        }
    }
}
