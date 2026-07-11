package dev.nioflow.stress;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.core.model.Splice;
import dev.nioflow.core.model.Stage;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Caza-bugs: edita la chain con splice REPLACE en un loop caliente mientras
 * miles de requests concurrentes la ejecutan. Cada request debe ver una chain
 * consistente: su resultado es input+1 o input+2, nunca un estado intermedio
 * roto, una excepción o un cuelgue. Detecta carreras entre el swap de la
 * chain y el snapshot que toma call().
 */
class ConcurrentSpliceStressTest {

    @Test
    void runtimeSpliceUnderLoadNeverProducesTornResults() throws Exception {
        var engine = new DefaultNioEngine();
        engine.append(new Stage("mutable", value -> (int) value + 1, false, null, null, List.of()));
        engine.seal();

        var stop = new AtomicBoolean(false);
        var editor = Thread.ofPlatform().start(() -> {
            int delta = 2;
            while (!stop.get()) {
                int currentDelta = delta;
                engine.splice("mutable", Splice.REPLACE, List.of(
                        new Stage("mutable", value -> (int) value + currentDelta, false, null, null, List.of())));
                delta = delta == 2 ? 1 : 2;
            }
        });

        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Object>> results = new ArrayList<>();
            for (int i = 0; i < 5_000; i++) {
                int input = i;
                results.add(callers.submit(() -> engine.call(input, new ConcurrentHashMap<>())
                        .orTimeout(30, TimeUnit.SECONDS)
                        .join()));
            }
            for (int i = 0; i < results.size(); i++) {
                int result = (int) results.get(i).get();
                int input = i;
                assertTrue(result == input + 1 || result == input + 2,
                        "input " + input + " produced torn result " + result);
            }
        } finally {
            stop.set(true);
            editor.join();
            engine.shutdown(Duration.ofMillis(100));
        }
    }
}
