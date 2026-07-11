package dev.nioflow.application.facade;

import dev.nioflow.core.model.Splice;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultNioEngineChainEditTest extends EngineTestSupport {

    @Test
    void spliceAtRuntimeDoesNotAffectInFlightCalls() {
        var gate = new CountDownLatch(1);
        engine.append(stage("slow", value -> {
            try {
                gate.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return value + ":slow";
        }));
        engine.seal();

        CompletableFuture<Object> inFlight = engine.call("a", new ConcurrentHashMap<>());
        engine.splice("slow", Splice.AFTER, List.of(stage("extra", value -> value + ":extra")));
        gate.countDown();

        assertEquals("a:slow", inFlight.join());
        assertEquals("b:slow:extra", engine.call("b", new ConcurrentHashMap<>()).join());
    }

    @Test
    void sealBlocksAppendUntilRelease() {
        engine.append(stage("first", value -> value));
        engine.seal();

        assertThrows(IllegalStateException.class, () -> engine.append(stage("second", value -> value)));

        engine.release();
        engine.append(stage("second", value -> value));
        assertEquals(2, engine.chain().size());
    }
}
