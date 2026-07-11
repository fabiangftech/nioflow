package dev.nioflow.application.facade;

import dev.nioflow.core.model.Filter;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultNioEngineFusionTest extends EngineTestSupport {

    @Test
    void filterCutsTheFlow() {
        engine.append(new Filter(value -> (int) value > 0, List.of()));
        engine.append(stage("after-filter", value -> "reached"));

        assertEquals("reached", engine.call(1, new ConcurrentHashMap<>()).join());
        assertNull(engine.call(-1, new ConcurrentHashMap<>()).join());
    }

    @Test
    void fusedFilterBetweenStagesCutsTheFlow() {
        engine.append(stage("head", value -> (int) value + 1));
        engine.append(new Filter(value -> (int) value > 10, List.of()));
        engine.append(stage("tail", value -> (int) value * 2));

        assertEquals(42, engine.call(20, new ConcurrentHashMap<>()).join());
        assertNull(engine.call(3, new ConcurrentHashMap<>()).join());
    }

    @Test
    void throwingFilterPredicateFailsTheCallInsteadOfHangingIt() {
        engine.append(new Filter(value -> {
            throw new IllegalStateException("boom");
        }, List.of()));

        CompletableFuture<Object> result = engine.call("x", new ConcurrentHashMap<>())
                .orTimeout(5, TimeUnit.SECONDS);

        CompletionException failure = assertThrows(CompletionException.class, result::join);
        assertInstanceOf(IllegalStateException.class, failure.getCause());
    }
}
