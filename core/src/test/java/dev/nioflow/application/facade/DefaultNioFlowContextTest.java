package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Context.Key;
import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Context API: typed per-execution scratch state for stages that opt in via
 * handleContextual. Plain stages never see it; the backing map lazy-inits on
 * the first put; keys are name-based so a map handed to engine.call(input,
 * map) interoperates.
 */
class DefaultNioFlowContextTest extends EngineTestSupport {

    private static final Key<String> USER = Key.of("user");
    private static final Key<Integer> HITS = Key.of("hits");

    @Test
    void contextTravelsAcrossStagesAndWorkerHops() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handleContextual("stash", (value, ctx) -> {
            ctx.put(USER, "user-" + value);
            return value;
        }).handle("plain-between", value -> value * 10)
                .handleContextual("read-back", (value, ctx) ->
                        value + ctx.get(USER).length());
        engine.seal();

        // "user-7".length() == 6: the entry survives the plain stage between.
        assertEquals(76, flow.just(7).execute());
    }

    @Test
    void missingKeysReadNullAndDefaultsApply() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handleContextual((value, ctx) -> {
            if (ctx.get(USER) != null) {
                throw new IllegalStateException("empty context must read null");
            }
            return value + ctx.getOrDefault(HITS, 100);
        });

        assertEquals(101, flow.just(1).execute());
    }

    @Test
    void concurrentExecutionsGetIsolatedContexts() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handleContextual("stash", (value, ctx) -> {
            ctx.put(HITS, value);
            return value;
        }).handle("shuffle", value -> value)
                .handleContextual("read", (value, ctx) -> ctx.get(HITS) * 1000 + value);
        engine.seal();

        List<CompletableFuture<Integer>> results = List.of(
                flow.just(1).executeAsync(),
                flow.just(2).executeAsync(),
                flow.just(3).executeAsync());

        assertEquals(1001, results.get(0).join());
        assertEquals(2002, results.get(1).join());
        assertEquals(3003, results.get(2).join());
    }

    @Test
    void contextualStagesWorkInsideLanes() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handleContextual("seed", (value, ctx) -> {
            ctx.put(HITS, value * 100);
            return value;
        }).when(value -> value % 2 == 0)
                .then(lane -> lane.handleContextual((value, ctx) -> value + ctx.get(HITS)))
                .otherwise(lane -> lane.handle(value -> -value));
        engine.seal();

        assertEquals(202, flow.just(2).execute()); // 2 + HITS(2 * 100)
        assertEquals(-3, flow.just(3).execute());
    }

    @Test
    void callerSuppliedMapInteroperatesByKeyName() {
        engine.append(new dev.nioflow.core.model.Stage("read-and-write",
                new ContextualFunction((value, ctx) -> {
                    String user = ctx.get(USER);
                    ctx.put(HITS, 1);
                    return value + ":" + user;
                }), false, null, null, List.of()));
        Map<String, Object> handed = new ConcurrentHashMap<>();
        handed.put("user", "fabian");

        assertEquals("x:fabian", engine.call("x", handed).join());
        // Writes land in the caller's own map, under the key's name.
        assertEquals(1, handed.get("hits"));
    }

    @Test
    void throwingContextualStageFailsTheValueAndIsRecoverable() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handleContextual((value, ctx) -> {
            throw new IllegalStateException("boom");
        }).recover(error -> -1);

        assertEquals(-1, flow.just(5).execute());
    }

    @Test
    void blankKeyNamesAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> Key.of(" "));
    }

    @Test
    void contextualStagesFuseLikePlainStages() {
        // Three no-timeout stages (contextual ones included) must still travel
        // as one fused run — same routing and result sealed or not.
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("first", value -> value + 1)
                .handleContextual("stash", (value, ctx) -> {
                    ctx.put(HITS, value);
                    return value;
                })
                .handleContextual("read", (value, ctx) -> value + ctx.get(HITS));
        assertEquals(12, flow.just(5).execute());

        engine.seal();
        assertEquals(12, flow.just(5).execute());
    }

    @Test
    void getOrDefaultReturnsTheStoredValueWhenThereIsOne() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handleContextual("seed", (value, ctx) -> {
                    ctx.put(HITS, 7);
                    return value;
                })
                .handleContextual("read", (value, ctx) -> value + ctx.getOrDefault(HITS, 100));

        assertEquals(8, flow.just(1).execute());
    }

    /**
     * A contextual stage is a BiFunction the engine unwraps at its single apply
     * point; applying it as a plain Function means someone bypassed the engine.
     */
    @Test
    void aContextualFunctionCannotBeAppliedWithoutTheEngine() {
        ContextualFunction contextual = new ContextualFunction((value, ctx) -> value);

        assertThrows(IllegalStateException.class, () -> contextual.apply(1));
    }
}
