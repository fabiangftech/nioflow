package dev.nioflow.infrastructure.reactive;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.Context.Key;
import dev.nioflow.core.facade.NioEngine;
import io.micrometer.context.ContextRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import java.time.Duration;

/**
 * RFC 0033 — the context bridge reads ThreadLocal-backed trace context, not only
 * the Reactor subscriber context.
 *
 * <p>Micrometer Tracing / Sleuth / MDC keep the trace id in a ThreadLocal exposed
 * through a registered {@code ThreadLocalAccessor}, NOT under a subscriber-context
 * entry whose key equals {@code Context.Key.name()}. A bridge that only read the
 * subscriber context would seed nothing against the very stack people reach for
 * propagation to serve. Here a {@code propagate(TRACE)} key that the subscriber
 * context does not carry is found among the registered accessors, by name — and
 * the subscriber context still wins when it does carry it.
 */
class ReactiveContextThreadLocalTest {

    private static final String KEY = "traceId";
    private static final Key<String> TRACE = Key.of(KEY);
    private static final ThreadLocal<String> TRACE_TL = new ThreadLocal<>();

    private NioEngine engine;
    private ReactiveFlow<String, String> flow;

    @BeforeEach
    void setUp() {
        engine = new DefaultNioEngine();
        flow = Reactive.<String, String>flow(DefaultNioFlow.from(String.class, engine)).propagate(TRACE);
        // How tracing exposes its ThreadLocal to the context-propagation machinery.
        ContextRegistry.getInstance().registerThreadLocalAccessor(KEY, TRACE_TL);
    }

    @AfterEach
    void tearDown() {
        ContextRegistry.getInstance().removeThreadLocalAccessor(KEY);
        TRACE_TL.remove();
        engine.shutdown(Duration.ofSeconds(1));
    }

    @Test
    void aDeclaredKeyCrossesFromAThreadLocalWhenTheSubscriberContextLacksIt() {
        TRACE_TL.set("tl-abc-123");   // set on the subscribing thread, as tracing would

        // NO contextWrite: the subscriber context carries nothing, so the value
        // must come from the ThreadLocal accessor — the case that silently failed
        // before RFC 0033.
        Mono<String> mono = flow.just("order-1")
                .handleContextual("read", (value, ctx) -> value + " @" + ctx.get(TRACE))
                .executeMono();

        StepVerifier.create(mono).expectNext("order-1 @tl-abc-123").verifyComplete();
    }

    @Test
    void theSubscriberContextStillWinsOverTheThreadLocal() {
        TRACE_TL.set("tl-value");

        Mono<String> mono = flow.just("order-1")
                .handleContextual("read", (value, ctx) -> value + " @" + ctx.get(TRACE))
                .executeMono()
                .contextWrite(Context.of(KEY, "ctx-wins"));

        StepVerifier.create(mono).expectNext("order-1 @ctx-wins").verifyComplete();
    }

    @Test
    void aThreadLocalThatCarriesNothingSeedsNothing() {
        // TRACE_TL not set: the accessor's value is null, so nothing is seeded and
        // the stage reads the key exactly as it would for one nobody ever wrote.
        Mono<String> mono = flow.just("order-1")
                .handleContextual("read", (value, ctx) -> value + " @" + ctx.getOrDefault(TRACE, "none"))
                .executeMono();

        StepVerifier.create(mono).expectNext("order-1 @none").verifyComplete();
    }
}
