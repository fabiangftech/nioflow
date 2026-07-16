package dev.nioflow.infrastructure.reactive;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.Context;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.Pipeline;
import dev.nioflow.core.facade.Segment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * pipe() over a PREBUILT Pipeline (RFC 0014): the pipeline is declared once and
 * only executed per element. What must hold is that it is EQUIVALENT to the
 * per-element BiFunction form it optimizes — same output, same filter/failure
 * semantics, same ordering, same per-element isolation — with the assembly
 * moved out of the loop.
 */
class ReactivePipePrebuiltTest {

    private NioEngine engine;
    private ReactiveFlow<Integer, Integer> flow;

    @BeforeEach
    void setUp() {
        engine = new DefaultNioEngine();
        flow = Reactive.<Integer, Integer>flow(DefaultNioFlow.from(Integer.class, engine)).allowUnbudgeted();
    }

    @AfterEach
    void tearDown() {
        engine.shutdown(Duration.ofSeconds(2));
    }

    @Test
    void prebuiltPipeMatchesTheBiFunctionFormElementForElement() {
        Segment<Integer, Integer> segment = step -> step
                .handle("a", value -> value + 1)
                .handle("b", value -> value * 2);
        Pipeline<Integer, Integer> pipeline = flow.pipeline(segment);

        Function<Flux<Integer>, Flux<Integer>> prebuilt = flow.pipe(4, pipeline);
        Function<Flux<Integer>, Flux<Integer>> dynamic = flow.pipe(4,
                (input, step) -> step.handle("a", value -> value + 1).handle("b", value -> value * 2));

        List<Integer> fromPrebuilt = prebuilt.apply(Flux.range(1, 8)).sort().collectList().block();
        List<Integer> fromDynamic = dynamic.apply(Flux.range(1, 8)).sort().collectList().block();

        assertEquals(fromDynamic, fromPrebuilt);
        assertEquals(List.of(4, 6, 8, 10, 12, 14, 16, 18), fromPrebuilt);   // (n+1)*2
    }

    @Test
    void aPrebuiltPipelineRunsHandleMonoStages() {
        Pipeline<Integer, Integer> pipeline = flow.pipeline(step ->
                Reactive.lane(step).handleMono("enrich", value -> Mono.just(value + 10)));

        List<Integer> results = flow.pipe(4, pipeline).apply(Flux.range(1, 4)).sort().collectList().block();
        assertEquals(List.of(11, 12, 13, 14), results);
    }

    @Test
    void aFilteredElementDisappearsFromThePrebuiltStream() {
        Pipeline<Integer, Integer> pipeline = flow.pipeline(step -> step
                .filter(value -> value % 2 == 0)
                .handle("double", value -> value * 2));

        StepVerifier.create(flow.pipe(4, pipeline).apply(Flux.range(1, 6)).sort())
                .expectNext(4, 8, 12)
                .verifyComplete();
    }

    @Test
    void aFailingElementFailsThePrebuiltStream() {
        Pipeline<Integer, Integer> pipeline = flow.pipeline(step -> step
                .handle("boom", value -> {
                    if (value == 3) {
                        throw new IllegalStateException("element 3 is poison");
                    }
                    return value;
                }));

        StepVerifier.create(flow.pipe(1, pipeline).apply(Flux.range(1, 5)))
                .expectNext(1, 2)
                .verifyErrorMessage("element 3 is poison");
    }

    @Test
    void prebuiltPipeOrderedPreservesInputOrder() {
        Pipeline<Integer, Integer> pipeline = flow.pipeline(step ->
                Reactive.lane(step).handleMono("staggered",
                        value -> Mono.delay(Duration.ofMillis(20L * (6 - value))).map(ignored -> value * 10)));

        List<Integer> results = flow.pipeOrdered(5, pipeline).apply(Flux.range(1, 5)).collectList().block();
        assertEquals(List.of(10, 20, 30, 40, 50), results);
    }

    @Test
    void prebuiltPipeResilientDropsThePoisonElementOnceAndKeepsGoing() {
        List<Integer> reported = new CopyOnWriteArrayList<>();
        List<Throwable> engineSaw = new CopyOnWriteArrayList<>();
        flow.onError(engineSaw::add);

        Pipeline<Integer, Integer> pipeline = flow.pipeline(step -> step
                .handle("boom", value -> {
                    if (value == 3) {
                        throw new IllegalStateException("element 3 is poison");
                    }
                    return value;
                }));

        Function<Flux<Integer>, Flux<Integer>> pipe = flow.pipeResilient(1, pipeline,
                (element, error) -> reported.add(element));

        StepVerifier.create(pipe.apply(Flux.range(1, 5)))
                .expectNext(1, 2, 4, 5)
                .verifyComplete();

        assertEquals(List.of(3), reported);
        assertEquals(1, engineSaw.size(), "reported " + engineSaw.size() + " times: " + engineSaw);
        assertEquals("element 3 is poison", engineSaw.getFirst().getMessage());
    }

    @Test
    void prebuiltPipeResilientWithoutAHandlerIsRejected() {
        Pipeline<Integer, Integer> pipeline = flow.pipeline(step -> step.handle("work", value -> value));
        assertThrows(IllegalArgumentException.class, () -> flow.pipeResilient(2, pipeline, null));
    }

    @Test
    void aConcurrencyBelowOneIsRejectedOnThePrebuiltOverloads() {
        Pipeline<Integer, Integer> pipeline = flow.pipeline(step -> step.handle("work", value -> value));
        assertThrows(IllegalArgumentException.class, () -> flow.pipe(0, pipeline));
        assertThrows(IllegalArgumentException.class, () -> flow.pipeOrdered(-1, pipeline));
        assertThrows(IllegalArgumentException.class, () -> flow.pipe(2, -1, pipeline));
    }

    @Test
    void propagatedContextIsSeededPerSubscriptionOnThePrebuiltPipe() {
        // propagate() lifts the key from the subscriber context on every element's
        // execution — the same bridge executeMono uses, now on the pipe path.
        Context.Key<Integer> tag = Context.Key.of("tag");
        Pipeline<Integer, Integer> pipeline = flow.pipeline(step -> step
                .handleContextual("read", (value, context) -> value + context.get(tag)));

        Function<Flux<Integer>, Flux<Integer>> pipe = flow.propagate(tag).pipe(4, pipeline);

        List<Integer> results = pipe.apply(Flux.range(1, 3))
                .contextWrite(reactor.util.context.Context.of("tag", 100))
                .sort()
                .collectList()
                .block();

        assertEquals(List.of(101, 102, 103), results);   // each element read tag = 100
    }
}
