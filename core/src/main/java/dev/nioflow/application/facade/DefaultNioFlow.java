package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Cases;
import dev.nioflow.core.facade.Condition;
import dev.nioflow.core.facade.Context;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.NioStep;
import dev.nioflow.core.facade.Segment;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.Link;
import dev.nioflow.core.model.RateLimit;
import dev.nioflow.core.model.Retry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Root flow: the shared definition that owns the engine.
 *
 * <p>{@code I} is what {@link #just(Object)} accepts and {@code O} is what the
 * per-request pipeline is meant to return. The shared chain is
 * type-preserving (it takes an I and leaves an I), which is what lets
 * {@code just()} hand back a {@link NioStep} that starts at the input type —
 * re-typing is the per-request builder's job.
 *
 * <p>The constructors are private on purpose: they would let a caller build a
 * flow whose types promise something the chain does not do. Open one with
 * {@link #from(Class)} (recommended: the Class token lets {@code just()} reject
 * a wrong input) or {@link #create()} where the type cannot be named.
 */
public class DefaultNioFlow<I, O> extends AbstractChain<I> implements NioFlow<I, O>, AutoCloseable {

    private final NioEngine nioEngine;
    private final AtomicInteger anonymousLinks;
    private final List<Guard> guards;
    // The Class token from from(): lets just() reject a value that is not an I
    // at the entry point, instead of failing as a ClassCastException inside a
    // worker. Frameworks that inject by generics (Spring resolves a
    // NioFlow<?, ?> bean into ANY NioFlow<X, Y> field) can hand this flow the
    // wrong input; this is where that shows up, loudly. null with create().
    private final Class<I> inputType;

    private DefaultNioFlow(Class<I> inputType, NioEngine nioEngine, AtomicInteger anonymousLinks,
                           List<Guard> guards) {
        this.inputType = inputType;
        this.nioEngine = nioEngine;
        this.anonymousLinks = anonymousLinks;
        this.guards = guards;
    }

    /**
     * Typed entry point: the Class anchors I (what just() accepts). O comes
     * from the type you declare — it is the output your per-request pipelines
     * promise, and your own method's return type is what holds you to it.
     */
    public static <I, O> DefaultNioFlow<I, O> from(Class<I> type) {
        return from(type, new DefaultNioEngine());
    }

    public static <I, O> DefaultNioFlow<I, O> from(Class<I> type, NioEngine nioEngine) {
        if (type == null) {
            throw new IllegalArgumentException("The input type is required: it anchors what just() accepts");
        }
        return new DefaultNioFlow<>(boxed(type), nioEngine, new AtomicInteger(), List.of());
    }

    /**
     * Root flow for the places where the type cannot be named — a generic
     * factory, or a container bean declared as {@code NioFlow<?, ?>} and
     * injected into typed fields.
     *
     * <p>What you give up versus {@link #from(Class)} is the safety net: with
     * no Class token, {@code just()} cannot check that the value it receives
     * really is an {@code I}, so a mismatch surfaces later, inside the first
     * stage that touches the value. Name the type whenever you can.
     */
    public static <I, O> DefaultNioFlow<I, O> create() {
        return create(new DefaultNioEngine());
    }

    public static <I, O> DefaultNioFlow<I, O> create(NioEngine nioEngine) {
        return new DefaultNioFlow<>(null, nioEngine, new AtomicInteger(), List.of());
    }

    /**
     * Opens an independent execution over a snapshot of the shared chain. The
     * builder starts at the INPUT type: the shared chain preserved it, so the
     * first step you chain here receives exactly what you handed to just().
     */
    @Override
    public NioStep<I, O> just(I input) {
        checkInput(input);
        return new ExecutionNioFlow<>(nioEngine, input);
    }

    @Override
    public NioFlow<I, O> justAll(Iterable<I> inputs) {
        inputs.forEach(input -> {
            checkInput(input);
            nioEngine.inject(input);
        });
        return this;
    }

    @Override
    public NioFlow<I, O> handle(Function<I, I> function) {
        return handle(anonymousName("stage"), function);
    }

    @Override
    public NioFlow<I, O> handle(String name, Function<I, I> function) {
        stage(name, function);
        return this;
    }

    @Override
    public NioFlow<I, O> handle(String name, Function<I, I> function, Duration timeout) {
        stage(name, function, timeout);
        return this;
    }

    @Override
    public NioFlow<I, O> handle(String name, Function<I, I> function, Retry retry) {
        stage(name, function, retry);
        return this;
    }

    @Override
    public NioFlow<I, O> handle(String name, Function<I, I> function, Duration timeout, Retry retry) {
        stage(name, function, timeout, retry);
        return this;
    }

    @Override
    public NioFlow<I, O> handle(String name, Function<I, I> function, RateLimit rateLimit) {
        rateLimitedStage(name, function, rateLimit);
        return this;
    }

    @Override
    public NioFlow<I, O> handleContextual(BiFunction<I, Context, I> function) {
        return handleContextual(anonymousName("stage"), function);
    }

    @Override
    public NioFlow<I, O> handleContextual(String name, BiFunction<I, Context, I> function) {
        contextualStage(name, function);
        return this;
    }

    @Override
    public NioFlow<I, O> handleSync(Function<I, I> function) {
        return handleSync(anonymousName("sync"), function);
    }

    @Override
    public NioFlow<I, O> handleSync(String name, Function<I, I> function) {
        syncStage(name, function);
        return this;
    }

    @Override
    public NioFlow<I, O> background(Consumer<I> effect) {
        return background(anonymousName("background"), effect);
    }

    @Override
    public NioFlow<I, O> background(String name, Consumer<I> effect) {
        backgroundEffect(name, effect);
        return this;
    }

    @Override
    public NioFlow<I, O> filter(Predicate<I> predicate) {
        filterValues(predicate);
        return this;
    }

    @Override
    public <R> NioFlow<I, O> fanOut(List<Function<I, R>> branches, Function<List<R>, I> join) {
        return fanOut(anonymousName("fanout"), branches, join);
    }

    @Override
    public <R> NioFlow<I, O> fanOut(String name, List<Function<I, R>> branches, Function<List<R>, I> join) {
        fanOutBranches(name, branches, join);
        return this;
    }

    @Override
    public NioFlow<I, O> batch(int size, Duration window, Function<List<I>, List<I>> bulk) {
        return batch(anonymousName("batch"), size, window, bulk);
    }

    @Override
    public NioFlow<I, O> batch(String name, int size, Duration window, Function<List<I>, List<I>> bulk) {
        batchValues(name, size, window, bulk);
        return this;
    }

    @Override
    public NioFlow<I, O> use(Segment<I, I> segment) {
        embed(segment);
        return this;
    }

    @Override
    public NioFlow<I, O> use(String region, Segment<I, I> segment) {
        embed(region, segment);
        return this;
    }

    @Override
    public NioFlow<I, O> recover(Function<Throwable, I> function) {
        return recover(anonymousName("recovery"), function);
    }

    @Override
    public NioFlow<I, O> recover(String name, Function<Throwable, I> function) {
        recovery(name, function);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public NioFlow<I, O> onComplete(Consumer<O> callback) {
        // Shared-definition scope: every execution of this engine reports here.
        nioEngine.addCompleteHandler(value -> callback.accept((O) value));
        return this;
    }

    @Override
    public NioFlow<I, O> onError(Consumer<Throwable> callback) {
        nioEngine.addErrorHandler(callback);
        return this;
    }

    @Override
    public Condition<I, O> when(Predicate<I> predicate) {
        return new DefaultCondition<>(this, appendDecision(predicate));
    }

    @Override
    public Cases<I, O> match() {
        return new DefaultCases<>(this);
    }

    /**
     * Swaps the whole named region for a freshly-recorded segment, atomically.
     * The segment records off-chain but draws decision ids and anonymous names
     * from THIS flow, so guards and anchors never collide with the live chain.
     * Type-preserving, like everything in the shared definition.
     */
    public void replaceRegion(String region, Segment<I, I> segment) {
        List<Link> recorded = new ArrayList<>();
        segment.define(new DefaultLane<>(new RecordingChain<I>(nioEngine, recorded, anonymousLinks)));
        nioEngine.spliceRegion(region, List.copyOf(recorded));
    }

    /**
     * Lifecycle lives ONLY here, on the root flow that owns the engine — the
     * NioFlow contract, branches, lanes and executions do not expose close().
     */
    @Override
    public void close() {
        nioEngine.shutdown(Duration.ofSeconds(5));
    }

    /**
     * Guards the entry point against an input that is not an I — which the
     * compiler cannot catch when the flow arrives through an unchecked cast
     * (a raw type, or a framework injecting by generics).
     */
    private void checkInput(Object input) {
        if (inputType == null) {
            return;   // created without a token (create()): nothing to check against
        }
        if (input != null && !inputType.isInstance(input)) {
            throw new IllegalArgumentException("This flow accepts " + inputType.getName()
                    + " as input, but got " + input.getClass().getName()
                    + ". A NioFlow<I, O> declares I = what just() takes and O = what the pipeline returns;"
                    + " build it with DefaultNioFlow.from(" + input.getClass().getSimpleName() + ".class).");
        }
    }

    /** int.class.isInstance(5) is false: validate against the wrapper. */
    @SuppressWarnings("unchecked")
    private static <I> Class<I> boxed(Class<I> type) {
        if (!type.isPrimitive()) {
            return type;
        }
        Class<?> wrapper = switch (type.getName()) {
            case "boolean" -> Boolean.class;
            case "byte" -> Byte.class;
            case "char" -> Character.class;
            case "short" -> Short.class;
            case "int" -> Integer.class;
            case "long" -> Long.class;
            case "float" -> Float.class;
            case "double" -> Double.class;
            default -> Object.class;   // void.class
        };
        return (Class<I>) wrapper;
    }

    @Override
    NioEngine engine() {
        return nioEngine;
    }

    @Override
    void appendLink(Link link) {
        nioEngine.append(link);
    }

    @Override
    List<Guard> guards() {
        return guards;
    }

    @Override
    AbstractChain<I> withGuards(List<Guard> guards) {
        return new DefaultNioFlow<I, O>(inputType, nioEngine, anonymousLinks, guards);
    }

    @Override
    String anonymousName(String prefix) {
        return prefix + "-" + anonymousLinks.getAndIncrement();
    }
}
