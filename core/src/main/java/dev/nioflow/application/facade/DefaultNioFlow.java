package dev.nioflow.application.facade;

import dev.nioflow.core.facade.FlowResult;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.Segment;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.Link;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Root flow: the shared definition that owns the engine.
 *
 * <p>The two type parameters are a promise the API keeps: {@code I} is what
 * {@link #just(Object)} accepts, {@code T} is what the pipeline holds at this
 * point — so {@code execute()} returns {@code T}. A brand-new flow is always
 * {@code <I, I>} (nothing has transformed the value yet) and ONLY
 * {@link #adapt(java.util.function.Function)} — which the compiler checks —
 * can move {@code T}. That is why the constructors are not public: they would
 * let a caller claim a {@code DefaultNioFlow<Integer, String>} for an empty
 * flow, a type that lies about what {@code execute()} really returns. Build
 * root flows through {@link #from(Class)}.
 */
public class DefaultNioFlow<I, T> extends AbstractNioFlow<I, T> implements AutoCloseable {

    private final NioEngine nioEngine;
    private final AtomicInteger anonymousLinks;
    private final List<Guard> guards;
    // The Class token from from(): lets just() reject a value that is not an I
    // at the entry point, instead of failing as a ClassCastException inside a
    // worker. Frameworks that inject by generics (Spring resolves a
    // NioFlow<?, ?> bean into ANY NioFlow<X, Y> field) can hand this flow the
    // wrong input; this is where that shows up, loudly.
    private final Class<I> inputType;

    private DefaultNioFlow(Class<I> inputType, NioEngine nioEngine, AtomicInteger anonymousLinks,
                           List<Guard> guards) {
        this.inputType = inputType;
        this.nioEngine = nioEngine;
        this.anonymousLinks = anonymousLinks;
        this.guards = guards;
    }

    /**
     * Typed entry point: the Class anchors I (what just accepts) and the
     * pipeline starts with T = I; only adapt() changes T from there on.
     */
    public static <I> DefaultNioFlow<I, I> from(Class<I> type) {
        return from(type, new DefaultNioEngine());
    }

    public static <I> DefaultNioFlow<I, I> from(Class<I> type, NioEngine nioEngine) {
        if (type == null) {
            throw new IllegalArgumentException("The input type is required: it anchors what just() accepts");
        }
        return new DefaultNioFlow<>(boxed(type), nioEngine, new AtomicInteger(), List.of());
    }

    /**
     * Opens an independent execution: it starts from a snapshot of the shared
     * chain and any links added afterwards live only in that execution. N
     * concurrent requests can each do just(...)...execute() without clashing.
     */
    @Override
    public NioFlow<I, T> just(I input) {
        checkInput(input);
        return new ExecutionNioFlow<>(nioEngine, input);
    }

    /**
     * Guards the entry point against an input that is not an I — which the
     * compiler cannot catch when the flow arrives through an unchecked cast
     * (a raw type, or a framework injecting by generics).
     */
    private void checkInput(Object input) {
        if (input != null && !inputType.isInstance(input)) {
            throw new IllegalArgumentException("This flow accepts " + inputType.getName()
                    + " as input, but got " + input.getClass().getName()
                    + ". A NioFlow<I, T> declares I = what just() takes and T = what execute() returns;"
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
    public NioFlow<I, T> justAll(Iterable<I> inputs) {
        inputs.forEach(input -> {
            checkInput(input);
            nioEngine.inject(input);
        });
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public NioFlow<I, T> onComplete(Consumer<T> callback) {
        // Shared-definition scope: every execution of this engine reports here.
        nioEngine.addCompleteHandler(value -> callback.accept((T) value));
        return this;
    }

    @Override
    public NioFlow<I, T> onError(Consumer<Throwable> callback) {
        nioEngine.addErrorHandler(callback);
        return this;
    }

    @Override
    public NioFlow<I, T> key(Object key) {
        throw new IllegalStateException("key() orders one execution; start it with just(input) first");
    }

    @Override
    public T execute() {
        throw new IllegalStateException("This flow has no input; start an execution with just(input)");
    }

    @Override
    public CompletableFuture<T> executeAsync() {
        throw new IllegalStateException("This flow has no input; start an execution with just(input)");
    }

    @Override
    public FlowResult<T> executeResult() {
        throw new IllegalStateException("This flow has no input; start an execution with just(input)");
    }

    /**
     * Swaps the whole named region for a freshly-recorded segment,
     * atomically. The segment records off-chain but draws decision ids and
     * anonymous names from THIS flow, so guards and anchors never collide
     * with the live chain. Top-level regions only: the recorded links carry
     * no lane guards (swap a lane-scoped region through
     * engine.spliceRegion with explicitly guarded links).
     */
    public <R> void replaceRegion(String region, Segment<T, R> segment) {
        List<Link> recorded = new ArrayList<>();
        segment.define(new DefaultLane<>(new RecordingNioFlow<>(nioEngine, recorded, anonymousLinks)));
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
    AbstractNioFlow<I, T> withGuards(List<Guard> guards) {
        return new DefaultNioFlow<>(inputType, nioEngine, anonymousLinks, guards);
    }

    @Override
    String anonymousName(String prefix) {
        return prefix + "-" + anonymousLinks.getAndIncrement();
    }
}
