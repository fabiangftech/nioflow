package dev.nioflow.application.facade;

import dev.nioflow.core.facade.FlowResult;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.FlowSignal;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.Link;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

/**
 * Ejecución efímera creada por just(): usa la definición compartida tal cual
 * (sin copiarla) hasta que se agrega el primer link local, y se ejecuta con
 * engine.call(input, context, chain) sin sellar ni mutar nada compartido.
 * Las vistas de lane (withGuards) comparten el mismo State, así los links
 * declarados dentro de un fork caen en la misma chain local.
 */
final class ExecutionNioFlow<I, T> extends AbstractNioFlow<I, T> {

    private final State state;
    private final List<Guard> guards;

    ExecutionNioFlow(NioEngine nioEngine, Object seed) {
        this(new State(nioEngine, seed), List.of());
    }

    private ExecutionNioFlow(State state, List<Guard> guards) {
        this.state = state;
        this.guards = guards;
    }

    @Override
    public NioFlow<I, T> just(I input) {
        state.seed = input;
        return this;
    }

    @Override
    public NioFlow<I, T> justAll(Iterable<I> inputs) {
        throw new UnsupportedOperationException("justAll() applies to the shared flow, not to a just() execution");
    }

    @Override
    @SuppressWarnings("unchecked")
    public T execute() {
        Object value = rawFuture().join();
        return value == FlowSignal.FILTERED ? null : (T) value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<T> executeAsync() {
        return rawFuture().thenApply(value -> value == FlowSignal.FILTERED ? null : (T) value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public FlowResult<T> executeResult() {
        Object value = rawFuture().join();
        return value == FlowSignal.FILTERED
                ? new FlowResult.Filtered<>()
                : new FlowResult.Completed<>((T) value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public NioFlow<I, T> onComplete(Consumer<T> callback) {
        Consumer<Object> untyped = value -> callback.accept((T) value);
        state.onComplete = state.onComplete == null ? untyped : state.onComplete.andThen(untyped);
        return this;
    }

    @Override
    public NioFlow<I, T> onError(Consumer<Throwable> callback) {
        state.onError = state.onError == null ? callback : state.onError.andThen(callback);
        return this;
    }

    @Override
    public NioFlow<I, T> key(Object key) {
        state.key = key;
        return this;
    }

    private CompletableFuture<Object> rawFuture() {
        List<Link> chain = state.links != null ? state.links : state.nioEngine.chain();
        CompletableFuture<Object> raw = state.nioEngine.call(state.seed, null, chain, state.key);
        if (state.onComplete == null && state.onError == null) {
            // Pay for what you use: no callbacks, no dependent future.
            return raw;
        }
        // execute()/executeAsync() join/compose on the DEPENDENT future, so
        // execution-scoped callbacks are guaranteed done before the caller
        // observes the result — same ordering the engine handlers give.
        return raw.whenComplete((value, error) -> {
            if (error != null) {
                if (state.onError != null) {
                    state.onError.accept(unwrap(error));
                }
            } else if (state.onComplete != null) {
                state.onComplete.accept(value == FlowSignal.FILTERED ? null : value);
            }
        });
    }

    private static Throwable unwrap(Throwable error) {
        return error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
    }

    @Override
    NioEngine engine() {
        return state.nioEngine;
    }

    @Override
    void appendLink(Link link) {
        state.localLinks().add(link);
    }

    @Override
    List<Guard> guards() {
        return guards;
    }

    @Override
    AbstractNioFlow<I, T> withGuards(List<Guard> guards) {
        return new ExecutionNioFlow<>(state, guards);
    }

    @Override
    String anonymousName(String prefix) {
        return prefix + "-" + state.anonymousLinks++;
    }

    /**
     * Estado de la ejecución, compartido entre el flow y sus vistas de lane.
     * Se construye en el hilo del request: no necesita sincronización.
     */
    private static final class State {

        private final NioEngine nioEngine;
        // null = solo la definición compartida; se copia recién al agregar links locales
        private List<Link> links;
        private Object seed;
        private int anonymousLinks;
        // Callbacks scoped a ESTA ejecución (null = ninguno); compuestos con
        // andThen si se registran varios. Compartidos con las vistas de lane.
        private Consumer<Object> onComplete;
        private Consumer<Throwable> onError;
        // Clave de orden (null = sin orden): misma clave → mismo boss, FIFO.
        private Object key;

        private State(NioEngine nioEngine, Object seed) {
            this.nioEngine = nioEngine;
            this.seed = seed;
        }

        private List<Link> localLinks() {
            if (links == null) {
                links = new ArrayList<>(nioEngine.chain());
            }
            return links;
        }
    }
}
