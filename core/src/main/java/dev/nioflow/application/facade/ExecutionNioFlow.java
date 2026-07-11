package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.Link;

import java.util.ArrayList;
import java.util.List;

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
        List<Link> chain = state.links != null ? state.links : state.nioEngine.chain();
        return (T) state.nioEngine.call(state.seed, null, chain).join();
    }

    @Override
    public void close() {
        // La ejecución no es dueña del engine; no hay recursos propios que liberar.
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
