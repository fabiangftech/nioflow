package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.Link;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultNioFlow<I, T> extends AbstractNioFlow<I, T> {

    private final NioEngine nioEngine;
    private final AtomicInteger anonymousLinks;
    private final List<Guard> guards;

    public DefaultNioFlow() {
        this(new DefaultNioEngine());
    }

    public DefaultNioFlow(NioEngine nioEngine) {
        this(nioEngine, new AtomicInteger(), List.of());
    }

    private DefaultNioFlow(NioEngine nioEngine, AtomicInteger anonymousLinks, List<Guard> guards) {
        this.nioEngine = nioEngine;
        this.anonymousLinks = anonymousLinks;
        this.guards = guards;
    }

    /**
     * Punto de partida tipado: el Class ancla I (lo que acepta just) y el
     * pipeline arranca con T = I; solo adapt() cambia T de ahí en adelante.
     */
    public static <I> NioFlow<I, I> from(Class<I> type) {
        return new DefaultNioFlow<>();
    }

    public static <I> NioFlow<I, I> from(Class<I> type, NioEngine nioEngine) {
        return new DefaultNioFlow<>(nioEngine);
    }

    /**
     * Abre una ejecución independiente: parte de un snapshot de la chain compartida
     * y los links que se agreguen después viven solo en esa ejecución. N requests
     * concurrentes pueden hacer just(...)...execute() sin chocar entre sí.
     */
    @Override
    public NioFlow<I, T> just(I input) {
        return new ExecutionNioFlow<>(nioEngine, input);
    }

    @Override
    public NioFlow<I, T> justAll(Iterable<I> inputs) {
        inputs.forEach(input -> nioEngine.inject(input, new ConcurrentHashMap<>()));
        return this;
    }

    @Override
    public T execute() {
        throw new IllegalStateException("This flow has no input; start an execution with just(input)");
    }

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
        return new DefaultNioFlow<>(nioEngine, anonymousLinks, guards);
    }

    @Override
    String anonymousName(String prefix) {
        return prefix + "-" + anonymousLinks.getAndIncrement();
    }
}
