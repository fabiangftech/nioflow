package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Cases;
import dev.nioflow.core.facade.Condition;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.Background;
import dev.nioflow.core.model.Filter;
import dev.nioflow.core.model.Link;
import dev.nioflow.core.model.Stage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public class DefaultNioFlow<I, T> implements NioFlow<I, T> {

    private final NioEngine nioEngine;
    private final AtomicInteger anonymousLinks = new AtomicInteger();

    public DefaultNioFlow() {
        this(new DefaultNioEngine());
    }

    public DefaultNioFlow(NioEngine nioEngine) {
        this.nioEngine = nioEngine;
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
    public NioFlow<I, T> handle(Function<T, T> function) {
        return handle(anonymousName("stage"), function);
    }

    @Override
    public NioFlow<I, T> handle(String name, Function<T, T> function) {
        nioEngine.append(new Stage(name, asObjectFunction(function), false, null, List.of()));
        return this;
    }

    @Override
    public NioFlow<I, T> background(Consumer<T> effect) {
        return background(anonymousName("background"), effect);
    }

    @Override
    @SuppressWarnings("unchecked")
    public NioFlow<I, T> background(String name, Consumer<T> effect) {
        nioEngine.append(new Background(name, (Consumer<Object>) effect, List.of()));
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> NioFlow<I, R> adapt(Function<T, R> function) {
        nioEngine.append(new Stage(anonymousName("adapt"), asObjectFunction(function), false, null, List.of()));
        return (NioFlow<I, R>) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public NioFlow<I, T> filter(Predicate<T> predicate) {
        nioEngine.append(new Filter((Predicate<Object>) predicate, List.of()));
        return this;
    }

    @Override
    public Condition<I, T> when(Predicate<T> predicate) {
        throw new UnsupportedOperationException("when() is not implemented yet");
    }

    @Override
    public Cases<I, T> match() {
        throw new UnsupportedOperationException("match() is not implemented yet");
    }

    @Override
    public T execute() {
        throw new IllegalStateException("This flow has no input; start an execution with just(input)");
    }

    @Override
    public void close() {
        nioEngine.shutdown(Duration.ofSeconds(5));
    }

    private String anonymousName(String prefix) {
        return prefix + "-" + anonymousLinks.getAndIncrement();
    }

    @SuppressWarnings("unchecked")
    private static Function<Object, Object> asObjectFunction(Function<?, ?> function) {
        return (Function<Object, Object>) function;
    }

    /**
     * Ejecución efímera creada por just(): usa la definición compartida tal cual
     * (sin copiarla) hasta que se agrega el primer link local, y se ejecuta con
     * engine.call(input, context, chain) sin sellar ni mutar nada compartido.
     * adapt() re-tipa el pipeline devolviendo la misma instancia vista como R.
     */
    private static final class ExecutionNioFlow<I, T> implements NioFlow<I, T> {

        private final NioEngine nioEngine;
        // null = solo la definición compartida; se copia recién al agregar links locales
        private List<Link> links;
        // La ejecución se construye en el hilo del request; no necesita contadores atómicos
        private int anonymousLinks;
        private Object seed;

        private ExecutionNioFlow(NioEngine nioEngine, Object seed) {
            this.nioEngine = nioEngine;
            this.seed = seed;
        }

        @Override
        public NioFlow<I, T> just(I input) {
            this.seed = input;
            return this;
        }

        @Override
        public NioFlow<I, T> justAll(Iterable<I> inputs) {
            throw new UnsupportedOperationException("justAll() applies to the shared flow, not to a just() execution");
        }

        @Override
        public NioFlow<I, T> handle(Function<T, T> function) {
            return handle(anonymousName("stage"), function);
        }

        @Override
        public NioFlow<I, T> handle(String name, Function<T, T> function) {
            localLinks().add(new Stage(name, asObjectFunction(function), false, null, List.of()));
            return this;
        }

        @Override
        public NioFlow<I, T> background(Consumer<T> effect) {
            return background(anonymousName("background"), effect);
        }

        @Override
        @SuppressWarnings("unchecked")
        public NioFlow<I, T> background(String name, Consumer<T> effect) {
            localLinks().add(new Background(name, (Consumer<Object>) effect, List.of()));
            return this;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <R> NioFlow<I, R> adapt(Function<T, R> function) {
            localLinks().add(new Stage(anonymousName("adapt"), asObjectFunction(function), false, null, List.of()));
            return (NioFlow<I, R>) this;
        }

        @Override
        @SuppressWarnings("unchecked")
        public NioFlow<I, T> filter(Predicate<T> predicate) {
            localLinks().add(new Filter((Predicate<Object>) predicate, List.of()));
            return this;
        }

        @Override
        public Condition<I, T> when(Predicate<T> predicate) {
            throw new UnsupportedOperationException("when() is not implemented yet");
        }

        @Override
        public Cases<I, T> match() {
            throw new UnsupportedOperationException("match() is not implemented yet");
        }

        @Override
        @SuppressWarnings("unchecked")
        public T execute() {
            List<Link> chain = links != null ? links : nioEngine.chain();
            return (T) nioEngine.call(seed, null, chain).join();
        }

        @Override
        public void close() {
            // La ejecución no es dueña del engine; no hay recursos propios que liberar.
        }

        private List<Link> localLinks() {
            if (links == null) {
                links = new ArrayList<>(nioEngine.chain());
            }
            return links;
        }

        private String anonymousName(String prefix) {
            return prefix + "-" + anonymousLinks++;
        }
    }
}
