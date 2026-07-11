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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public class DefaultNioFlow<T> implements NioFlow<T> {

    private final NioEngine nioEngine;
    private final AtomicInteger anonymousLinks = new AtomicInteger();

    public DefaultNioFlow() {
        this(new DefaultNioEngine());
    }

    /**
     * El Class solo ancla el tipo del pipeline en el punto de partida
     * (evita inferir Object en los steps); no se usa en runtime.
     */
    public DefaultNioFlow(Class<T> type) {
        this(new DefaultNioEngine());
    }

    public DefaultNioFlow(Class<T> type, NioEngine nioEngine) {
        this(nioEngine);
    }

    public DefaultNioFlow(NioEngine nioEngine) {
        this.nioEngine = nioEngine;
    }

    /**
     * Abre una ejecución independiente: parte de un snapshot de la chain compartida
     * y los links que se agreguen después viven solo en esa ejecución. N requests
     * concurrentes pueden hacer just(...)...execute() sin chocar entre sí.
     */
    @Override
    public <I> NioFlow<I> just(I input) {
        return new ExecutionNioFlow<>(nioEngine, input);
    }

    @Override
    public <I> NioFlow<T> justAll(Iterable<I> inputs) {
        inputs.forEach(input -> nioEngine.inject(input, new ConcurrentHashMap<>()));
        return this;
    }

    @Override
    public NioFlow<T> handle(Function<T, T> function) {
        return handle(anonymousName("stage"), function);
    }

    @Override
    public NioFlow<T> handle(String name, Function<T, T> function) {
        nioEngine.append(new Stage(name, asObjectFunction(function), false, null, List.of()));
        return this;
    }

    @Override
    public NioFlow<T> background(Consumer<T> effect) {
        return background(anonymousName("background"), effect);
    }

    @Override
    @SuppressWarnings("unchecked")
    public NioFlow<T> background(String name, Consumer<T> effect) {
        nioEngine.append(new Background(name, (Consumer<Object>) effect, List.of()));
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> NioFlow<R> adapt(Function<T, R> function) {
        nioEngine.append(new Stage(anonymousName("adapt"), asObjectFunction(function), false, null, List.of()));
        return (NioFlow<R>) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public NioFlow<T> filter(Predicate<T> predicate) {
        nioEngine.append(new Filter((Predicate<Object>) predicate, List.of()));
        return this;
    }

    @Override
    public Condition<T> when(Predicate<T> predicate) {
        throw new UnsupportedOperationException("when() is not implemented yet");
    }

    @Override
    public Cases<T> match() {
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
     * Ejecución efímera creada por just(): chain local (snapshot compartido + links
     * propios) que se ejecuta con engine.call(input, context, chain) sin sellar ni
     * mutar la definición compartida. adapt() re-tipa el pipeline devolviendo la
     * misma instancia vista como NioFlow<R>.
     */
    private static final class ExecutionNioFlow<T> implements NioFlow<T> {

        private final NioEngine nioEngine;
        private final List<Link> links;
        private final Map<String, Object> context = new ConcurrentHashMap<>();
        private final AtomicInteger anonymousLinks = new AtomicInteger();
        private Object seed;

        private ExecutionNioFlow(NioEngine nioEngine, Object seed) {
            this.nioEngine = nioEngine;
            this.seed = seed;
            this.links = new ArrayList<>(nioEngine.chain());
        }

        @Override
        @SuppressWarnings("unchecked")
        public <I> NioFlow<I> just(I input) {
            this.seed = input;
            return (NioFlow<I>) this;
        }

        @Override
        public <I> NioFlow<T> justAll(Iterable<I> inputs) {
            throw new UnsupportedOperationException("justAll() applies to the shared flow, not to a just() execution");
        }

        @Override
        public NioFlow<T> handle(Function<T, T> function) {
            return handle(anonymousName("stage"), function);
        }

        @Override
        public NioFlow<T> handle(String name, Function<T, T> function) {
            links.add(new Stage(name, asObjectFunction(function), false, null, List.of()));
            return this;
        }

        @Override
        public NioFlow<T> background(Consumer<T> effect) {
            return background(anonymousName("background"), effect);
        }

        @Override
        @SuppressWarnings("unchecked")
        public NioFlow<T> background(String name, Consumer<T> effect) {
            links.add(new Background(name, (Consumer<Object>) effect, List.of()));
            return this;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <R> NioFlow<R> adapt(Function<T, R> function) {
            links.add(new Stage(anonymousName("adapt"), asObjectFunction(function), false, null, List.of()));
            return (NioFlow<R>) this;
        }

        @Override
        @SuppressWarnings("unchecked")
        public NioFlow<T> filter(Predicate<T> predicate) {
            links.add(new Filter((Predicate<Object>) predicate, List.of()));
            return this;
        }

        @Override
        public Condition<T> when(Predicate<T> predicate) {
            throw new UnsupportedOperationException("when() is not implemented yet");
        }

        @Override
        public Cases<T> match() {
            throw new UnsupportedOperationException("match() is not implemented yet");
        }

        @Override
        @SuppressWarnings("unchecked")
        public T execute() {
            return (T) nioEngine.call(seed, context, links).join();
        }

        @Override
        public void close() {
            // La ejecución no es dueña del engine; no hay recursos propios que liberar.
        }

        private String anonymousName(String prefix) {
            return prefix + "-" + anonymousLinks.getAndIncrement();
        }
    }
}
