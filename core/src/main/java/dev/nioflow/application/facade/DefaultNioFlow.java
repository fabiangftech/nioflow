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

public class DefaultNioFlow implements NioFlow {

    private final NioEngine nioEngine;
    private final AtomicInteger anonymousLinks = new AtomicInteger();

    public DefaultNioFlow() {
        this(new DefaultNioEngine());
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
    public <T> NioFlow just(T input) {
        return new ExecutionNioFlow(nioEngine, input);
    }

    @Override
    public <T> NioFlow justAll(Iterable<T> inputs) {
        inputs.forEach(input -> nioEngine.inject(input, new ConcurrentHashMap<>()));
        return this;
    }

    @Override
    public <T> NioFlow handle(Function<T, T> function) {
        return handle(anonymousName("stage"), function);
    }

    @Override
    public <T> NioFlow handle(String name, Function<T, T> function) {
        nioEngine.append(new Stage(name, asObjectFunction(function), false, null, List.of()));
        return this;
    }

    @Override
    public <T> NioFlow background(Consumer<T> effect) {
        return background(anonymousName("background"), effect);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> NioFlow background(String name, Consumer<T> effect) {
        nioEngine.append(new Background(name, (Consumer<Object>) effect, List.of()));
        return this;
    }

    @Override
    public <T, R> NioFlow adapt(Function<T, R> function) {
        nioEngine.append(new Stage(anonymousName("adapt"), asObjectFunction(function), false, null, List.of()));
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> NioFlow filter(Predicate<T> predicate) {
        nioEngine.append(new Filter((Predicate<Object>) predicate, List.of()));
        return this;
    }

    @Override
    public <T> Condition when(Predicate<T> predicate) {
        throw new UnsupportedOperationException("when() is not implemented yet");
    }

    @Override
    public Cases match() {
        throw new UnsupportedOperationException("match() is not implemented yet");
    }

    @Override
    public <T> T execute() {
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
     * mutar la definición compartida.
     */
    private static final class ExecutionNioFlow implements NioFlow {

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
        public <T> NioFlow just(T input) {
            this.seed = input;
            return this;
        }

        @Override
        public <T> NioFlow justAll(Iterable<T> inputs) {
            throw new UnsupportedOperationException("justAll() applies to the shared flow, not to a just() execution");
        }

        @Override
        public <T> NioFlow handle(Function<T, T> function) {
            return handle(anonymousName("stage"), function);
        }

        @Override
        public <T> NioFlow handle(String name, Function<T, T> function) {
            links.add(new Stage(name, asObjectFunction(function), false, null, List.of()));
            return this;
        }

        @Override
        public <T> NioFlow background(Consumer<T> effect) {
            return background(anonymousName("background"), effect);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> NioFlow background(String name, Consumer<T> effect) {
            links.add(new Background(name, (Consumer<Object>) effect, List.of()));
            return this;
        }

        @Override
        public <T, R> NioFlow adapt(Function<T, R> function) {
            links.add(new Stage(anonymousName("adapt"), asObjectFunction(function), false, null, List.of()));
            return this;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> NioFlow filter(Predicate<T> predicate) {
            links.add(new Filter((Predicate<Object>) predicate, List.of()));
            return this;
        }

        @Override
        public <T> Condition when(Predicate<T> predicate) {
            throw new UnsupportedOperationException("when() is not implemented yet");
        }

        @Override
        public Cases match() {
            throw new UnsupportedOperationException("match() is not implemented yet");
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T execute() {
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
