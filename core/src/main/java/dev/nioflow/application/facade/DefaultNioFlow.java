package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Cases;
import dev.nioflow.core.facade.Condition;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.Background;
import dev.nioflow.core.model.Filter;
import dev.nioflow.core.model.Stage;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public class DefaultNioFlow implements NioFlow {

    private final NioEngine nioEngine;
    // Seed y context por hilo: N requests concurrentes pueden hacer just() + execute()
    // sobre la misma instancia sin pisarse.
    private final ThreadLocal<Object> seed = new ThreadLocal<>();
    private final ThreadLocal<Map<String, Object>> context = ThreadLocal.withInitial(ConcurrentHashMap::new);
    private final AtomicInteger anonymousLinks = new AtomicInteger();

    public DefaultNioFlow() {
        this(new DefaultNioEngine());
    }

    public DefaultNioFlow(NioEngine nioEngine) {
        this.nioEngine = nioEngine;
    }

    @Override
    public <T> T just(T input) {
        seed.set(input);
        return input;
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
    @SuppressWarnings("unchecked")
    public <T> T execute() {
        nioEngine.seal();
        try {
            return (T) nioEngine.call(seed.get(), context.get()).join();
        } finally {
            seed.remove();
            context.remove();
        }
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
}
