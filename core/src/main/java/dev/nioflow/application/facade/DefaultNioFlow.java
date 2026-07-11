package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Cases;
import dev.nioflow.core.facade.Condition;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlow;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public class DefaultNioFlow implements NioFlow {

    private final NioEngine nioEngine;

    public DefaultNioFlow() {
        this(new DefaultNioEngine());
    }

    public DefaultNioFlow(NioEngine nioEngine) {
        this.nioEngine = nioEngine;
    }

    @Override
    public <T> T just(T input) {
        return null;
    }

    @Override
    public <T> NioFlow justAll(Iterable<T> inputs) {
        return null;
    }

    @Override
    public <T> NioFlow handle(Function<T, T> function) {
        return null;
    }

    @Override
    public <T> NioFlow handle(String name, Function<T, T> function) {
        return null;
    }

    @Override
    public <T> NioFlow background(Consumer<T> effect) {
        return null;
    }

    @Override
    public <T> NioFlow background(String name, Consumer<T> effect) {
        return null;
    }

    @Override
    public <T, R> NioFlow adapt(Function<T, R> function) {
        return null;
    }

    @Override
    public <T> NioFlow filter(Predicate<T> predicate) {
        return null;
    }

    @Override
    public <T> Condition when(Predicate<T> predicate) {
        return null;
    }

    @Override
    public Cases match() {
        return null;
    }

    @Override
    public <T> T execute() {
        return null;
    }

    @Override
    public void close() throws Exception {

    }
}
