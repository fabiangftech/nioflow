package dev.nioflow.core.facade;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public interface NioFlow extends AutoCloseable {

    <T> T just(T input);

    <T> NioFlow justAll(Iterable<T> inputs);

    <T> NioFlow handle(Function<T, T> function);

    <T> NioFlow handle(String name, Function<T, T> function);

    <T> NioFlow background(Consumer<T> effect);

    <T> NioFlow background(String name, Consumer<T> effect);

    <T, R> NioFlow adapt(Function<T, R> function);

    <T> NioFlow filter(Predicate<T> predicate);

    <T> Condition when(Predicate<T> predicate);

    Cases match();

    <T> T execute();
}
