package dev.nioflow.core.facade;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Pipeline tipado: T es el tipo del valor en el punto actual de la cadena.
 * adapt() es el único paso que cambia T; el resto lo preserva.
 */
public interface NioFlow<T> extends AutoCloseable {

    <I> NioFlow<I> just(I input);

    <I> NioFlow<T> justAll(Iterable<I> inputs);

    NioFlow<T> handle(Function<T, T> function);

    NioFlow<T> handle(String name, Function<T, T> function);

    NioFlow<T> background(Consumer<T> effect);

    NioFlow<T> background(String name, Consumer<T> effect);

    <R> NioFlow<R> adapt(Function<T, R> function);

    NioFlow<T> filter(Predicate<T> predicate);

    Condition<T> when(Predicate<T> predicate);

    Cases<T> match();

    T execute();
}
