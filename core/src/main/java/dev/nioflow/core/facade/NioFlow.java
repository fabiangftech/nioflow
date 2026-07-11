package dev.nioflow.core.facade;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Pipeline tipado de punta a punta: I es el tipo de entrada (lo que acepta
 * just), T es el tipo del valor en el punto actual de la cadena. adapt() es
 * el único paso que cambia T; el resto lo preserva.
 */
public interface NioFlow<I, T> extends AutoCloseable {

    NioFlow<I, T> just(I input);

    NioFlow<I, T> justAll(Iterable<I> inputs);

    NioFlow<I, T> handle(Function<T, T> function);

    NioFlow<I, T> handle(String name, Function<T, T> function);

    NioFlow<I, T> background(Consumer<T> effect);

    NioFlow<I, T> background(String name, Consumer<T> effect);

    <R> NioFlow<I, R> adapt(Function<T, R> function);

    NioFlow<I, T> filter(Predicate<T> predicate);

    Condition<I, T> when(Predicate<T> predicate);

    Cases<I, T> match();

    T execute();
}
