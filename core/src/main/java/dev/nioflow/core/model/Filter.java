package dev.nioflow.core.model;

import java.util.List;
import java.util.function.Predicate;

/**
 * A deliberate drop point: values failing the predicate leave the nio-flow — they
 * fire neither completion nor error handlers and stop counting as in flight, which
 * also frees their backpressure slot. A throwing predicate is a normal stage
 * failure instead.
 */
public record Filter(Predicate<Object> predicate, List<Guard> guards) implements Link {
}
