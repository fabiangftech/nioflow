package dev.nioflow.core.model;

import java.util.List;
import java.util.function.Predicate;

/**
 * A {@code when} fork point: evaluates the predicate against the flowing value and
 * records the outcome under {@code id}, routing the value into the then or otherwise
 * lane of the links that follow.
 */
public record Decision(Predicate<Object> predicate, int id, List<Guard> guards) implements Link {
}
