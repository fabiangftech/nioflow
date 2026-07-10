package dev.nioflow.core.model;

import java.util.List;
import java.util.function.Predicate;

/**
 * A {@code when} fork point: evaluates the predicate against the flowing value and
 * records the outcome under {@code id}, routing the value into the then or otherwise
 * lane of the links that follow.
 *
 * @param predicate decides which lane the value takes; evaluated once per value
 * @param id        the fork's identity, referenced by the {@link Guard}s of the
 *                  links inside its lanes
 * @param guards    the lane markers deciding which values reach this link — one per
 *                  enclosing fork when forks nest
 */
public record Decision(Predicate<Object> predicate, int id, List<Guard> guards) implements Link {
}
