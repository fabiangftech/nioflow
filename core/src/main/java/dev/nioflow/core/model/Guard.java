package dev.nioflow.core.model;

/**
 * Lane marker for a link: the link only runs for values whose recorded outcome of
 * {@code decision} equals {@code expected}. A link inside nested branches carries
 * one guard per enclosing {@code when}.
 *
 * @param decision the id of the {@link Decision} whose outcome this guard checks
 * @param expected the outcome a value must have recorded for the link to run —
 *                 true for the then lane, false for the otherwise lane
 */
public record Guard(int decision, boolean expected) {
}
