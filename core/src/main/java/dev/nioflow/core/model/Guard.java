package dev.nioflow.core.model;

/**
 * Lane marker for a link: the link only runs for values whose recorded outcome of
 * {@code decision} equals {@code expected}. A link inside nested branches carries
 * one guard per enclosing {@code when}.
 */
public record Guard(int decision, boolean expected) {
}
