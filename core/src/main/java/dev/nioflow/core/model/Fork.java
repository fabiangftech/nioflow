package dev.nioflow.core.model;

import java.util.List;

/**
 * Detached sub-flow: the value reaching this link is handed to a child
 * execution that runs on its own, and the main line continues IMMEDIATELY with
 * the same value — it never waits for the fork, not even for its first link.
 *
 * <p>The child is a full execution: its own decisions, its own copy of the
 * context, its own result future (dropped — nobody joins it). Failures it does
 * not recover() reach the error handlers, never the caller's future, exactly
 * like a Background effect. Its chain is immutable and guard-closed: every
 * guard inside it refers to a Decision declared inside it, which is what lets
 * the child size its own decision bitset.
 *
 * <p>Need the result back on the main line? That is FanOut, not Fork.
 */
public record Fork(String name, List<Link> chain, List<Guard> guards) implements Link {
}
