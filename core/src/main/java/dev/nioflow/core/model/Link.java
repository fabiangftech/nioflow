package dev.nioflow.core.model;

import java.util.List;

/**
 * One element of the shared chain: a value-transforming {@link Stage}, a branching
 * {@link Decision}, a {@link Recovery} fallback, a dropping {@link Filter}, a
 * grouping {@link Batch} or a splitting {@link FanOut}. A value skips any link
 * whose guards it does not satisfy.
 */
public sealed interface Link permits Stage, Decision, Recovery, Filter, Batch, FanOut {

    List<Guard> guards();
}
