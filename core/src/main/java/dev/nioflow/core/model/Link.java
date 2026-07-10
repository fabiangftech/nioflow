package dev.nioflow.core.model;

import java.util.List;

/**
 * One element of the shared chain: a value-transforming {@link Stage}, a branching
 * {@link Decision}, a {@link Recovery} fallback, a dropping {@link Filter}, a
 * grouping {@link Batch} or a splitting {@link FanOut}. A value skips any link
 * whose guards it does not satisfy.
 */
public sealed interface Link permits Stage, Decision, Recovery, Filter, Batch, FanOut {

    /**
     * The lane markers deciding which values run this link — empty for a main-line
     * link, one guard per enclosing fork for a link inside lanes.
     *
     * @return the guards a value must satisfy for this link to run
     */
    List<Guard> guards();
}
