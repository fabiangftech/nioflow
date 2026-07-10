package dev.nioflow.core.model;

import java.util.List;

/**
 * A point-in-time snapshot of a running nio-flow: the chain's shape (one readable
 * line per link) and the engine's counters. {@code toString()} renders the full
 * dump — logging the nio-flow itself is enough to see what it looks like inside.
 */
public record Diagnostics(List<String> chain, int submissionQueue, int completionQueue,
                          int active, int parked, int batched, long injected,
                          boolean sealed, boolean closed) {

    @Override
    public String toString() {
        StringBuilder dump = new StringBuilder("Pipeline[")
                .append("active=").append(active)
                .append(", parked=").append(parked)
                .append(", batched=").append(batched)
                .append(", submissionQueue=").append(submissionQueue)
                .append(", completionQueue=").append(completionQueue)
                .append(", injected=").append(injected)
                .append(", sealed=").append(sealed)
                .append(", closed=").append(closed)
                .append(']');
        for (int i = 0; i < chain.size(); i++) {
            dump.append("\n  ").append(i + 1).append(". ").append(chain.get(i));
        }
        return dump.toString();
    }
}
