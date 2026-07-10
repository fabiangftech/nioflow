package dev.nioflow.core.model;

import java.util.List;

/**
 * A point-in-time snapshot of a running nio-flow: the chain's shape (one readable
 * line per link) and the engine's counters. {@code toString()} renders the full
 * dump — logging the nio-flow itself is enough to see what it looks like inside.
 *
 * @param chain           one readable line per declared link, in chain order,
 *                        e.g. {@code submit[save] if{0=true}}
 * @param submissionQueue values waiting for a handle worker
 * @param completionQueue reaped async results waiting to resume their value
 * @param active          values currently in flight (injected, not yet finished,
 *                        failed or dropped)
 * @param parked          values that reached the end of an unsealed chain and wait
 *                        for more links
 * @param batched         values waiting inside batch buffers for their group
 * @param injected        values admitted since the nio-flow was created
 * @param sealed          whether the chain is frozen against further links
 * @param closed          whether the nio-flow was shut down
 */
public record Diagnostics(List<String> chain, int submissionQueue, int completionQueue,
                          int active, int parked, int batched, long injected,
                          boolean sealed, boolean closed) {

    /** The full dump: one line of counters, then one indented line per link. */
    @Override
    public String toString() {
        StringBuilder dump = new StringBuilder("NioFlow[")
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
