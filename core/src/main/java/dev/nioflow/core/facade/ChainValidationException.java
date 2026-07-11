package dev.nioflow.core.facade;

import java.util.List;

/**
 * Thrown by seal() — and by splice() on a sealed chain — when the chain has
 * structural problems: dangling guards, contradictory guards, duplicate
 * anchor names or dead recoveries. Failing at seal time means a broken
 * definition stops the deploy instead of producing runtime surprises; a
 * rejected splice leaves the previous chain untouched.
 */
public class ChainValidationException extends IllegalStateException {

    private final List<String> problems;

    public ChainValidationException(List<String> problems) {
        super("Invalid chain (" + problems.size() + " problem" + (problems.size() == 1 ? "" : "s") + "): "
                + String.join("; ", problems));
        this.problems = List.copyOf(problems);
    }

    public List<String> problems() {
        return problems;
    }
}
