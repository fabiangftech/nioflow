package dev.nioflow.core.model;

/**
 * Wraps a failure of a named stage so errors say where they happened. Only named
 * stages wrap — unnamed ones deliver the thrown exception untouched.
 */
public final class StageException extends RuntimeException {

    private final String stage;

    public StageException(String stage, Throwable cause) {
        super("stage '" + stage + "' failed: " + cause, cause);
        this.stage = stage;
    }

    public String stage() {
        return stage;
    }
}
