package dev.nioflow.core.model;

/**
 * Wraps a failure of a named stage so errors say where they happened. Only named
 * stages wrap — unnamed ones deliver the thrown exception untouched. Error handlers
 * and recoveries can branch on {@link #stage()} without parsing messages.
 */
public final class StageException extends RuntimeException {

    /** The declared name of the failing stage. */
    private final String stage;

    /**
     * Wraps a named stage's failure.
     *
     * @param stage the name of the stage that failed
     * @param cause the failure the stage threw
     */
    public StageException(String stage, Throwable cause) {
        super("stage '" + stage + "' failed: " + cause, cause);
        this.stage = stage;
    }

    /**
     * The name of the stage that failed, as declared on the nio-flow.
     *
     * @return the stage name, never null
     */
    public String stage() {
        return stage;
    }
}
