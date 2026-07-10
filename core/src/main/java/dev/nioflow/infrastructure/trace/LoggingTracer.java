package dev.nioflow.infrastructure.trace;

import dev.nioflow.core.facade.NioFlowTracer;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;

/**
 * {@link NioFlowTracer} that logs every transition through the JDK's
 * {@link Logger} — no logging framework required, and it routes into
 * whatever backend the application wires (JUL by default, SLF4J via bridge).
 * Messages include raw payloads: mind what flows through a traced nio-flow.
 */
public final class LoggingTracer implements NioFlowTracer {

    private final Logger logger;
    private final Level level;

    private LoggingTracer(Logger logger, Level level) {
        this.logger = logger;
        this.level = level;
    }

    /**
     * Logs at DEBUG on the {@code dev.nioflow.trace} logger — the usual choice:
     * silent in production until that logger is turned up.
     *
     * @return a tracer logging every transition at DEBUG
     */
    public static LoggingTracer debug() {
        return new LoggingTracer(System.getLogger("dev.nioflow.trace"), Level.DEBUG);
    }

    /**
     * Logs at INFO on the {@code dev.nioflow.trace} logger — for short-lived
     * diagnostics where DEBUG output would be filtered away.
     *
     * @return a tracer logging every transition at INFO
     */
    public static LoggingTracer info() {
        return new LoggingTracer(System.getLogger("dev.nioflow.trace"), Level.INFO);
    }

    /**
     * Full control over destination and level.
     *
     * @param logger the logger every transition is written to
     * @param level  the level every transition is logged at
     * @return a tracer logging to that logger at that level
     */
    public static LoggingTracer to(Logger logger, Level level) {
        return new LoggingTracer(logger, level);
    }

    @Override
    public void injected(long value, Object payload) {
        logger.log(level, "value {0} injected: {1}", value, payload);
    }

    @Override
    public void stage(long value, String name, boolean async, Object in, Object out, Throwable error) {
        String kind = async ? "submit" : "handle";
        String stage = name == null ? kind : name;
        if (error == null) {
            logger.log(level, "value {0} {1}[{2}]: {3} -> {4}", value, kind, stage, in, out);
        } else {
            logger.log(level, "value {0} {1}[{2}]: {3} -> failed: {4}", value, kind, stage, in, error);
        }
    }

    @Override
    public void lane(long value, int decision, boolean outcome) {
        logger.log(level, "value {0} decision {1} -> {2}", value, decision, outcome);
    }

    @Override
    public void dropped(long value, Object payload) {
        logger.log(level, "value {0} dropped by filter: {1}", value, payload);
    }

    @Override
    public void fannedOut(long value, int children) {
        logger.log(level, "value {0} fanned out into {1} values", value, children);
    }

    @Override
    public void recovered(long value, Throwable error, Object fallback) {
        logger.log(level, "value {0} recovered from {1} with {2}", value, error, fallback);
    }

    @Override
    public void failed(long value, Throwable error) {
        logger.log(level, "value {0} failed: {1}", value, error);
    }

    @Override
    public void completed(long value, Object result) {
        logger.log(level, "value {0} completed: {1}", value, result);
    }
}
