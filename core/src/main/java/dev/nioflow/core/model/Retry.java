package dev.nioflow.core.model;

import java.time.Duration;

/**
 * Per-stage retry policy: total attempts (>= 1) with a backoff between them,
 * multiplied on every subsequent retry (1.0 = fixed backoff). Attempts run on
 * the worker; with a stage timeout, the budget applies to EACH attempt. Once
 * attempts are exhausted, the last failure flows to the recovery path.
 */
public record Retry(int attempts, Duration backoff, double multiplier) {

    public Retry {
        if (attempts < 1) {
            throw new IllegalArgumentException("attempts must be >= 1");
        }
        if (backoff == null || backoff.isNegative()) {
            throw new IllegalArgumentException("backoff must be >= 0");
        }
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("multiplier must be >= 1.0");
        }
    }

    public static Retry of(int attempts, Duration backoff) {
        return new Retry(attempts, backoff, 1.0);
    }

    public static Retry exponential(int attempts, Duration initialBackoff) {
        return new Retry(attempts, initialBackoff, 2.0);
    }

    /** Delay before the next attempt, given how many attempts already ran. */
    public long delayNanos(int completedAttempts) {
        return (long) (backoff.toNanos()
                       * Math.pow(multiplier, (double) completedAttempts - 1));
    }
}
