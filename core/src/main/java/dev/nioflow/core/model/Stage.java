package dev.nioflow.core.model;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

/**
 * A value-transforming link of the shared chain. Sync stages (handle/adapt/when
 * branches) run on the engine's handle-worker pool; async stages (submit) run on
 * the supplied executor so blocking IO never occupies a handle worker. A non-null
 * {@code timeout} bounds an async stage: on expiry the worker is interrupted and
 * the value fails with a {@code TimeoutException}. A non-null {@code name} makes
 * failures self-describing: they arrive wrapped in a {@link StageException}.
 *
 * @param name     the stage name reported by failures, diagnostics and metrics, or
 *                 null for an unnamed stage
 * @param function the transformation applied to each flowing value
 * @param async    true for {@code submit} stages (executor), false for
 *                 {@code handle}/{@code adapt} stages (handle workers)
 * @param timeout  how long one async execution may run before it is cancelled, or
 *                 null for unbounded; only async stages honor it
 * @param guards   the lane markers deciding which values run this stage
 */
public record Stage(String name, Function<Object, Object> function, boolean async, Duration timeout,
                    List<Guard> guards) implements Link {
}
