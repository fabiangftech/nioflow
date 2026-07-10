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
 */
public record Stage(String name, Function<Object, Object> function, boolean async, Duration timeout,
                    List<Guard> guards) implements Link {
}
