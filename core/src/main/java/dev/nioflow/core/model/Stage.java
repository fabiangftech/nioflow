package dev.nioflow.core.model;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

/**
 * sync is the opt-in boss-inline marker: the function runs on the boss,
 * skipping both thread hops. Same contract as Decision predicates — pure
 * CPU, sub-microsecond, never blocking; a throw fails the value, never the
 * boss task. Incompatible with timeout (nothing can cut a boss-inlined call)
 * and retry (backoff would park the boss) — validation rejects both.
 */
public record Stage(String name, Function<Object, Object> function, boolean sync, Duration timeout,
                    Retry retry, List<Guard> guards) implements Link {
}
