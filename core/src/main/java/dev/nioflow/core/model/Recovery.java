package dev.nioflow.core.model;

import java.util.List;
import java.util.function.Function;

/**
 * An {@code onErrorResume} fallback link. Values flowing normally skip it; when a
 * value fails at an upstream link, the engine hands the error to the first recovery
 * downstream whose guards the value satisfies, and the fallback's result resumes
 * the flow from there.
 */
public record Recovery(Function<Throwable, Object> function, List<Guard> guards) implements Link {
}
