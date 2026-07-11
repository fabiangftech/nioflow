package dev.nioflow.core.model;

import java.util.List;
import java.util.function.Consumer;

/**
 * A fire-and-forget effect: the consumer is launched on the executor with the
 * value's payload as of this point, and the value moves on immediately — neither
 * the value nor any caller waiting on it ({@code join}, {@code call}) waits for
 * the effect. A throwing effect is reported to {@code onError} handlers and
 * metrics; the value itself is unaffected.
 *
 * @param name   the effect name reported by failures and metrics, or null
 * @param effect the side effect, receiving the payload as of this link
 * @param guards the lane markers deciding which values run this link
 */
public record Background(String name, Consumer<Object> effect, List<Guard> guards) implements Link {
}
