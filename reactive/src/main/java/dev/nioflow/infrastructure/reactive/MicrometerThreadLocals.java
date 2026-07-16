package dev.nioflow.infrastructure.reactive;

import io.micrometer.context.ContextRegistry;
import io.micrometer.context.ThreadLocalAccessor;

/**
 * The one class that touches {@code io.micrometer:context-propagation}, and
 * nothing else does (RFC 0033). It is referenced only from
 * {@link ThreadLocalContext#get} after its probe confirmed the dependency is
 * present, so this class is never loaded — and its {@code ContextRegistry}
 * reference never resolved — on a consumer that did not bring it. Absent the
 * dependency the context bridge stays the plain subscriber-context string
 * lookup it always was.
 */
final class MicrometerThreadLocals {

    private MicrometerThreadLocals() {
    }

    /**
     * The current value a registered {@link ThreadLocalAccessor} holds under
     * {@code name}, or null if none does. Keys line up by NAME, exactly as the
     * subscriber-context path does: a {@code propagate(Context.Key.of("traceId"))}
     * reads the accessor whose {@code key()} is {@code "traceId"}.
     */
    static Object read(String name) {
        for (ThreadLocalAccessor<?> accessor : ContextRegistry.getInstance().getThreadLocalAccessors()) {
            if (name.equals(accessor.key())) {
                return accessor.getValue();
            }
        }
        return null;
    }
}
