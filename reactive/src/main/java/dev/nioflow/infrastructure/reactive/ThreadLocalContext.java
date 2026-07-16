package dev.nioflow.infrastructure.reactive;

/**
 * The optional ThreadLocal side of the context bridge (RFC 0033).
 *
 * <p>Reactor's subscriber context is one source a {@code propagate()} key can
 * come from; a ThreadLocal is the other, and the one tracing actually uses.
 * Micrometer Tracing / Sleuth / MDC keep the trace id in a ThreadLocal exposed
 * through a registered {@code ThreadLocalAccessor}, not under a subscriber-context
 * entry whose key string equals {@code Context.Key.name()}. So a bridge that only
 * read the subscriber context would silently seed nothing against the very stack
 * people reach for propagation to serve. This closes that: a declared key the
 * subscriber context does not carry is looked up among the registered accessors.
 *
 * <p>Optional exactly like core's OpenTelemetry / Resilience4j adapters: the
 * probe runs once, nothing is required, and the micrometer types are touched
 * only from {@link MicrometerThreadLocals}, which is loaded only when the probe
 * passed. Absent {@code io.micrometer:context-propagation} this is inert and the
 * bridge stays the plain subscriber-context string lookup.
 */
final class ThreadLocalContext {

    // Probed once: io.micrometer:context-propagation is present on the classpath
    // (it is, in any Spring Boot WebFlux app) — or it is not, and this stays inert.
    private static final boolean AVAILABLE = probe();

    private ThreadLocalContext() {
    }

    private static boolean probe() {
        // initialize=false, so no static-init runs here (ExceptionInInitializerError
        // cannot arise): the only ways to fail are the class being absent
        // (ClassNotFoundException) or present-but-broken (a LinkageError, e.g.
        // NoClassDefFoundError). Either is exactly "optional dependency not usable"
        // → not available. This runs in a static-field initializer, so letting one
        // escape would fail the whole class's init and take Monos.seed down with it.
        try {
            Class.forName("io.micrometer.context.ContextRegistry", false,
                    ThreadLocalContext.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException | LinkageError absent) {
            return false;
        }
    }

    /**
     * The current ThreadLocal value registered under {@code name}, or null when
     * context-propagation is absent or no accessor carries it. Keys line up by
     * NAME, exactly as the subscriber-context path does.
     */
    static Object get(String name) {
        return AVAILABLE ? MicrometerThreadLocals.read(name) : null;
    }
}
