package dev.nioflow.core.facade;

/**
 * Typed view over the per-execution context: scratch state shared by the
 * stages of ONE execution (request metadata, auth info, correlation ids)
 * without threading it through the value type. Stages opt in through the
 * handle(name, (value, ctx) -> ...) overloads; plain stages never see it
 * and never pay for it (the backing map is created on the first put).
 *
 * Keys are name-based so a context map handed to engine.call(input, map)
 * interoperates: Key.of("auth") reads and writes map entry "auth". Stage
 * applications within one execution are serialized by the engine (one
 * continuation at a time), so implementations need no synchronization.
 */
public interface Context {

    /** The value under the key, or null when absent. */
    <T> T get(Key<T> key);

    <T> T getOrDefault(Key<T> key, T fallback);

    /** Associates the value and returns this context, for chained puts. */
    <T> Context put(Key<T> key, T value);

    record Key<T>(String name) {

        public Key {
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("Key name must not be blank");
            }
        }

        public static <T> Key<T> of(String name) {
            return new Key<>(name);
        }
    }
}
