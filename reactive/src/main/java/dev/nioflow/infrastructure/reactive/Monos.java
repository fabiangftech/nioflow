package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Cancellable;
import dev.nioflow.core.facade.Context;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Where the Reactor side and the engine side meet, in one place: a {@link Mono}
 * over one engine execution, and the subscriber-context seed the bridge lifts.
 * Shared by the per-request step ({@link DefaultReactiveStep#executeMono}) and
 * the prebuilt-pipeline pipe ({@link DefaultReactiveFlow}), so both cancel the
 * same way and read the same whitelist.
 */
final class Monos {

    private Monos() {
    }

    /**
     * A Mono over an engine execution: {@code doOnCancel} fires on dispose (and
     * on a downstream {@code take(1)}, a timeout, a disconnected client) and
     * hands the cancellation to the execution itself — the chain stops at the
     * next link and the in-flight async call is cancelled.
     */
    static <T> Mono<T> fromCancellable(Cancellable<T> handle) {
        return Mono.fromFuture(handle.future()).doOnCancel(handle::cancel);
    }

    /**
     * The declared keys, and only them — the bridge is a WHITELIST. Each is read
     * from two sources, in order: Reactor's subscriber {@link ContextView} first
     * (a value a caller wrote with {@code contextWrite}, or that automatic context
     * propagation already lifted there), then — for a key the context does not
     * carry — a registered ThreadLocalAccessor of the same name (RFC 0033), which
     * is where Micrometer Tracing / Sleuth / MDC keep it. A key neither source
     * carries is not seeded at all: no null entry, and a stage reading it gets
     * back null exactly as it would for a key nobody ever wrote. Keys line up by
     * NAME on both paths.
     */
    static Map<String, Object> seed(ContextView view, List<Context.Key<?>> keys) {
        Map<String, Object> seeded = HashMap.newHashMap(keys.size());
        for (Context.Key<?> key : keys) {
            var fromView = view.getOrEmpty(key.name());
            if (fromView.isPresent()) {
                seeded.put(key.name(), fromView.get());
                continue;
            }
            Object fromThreadLocal = ThreadLocalContext.get(key.name());
            if (fromThreadLocal != null) {
                seeded.put(key.name(), fromThreadLocal);
            }
        }
        return seeded;
    }
}
