package dev.nioflow.springwebfluxwithnioflow.config;

import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The WebFlux half of the bridge: the trace id enters the request ONCE, here,
 * and lives in Reactor's subscriber context from then on.
 *
 * <p>The other half is {@code propagate(TRACE)} on the flow: every
 * {@code executeMono()} lifts this entry into the execution's context, so no
 * controller method and no service method takes a {@code traceId} parameter.
 *
 * <p>This method runs on NETTY'S EVENT LOOP, which is why the fallback id is a
 * counter and not {@code UUID.randomUUID()}: the first randomUUID seeds
 * SecureRandom, which reads /dev/urandom — a blocking file read on the one thread
 * that must never block. BlockHound fails the build over it (it did, while this
 * filter was being written). A stage would have been free to call it; a WebFilter
 * is not.
 */
@Component
public class TraceWebFilter implements WebFilter {

    private static final String HEADER = "X-Trace-Id";

    private final AtomicLong fallback = new AtomicLong();

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String traceId = exchange.getRequest().getHeaders().getFirst(HEADER);
        if (traceId == null || traceId.isBlank()) {
            traceId = "trace-" + fallback.incrementAndGet();
        }
        exchange.getResponse().getHeaders().set(HEADER, traceId);
        return chain.filter(exchange).contextWrite(Context.of(FlowKeys.TRACE.name(), traceId));
    }
}
