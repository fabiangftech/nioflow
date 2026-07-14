package dev.nioflow.springwebfluxwithnioflow;

import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

/**
 * What BlockHound is told about nioflow's threads, in the example's TEST scope
 * only (registered through META-INF/services — the JUnit platform module picks
 * it up when it installs BlockHound over this JVM).
 *
 * <p>Two rules, and they are the engine's own:
 *
 * <ul>
 * <li><b>The virtual workers MAY block.</b> They are not marked, so BlockHound
 *     ignores them — which is the design: a stage parks a virtual thread that
 *     unmounts, and the blocking repository call in this example is safe for
 *     exactly that reason. This is the "allowlist": nothing to allow, because
 *     nothing about a virtual worker is non-blocking.</li>
 * <li><b>The bosses MUST NOT.</b> A boss orchestrates every execution pinned to
 *     it and never runs user code; one blocking call in there stalls all of them,
 *     the same way a blocking call stalls a Netty event loop. So the boss threads
 *     are marked non-blocking, and BlockHound holds them to it for the whole
 *     suite — the invariant becomes mechanical instead of documented.</li>
 * </ul>
 *
 * <p>Then two allowances, and neither of them is nioflow's:
 *
 * <ul>
 * <li>the boss's own idle park — each boss is a single-thread executor, and a
 *     thread with nothing to do blocks on its task queue. That is the pool
 *     waiting for work, not the engine waiting on something;</li>
 * <li>Jackson's deserializer cache — it takes a ReentrantLock the first time it
 *     builds a deserializer for a type, and BlockHound sees the park inside that
 *     lock. It is framework code on the cold path of the FIRST request for a
 *     type, on Netty's thread, where WebFlux always decodes bodies. Without this
 *     the very first POST of the suite 500s, which would say nothing about
 *     nioflow.</li>
 * </ul>
 *
 * <p>Note what is NOT allowed: any blocking call from {@code dev.nioflow}. If a
 * stage's work ever ended up on a boss or on an event loop, this suite goes red.
 */
public class NioFlowBlockHoundIntegration implements BlockHoundIntegration {

    @Override
    public void applyTo(BlockHound.Builder builder) {
        builder.nonBlockingThreadPredicate(current ->
                current.or(thread -> thread.getName().startsWith("nio-flow-boss-")));
        builder.allowBlockingCallsInside("java.util.concurrent.ThreadPoolExecutor", "getTask");
        builder.allowBlockingCallsInside(
                "tools.jackson.databind.deser.DeserializerCache", "_createAndCacheValueDeserializer");
    }
}
