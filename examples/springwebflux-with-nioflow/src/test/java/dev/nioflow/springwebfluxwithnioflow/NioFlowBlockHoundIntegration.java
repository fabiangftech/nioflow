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
 * <li>the boss's own idle wait — each boss is a spin-then-park event loop
 *     ({@code BossLoop}, RFC 0009), and a boss with nothing to do burns
 *     {@code Thread.onSpinWait} and then {@code LockSupport.park}, both inside
 *     {@code BossLoop.runLoop}. That is the loop waiting for work, not the engine
 *     waiting on something, so {@code runLoop} is allowed. USER code the boss
 *     inlines (a {@code handleSync} stage, a {@code when}/{@code match} predicate)
 *     must still trip — it runs under {@code BossLoop.run(task)}, which is
 *     disallowed, and BlockHound's closest-frame-wins rule catches it before
 *     {@code runLoop}. (Before RFC 0009 the boss was a single-thread
 *     {@code ThreadPoolExecutor} and this allowance was {@code getTask}; it moved
 *     with the loop.)</li>
 * <li>Jackson's deserializer cache — it takes a ReentrantLock the first time it
 *     builds a deserializer for a type, and BlockHound sees the park inside that
 *     lock. It is framework code on the cold path of the FIRST request for a
 *     type, on Netty's thread, where WebFlux always decodes bodies. Without this
 *     the very first POST of the suite 500s, which would say nothing about
 *     nioflow.</li>
 * </ul>
 *
 * <p>Note what is NOT allowed: the only nioflow frame allowed is the boss loop's
 * idle wait ({@code BossLoop.runLoop}), and its user-code dispatch
 * ({@code BossLoop.run}) is explicitly disallowed. So if a stage's work — anything
 * the boss inlines, or anything that lands on an event loop — ever blocks, this
 * suite goes red.
 */
public class NioFlowBlockHoundIntegration implements BlockHoundIntegration {

    @Override
    public void applyTo(BlockHound.Builder builder) {
        builder.nonBlockingThreadPredicate(current ->
                current.or(thread -> thread.getName().startsWith("nio-flow-boss-")));
        // The boss's own idle wait: the spin-then-park loop's Thread.onSpinWait and
        // LockSupport.park are legitimate (the loop waiting for work). But USER code
        // the boss inlines — a handleSync stage, a when()/match() predicate — must
        // still trip: it runs under BossLoop.run(task), which is disallowed below.
        // BlockHound walks the stack outward and the closest matching rule wins, so
        // a boss-inlined stage's park hits `run` (disallowed) before `runLoop`.
        builder.allowBlockingCallsInside("dev.nioflow.application.facade.BossLoop", "runLoop");
        builder.disallowBlockingCallsInside("dev.nioflow.application.facade.BossLoop", "run");
        builder.allowBlockingCallsInside(
                "tools.jackson.databind.deser.DeserializerCache", "_createAndCacheValueDeserializer");
    }
}
