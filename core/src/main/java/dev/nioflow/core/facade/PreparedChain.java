package dev.nioflow.core.facade;

/**
 * An opaque, immutable dispatch plan for a chain: the links plus the fusion
 * windows compiled for them, built once and handed back to
 * {@link NioEngine#call(Object, java.util.Map, PreparedChain, Object)} on every
 * request. The plan is the same one {@code seal()} builds for the shared
 * definition — a per-request pipeline is simply allowed to carry one, so it
 * stops rescanning and re-deriving fusion runs on every dispatch.
 *
 * <p>Produced by {@link NioEngine#prepare(java.util.List)} (validated, for a
 * reusable {@link Pipeline}) or {@link NioEngine#planFor(java.util.List)}
 * (compiled only, for a cached per-request snapshot). The type is opaque on
 * purpose: what it holds is the engine's business, and the only thing a caller
 * does with it is hand it back to {@code call}.
 */
public interface PreparedChain {
}
