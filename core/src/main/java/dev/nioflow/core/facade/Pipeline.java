package dev.nioflow.core.facade;

/**
 * A per-request pipeline whose structure is fixed, declared once and dispatched
 * off a prebuilt plan.
 *
 * <p>The documented main path — {@code flow.just(x).handle(...).adapt(...)
 * .execute()} — rebuilds and re-interprets the chain on every request. When the
 * pipeline is the same every time (the lambdas do not close over the input),
 * declare it once with {@link NioFlow#pipeline(Segment)}: the segment is
 * recorded, <b>validated</b> and compiled a single time, and each request
 * allocates only its {@link PipelineRun}.
 *
 * <pre>
 * // startup — recorded, validated and compiled ONCE
 * Pipeline&lt;Integer, String&gt; charge = credits.pipeline(step -&gt; step
 *         .handle("charge", item -&gt; item * 2)
 *         .adapt(item -&gt; "EUR " + item));
 *
 * // per request — allocates a PipelineRun, nothing else
 * charge.just(cents).execute();
 * charge.just(cents).key(customerId).executeAsync();
 * </pre>
 *
 * <p>{@code I} is what {@link #just(Object)} accepts and {@code R} is what the
 * pipeline leaves — the {@link Segment}'s output type, which the compiler tracks
 * through {@code define}. A {@code Pipeline} snapshots the shared definition at
 * build time, so a later runtime {@code splice} on that definition does not
 * reach it: a prebuilt pipeline is stable by design.
 */
public interface Pipeline<I, R> {

    /**
     * Opens one request over the prebuilt plan. The handle carries only what
     * varies per request — the input, an optional ordering key, seeded context
     * and execution-scoped callbacks — and dispatches off the compiled plan
     * without copying or rescanning the chain.
     */
    PipelineRun<R> just(I input);
}
