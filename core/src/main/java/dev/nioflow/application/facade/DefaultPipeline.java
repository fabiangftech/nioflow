package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.Pipeline;
import dev.nioflow.core.facade.PipelineRun;
import dev.nioflow.core.facade.PreparedChain;

/**
 * A prebuilt per-request pipeline: the shared chain plus a recorded segment,
 * validated and compiled once into a {@link PreparedChain} at build time. Every
 * {@code just()} allocates a {@link DefaultPipelineRun} and dispatches off that
 * plan — no chain copy, no rescan, no per-dispatch fusion scan.
 *
 * <p>Immutable and thread-safe: the plan is fixed at construction, so any number
 * of concurrent requests share it and touch nothing of each other's.
 */
final class DefaultPipeline<I, R> implements Pipeline<I, R> {

    private final NioEngine nioEngine;
    // The input token from the flow that built this pipeline (null with
    // create()): lets just() reject a value that is not an I, exactly as
    // DefaultNioFlow.just() does — the net for unchecked casts.
    private final Class<I> inputType;
    private final PreparedChain prepared;

    DefaultPipeline(NioEngine nioEngine, Class<I> inputType, PreparedChain prepared) {
        this.nioEngine = nioEngine;
        this.inputType = inputType;
        this.prepared = prepared;
    }

    @Override
    public PipelineRun<R> just(I input) {
        if (inputType != null && input != null && !inputType.isInstance(input)) {
            throw new IllegalArgumentException("This pipeline accepts " + inputType.getName()
                    + " as input, but got " + input.getClass().getName());
        }
        return new DefaultPipelineRun<>(nioEngine, prepared, input);
    }
}
