package dev.nioflow.infrastructure.reactive;

/**
 * A value-carrying reactive step ({@code handleMono} / {@code adaptMono})
 * returned {@code Mono.empty()} mid-chain.
 *
 * <p>Such a step must emit exactly one value: the chain injects that value into
 * the next step, and {@code Mono.empty()} would inject a silent {@code null}
 * instead — a {@link NullPointerException} waiting for the first cache miss (RFC
 * 0027). So an empty value-carrying step is surfaced as this ordinary stage
 * failure, which {@code recover()} catches like any other, rather than a null
 * nobody reasoned about.
 *
 * <p>To model genuine absence, do it explicitly inside the Mono — map to an
 * {@code Optional} or a sentinel — or end the flow with {@code filter()} /
 * handle it with {@code recover()}. Note this is only about MID-CHAIN steps: an
 * empty <em>terminal</em> ({@code executeMono}) still surfaces as an empty Mono,
 * the correct reactive meaning of "no value".
 */
public final class EmptyMonoException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public EmptyMonoException(String step) {
        super("Reactive step '" + step + "' returned Mono.empty(): a value-carrying step (handleMono/adaptMono)"
                + " must emit exactly one value. Model absence explicitly — map to an Optional or a sentinel inside"
                + " the Mono, or use filter()/recover() — instead of injecting a silent null into the next step.");
    }
}
