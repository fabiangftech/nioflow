package dev.nioflow.core.model;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * The stage that does not park: its function returns a {@link CompletionStage}
 * instead of a value. A worker INVOKES it (the call is user code — it builds a
 * request, it may subscribe) and is released immediately; the boss resumes when
 * the stage completes. Nothing waits in between.
 *
 * <p>Against a {@link Stage} that blocks on a remote call, this trades fusion
 * for heap and cancellation: an AsyncStage is a dispatch boundary (it ends a
 * fused run), but an in-flight request retains no parked virtual thread, and
 * the CompletionStage is a HANDLE — so the timeout can <b>cancel</b> the call,
 * where a Stage timeout can only abandon the worker waiting on it.
 *
 * <p>No Reactor in the signature and none in the engine: {@code CompletionStage}
 * is {@code java.util.concurrent}, so this link serves {@code HttpClient
 * .sendAsync}, the AWS SDK and a Cassandra driver exactly as well as it serves
 * a {@code Mono} (which arrives through {@code mono.toFuture()}).
 */
public record AsyncStage(String name,
                         Function<Object, CompletionStage<Object>> call,
                         Duration timeout,
                         Retry retry,
                         List<Guard> guards) implements Link {
}
