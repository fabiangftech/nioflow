# Observability

## Flow-level taps

The lightest tool — see [Pipeline API](pipeline-api.md):

```java
flow.onComplete(receipt -> metrics.increment("orders.completed"))
    .onError(error -> alerting.notify(error));
```

On the shared definition these observe **every** execution; `onError` also sees non-execution errors (values rejected or dropped by backpressure, failing `background` effects). Callbacks run on engine threads: keep them fast and never throw — a throwing complete callback is contained and reported through `onError`.

## The metrics SPI

`NioFlowMetrics` is a small interface with no-op defaults — implement only what you need and install it once:

```java
engine.metrics(new NioFlowMetrics() {
    @Override public void executionCompleted(long nanos) { histogram.record(nanos); }
    @Override public void stageCompleted(String stage, long nanos) { perStage.record(stage, nanos); }
    @Override public void stageRetried(String stage) { retries.increment(stage); }
});
```

| Callback | Fires when |
|---|---|
| `executionCompleted / Failed / Filtered` | An execution ends, classified, with its latency |
| `stageCompleted(name, nanos)` | A stage function returns (timed on the worker, fused runs included) |
| `stageRetried(name)` | A retry attempt is about to run |
| `recoveryApplied(name)` | A `recover` caught a failure |
| `forkStarted / forkCompleted / forkFailed(name, …)` | A detached sub-flow spawned, finished, or failed unrecovered. **Separate from the execution metrics on purpose**: a fork's latency is not the request's — the response never waited for it |
| `forksInFlight(count)` | Detached sub-flows running right now — what tells you whether a fork storm is real |
| `valueDropped()` | Backpressure `DROP` discarded a value |
| `queueDepth(depth)` | The fire-and-forget results queue changes size |

`null` metrics (the default) means literally zero instrumentation on the hot path.

## OpenTelemetry adapter

Bring `io.opentelemetry:opentelemetry-api` and install the ready-made exporter:

```java
engine.metrics(new OpenTelemetryMetrics(meter));
```

It publishes `nioflow.execution.duration` and `nioflow.stage.duration` histograms (µs, stages tagged with `nioflow.stage`), completion/failure/filter counters, recovery and drop counters, and a `nioflow.queue.depth` gauge — with attribute instances cached so instrumentation adds no allocation per request.

Detached sub-flows report apart from the request they came from: `nioflow.fork.duration`, `nioflow.forks.started` / `.failed`, and a `nioflow.fork.in_flight` gauge, all tagged with `nioflow.fork`.

Stages **inside** a fork report through the ordinary `stageCompleted` / `stageRetried` hooks under their own names — one of the main reasons to prefer `fork` over a hand-rolled `background`.

## Reading latency

- A **rate-limited** stage's wait is included in its stage latency — throttling is visible without extra wiring.
- A **timeout+retry** stage reports each attempt through `stageRetried`; the execution latency covers all attempts.
- **Filtered** executions are classified separately, so deliberate cuts don't pollute failure rates.
