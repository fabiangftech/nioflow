# RFC 0033 — `propagate` bridges strings, not trace context: make it work or say so

- **Status**: 📋 Proposed
- **Target**: `reactive` (`Monos.seed`, `ReactiveFlow.propagate`, and its docs / RFC 0005)
- **Depends on**: RFC 0005 (the context bridge this refines)
- **Severity**: **Medium-High** — the #1 reason people reach for reactive context propagation (tracing/MDC) compiles, runs, and silently seeds nothing
- **Realized by**: integrating Micrometer's `ContextRegistry` accessor path so ThreadLocal-bridged values are found, OR documenting loudly that `propagate` is a raw-string bridge that requires the producer to have written under the exact `Context.Key.name()`.

## The finding

`Monos.seed` (`Monos.java:40-43`) lifts declared keys out of Reactor's subscriber context with a plain string lookup:

```java
view.getOrEmpty(key.name()).ifPresent(value -> seeded.put(key.name(), value));
```

There is **no Micrometer `ContextRegistry` / context-propagation integration anywhere** in the module. In real WebFlux applications, trace context (Micrometer Tracing / Sleuth, `reactor.core.publisher.Hooks.enableAutomaticContextPropagation`, the `ServerWebExchange`) is stored under *typed, ThreadLocal-backed accessors* — not under a subscriber-context entry whose key is a `String` equal to your `Context.Key.name()`. So `propagate(TRACE)` only sees a value if some upstream code did exactly `contextWrite(c -> c.put("trace", ...))` with a matching literal name.

The result: the single most common thing users want context propagation *for* in WebFlux — carry a trace id / MDC across the reactive→engine boundary so downstream logs and spans correlate — will compile, run, and quietly seed an empty context. No error, no warning, no failing test. The RFC 0005 promise ("declared, never discovered — a reader sees exactly what crosses") is honest about *which* keys cross, but not about the fact that, wired against a normal tracing stack, *none* of them do.

## Why it blocks production

Silent tracing loss is uniquely bad: it is invisible until an incident, when the operator discovers the correlation ids they built the whole pipeline to carry are absent from exactly the logs they need. And it fails *toward* looking correct — the API call is present, the build is green, the `Context.Key` reads back fine inside a hand-written test that used `contextWrite` with the same string. The gap only shows against a real Micrometer/Sleuth producer, which the in-repo tests do not use.

## The options

1. **Integrate `ContextRegistry` (make it actually work).** In `Monos.seed`, when a declared key is absent from the plain `ContextView`, fall back to `ContextRegistry.getInstance()` accessors so a ThreadLocal-bridged value (the shape Micrometer Tracing uses) is found. Micrometer's `context-propagation` is already a compileOnly-friendly, tiny dependency; keep it optional (present only if the consumer brings it), consistent with the OpenTelemetry/Resilience4j pattern in `infrastructure`. This makes `propagate(TRACE)` do what its name says against the common stack.

2. **Document it as a raw-string bridge (minimum, do regardless).** State plainly in RFC 0005 and `webflux.md`: `propagate` lifts subscriber-context entries whose key *string* equals `Context.Key.name()`, and nothing else — no ThreadLocal, no Micrometer accessor, no `Hooks` bridge. Give the working recipe (`contextWrite(c -> c.put("traceId", id))` on the producer side) and name what does *not* work (out-of-the-box Micrometer Tracing). Honest, cheap, but leaves the common case a manual wiring exercise.

3. **Provide a first-class tracing recipe.** Ship a documented `propagateMdc(...)` / example that shows exactly how to bridge an MDC or a Micrometer trace context into `Context.Key`s, so the "declared, never discovered" philosophy is preserved (the user still names the keys) but the mechanism is real.

Recommended: **option 1** (make the headline use case work) *plus* **option 2's doc honesty**, because even with `ContextRegistry` the string-name matching rule still needs to be stated so users know how their keys line up.

## Testing

- A test that seeds a value through a Micrometer-style ThreadLocal accessor (not `contextWrite`) and asserts `propagate(KEY)` makes it visible in the per-execution `Context` inside a stage — the case that fails today.
- Keep the existing `ReactiveContextTest` (the `contextWrite`-by-name path) green: option 1 must be additive, not a replacement.
- Assert declaring no keys still allocates nothing and is one branch (RFC 0005's `monoOverhead` neutrality holds).

## Risks

- **A new optional dependency.** `ContextRegistry` (io.micrometer:context-propagation) must stay `compileOnly` and load only when present, or the zero-dependency promise breaks. Follow the exact SPI-load pattern the OpenTelemetry/Resilience4j adapters use.
- **Accessor semantics differ from a plain map read.** ThreadLocal accessors capture at subscription; make sure the `deferContextual` seeding point still gives per-subscription isolation (RFC 0005's reason it is not a `with()` builder step). Test the two-subscription race.
- **Doing nothing (option 2 alone) leaves the footgun armed.** It is the honest minimum, but a library that ships `propagate(TRACE)` and means "only if you also hand-wrote the string" is setting a trap the name disarms in the reader's mind. Prefer to actually make it work.
