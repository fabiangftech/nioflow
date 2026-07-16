# RFC 0027 — `Mono.empty()` must mean one thing, not two

- **Status**: ✅ Implemented — option 1 (empty is a failure), centralized in `Blocking`, applied to every value-carrying step on both paths; docs + tests updated
- **Target**: `reactive/` (`Blocking`, `EmptyMonoException`, the `handleMono`/`adaptMono` paths of `DefaultReactive{Flow,Step,Lane}`)
- **Depends on**: RFC 0002 (the blocking bridge), RFC 0004 (streaming terminal semantics)
- **Severity**: **Medium** — a latent `NullPointerException` / silent-null waiting for a `Mono.empty()`, undocumented
- **Sibling of**: RFC 0028 (the other reactive-bridge hole)
- **Realized by**: `EmptyMonoException` (new top-level) + `Blocking.required`/`awaitValue`/`requiredFuture` — the one place empty→error lives. `handleMono`/`adaptMono` (blocking) route through `awaitValue`, their async variants through `requiredFuture`, in all three facades. `adaptFlux` is untouched (it awaits a `collectList`, which emits an empty list, never an empty Mono). Docs: `ReactiveStep.handleMono` javadoc. Tests: `ReactiveEmptyMonoTest`, plus `ReactiveMonoSemanticsTest`/`ReactiveEquivalenceProbeTest` updated from the old null-injecting contract.

## The finding

The facade's whole trick is `Blocking.await` = `mono.block()`, turning a
Mono-returning call into a plain function the chain holds. But `Mono.block()`
**returns `null` on `Mono.empty()`**, and the facade gives that `null` two
different meanings depending on where the step sits:

- **Mid-chain** — `handleMono("lookup", v -> repo.find(v))` returns
  `Mono.empty()` when the lookup misses. `await` returns `null`, which is
  injected as the value into the **next stage**. The next stage receives a
  `null` it never expected → `NullPointerException` downstream, or worse, a
  silent `null` that propagates as if it were a real value.
- **Terminal** — `executeMono()` maps an empty engine result to an **empty
  Mono** (the filtered-cut semantics from RFC 0004): "no value" surfaces to the
  subscriber as completion-without-onNext, which is the *correct* reactive
  meaning of empty.

So the same `Mono.empty()` means "inject null and keep going" in one position
and "the stream produced nothing" in another. The `preferAsync` path has the
same null behaviour (`.toFuture()` of an empty Mono completes with `null`), so
block and async at least agree with *each other* — but neither agrees with the
terminal, and none of it is documented. A developer who writes a cache-miss
`handleMono` returning `Mono.empty()` gets a `null` they never reasoned about.

## Why it blocks production

`Mono.empty()` is not an edge case in reactive code — it is how Reactor idioms
express "not found", "nothing to do", "skip". A repository lookup, a conditional
call, a `filterWhen` that rejects: all produce empty Monos routinely. A facade
that silently converts those into `null` values mid-pipeline is a
`NullPointerException` generator that passes every happy-path test (where the
Mono always emits) and fails the first time production data has a miss. The bug
is in the interaction between a common Reactor idiom and the bridge's core
mechanism, which is the least-tested seam.

## The fix

Decide the contract explicitly and enforce it — the current behaviour is an
accident of `block()`, not a decision. Two coherent options; pick one and
document it on `handleMono`/`adaptMono`:

1. **Empty is illegal on a value-carrying step (recommended, fail-fast).** A
   mid-chain `handleMono`/`adaptMono` must emit exactly one value; an empty Mono
   is a programming error, surfaced as a clear stage failure that `recover()`
   catches — never a silent `null`. Implement in `Blocking.await` by
   distinguishing empty from a genuine value:

   ```java
   static <T> T await(Mono<T> mono) {
       try {
           return mono.switchIfEmpty(Mono.error(EmptyMonoException::new)).block();
       } catch (RuntimeException error) {
           Throwable cause = Exceptions.unwrap(error);
           if (cause instanceof RuntimeException runtime) throw runtime;
           throw new CompletionException(cause);
       }
   }
   ```

   `EmptyMonoException` (a new top-level exception in `reactive/`, per the
   no-nested-types rule) names the step and says "a value-carrying reactive step
   returned Mono.empty(); use filter/recover to model absence." This makes the
   trap a compile-time-obvious runtime error at the exact step, catchable like
   any stage failure — consistent with how the facade already turns Reactor
   failures into ordinary stage failures. Genuinely-nullable values are still
   expressible by mapping to `Optional`/a sentinel *inside* the Mono, which is
   the honest way to carry "maybe absent" through a typed chain.

2. **Empty means filtered (alternative, permissive).** Treat a mid-chain empty
   Mono the same way the terminal does — as a cut — by returning the engine's
   `FlowSignal.FILTERED` sentinel from the bridge so the execution completes with
   the filtered semantics rather than injecting `null`. This unifies empty to a
   single meaning ("no value → the flow stops here") across every position. It
   is more magical (an upstream lookup miss silently ends the pipeline) and
   harder to debug than an explicit failure, so it is the second choice — but it
   is at least *consistent*, which the status quo is not.

Reject the status quo (inject `null`) outright: it is the one option that is
neither safe nor consistent. Whichever is chosen, apply it identically to the
`preferAsync`/`.toFuture()` path so block and async stay in agreement, and state
it in the `handleMono`/`adaptMono` javadoc next to the existing budget note.

## Testing

`ReactiveEmptyMonoTest` (extends the RFC 0020 reactive probe family):

- A mid-chain `handleMono` returning `Mono.empty()` followed by a stage that
  would NPE on null. Option 1: assert it reaches `recover()` as
  `EmptyMonoException` naming the step, and the downstream stage never ran.
  Option 2: assert the execution completes filtered and the downstream never
  ran.
- The `adaptMono` re-typing variant (empty must behave identically).
- Parity: the same chain via `preferAsync` produces the same outcome as the
  blocking path (extend `ReactivePreferAsyncTest`).
- The terminal `executeMono()` empty semantics are **unchanged** — assert an
  empty terminal still surfaces as an empty Mono to the subscriber (this RFC
  only touches mid-chain steps).

## Risks

- **A source-visible behaviour change.** Any existing caller relying on empty →
  null mid-chain breaks — but that reliance is already a latent NPE, so breaking
  it loudly at the right step is the fix, not a regression. Note it in the
  changelog as a semantics correction.
- **Option 1 adds one operator per reactive step.** `switchIfEmpty(Mono.error)`
  is a cheap assembly-time operator on the cold reactive path; the
  `ReactiveBenchmark` gate (async-within-band-of-blocking) confirms the hot path
  is untouched. If it registers at all, gate it.

## Results

Shipped option 1 (empty is a failure, not a filter cut). The change is centralized
despite the three-facade duplication: the empty→error logic lives once in
`Blocking.required`, and the value-carrying steps call one of two thin wrappers —
`awaitValue` (blocking) or `requiredFuture` (async) — so the blocking and
`preferAsync` paths cannot drift, which is the parity the RFC demanded. The error
is deferred (`Mono.error(supplier)`), so a Mono that emits builds no exception.

- **Three existing tests encoded the *old* behaviour and were rewritten, not
  suppressed.** `ReactiveMonoSemanticsTest` had a whole "empty Mono → null the
  next stage must face" section pinning the bug on purpose; those became
  assertions that the empty step fails with `EmptyMonoException`, the next stage
  never runs, and `recover()` catches it. `ReactiveEquivalenceProbeTest`'s
  "empty is Completed(null) through executeResult" became "empty *fails* where a
  filter cut is Filtered" — the two notions of "no value" are now distinct, which
  is the entire point. That these tests existed and asserted the trap is why the
  RFC called it "the least-tested seam": it *was* tested, into the wrong shape.

- **The terminal is genuinely untouched.** An empty *terminal* (`executeMono`
  after a `filter()` cut, or a Mono that legitimately ends empty at the end)
  still surfaces as an empty Mono; only a *value-carrying mid-chain* step fails.
  The filter-cut half of the old conflated test still asserts `verifyComplete()`.

- **`adaptFlux` is correctly exempt.** It awaits `collectList`, which emits an
  empty *list* for an empty Flux — never an empty Mono — so it was left on the
  plain `await` path and its "empty Flux → empty list, not null" test still holds.

The only new SonarLint finding was an S1192 (the `"adaptMono"` step label, now a
literal in three places per file after this and RFC 0028) — extracted to an
`ADAPT_MONO` constant, so the diff over `reactive` is clean. `cd reactive &&
./gradlew test`, `cd core && ./gradlew test`, and the `tests/` suite are all green.
