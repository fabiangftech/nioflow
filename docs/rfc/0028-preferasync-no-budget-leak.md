# RFC 0028 — Close the `preferAsync` no-budget leak (and tell the truth about it)

- **Status**: ✅ Implemented — `requireBudget()` opt-in enforced centrally across all four entry points; the lane path unified onto `ReactiveConfig`; docs corrected; 9 tests
- **Target**: `reactive/` (`ReactiveConfig.budgetFor`, `ReactiveFlow.requireBudget`, the `DefaultReactive{Flow,Step,Lane}` reactive-step paths, the lane mirrors, `defaultBudget` docs)
- **Depends on**: RFC 0003 (`defaultBudget`, the safety property this closes a hole in), RFC 0006 (the async stage the leak lives on)
- **Severity**: **Medium-High** — a permanent resource leak on the exact escape hatch (`preferAsync`) sold to *avoid* the leak it warns about
- **Sibling of**: RFC 0027 (the other reactive-bridge hole)
- **Realized by**: `ReactiveConfig.budgetFor(step, stepBudget)` — the single place a reactive step resolves its effective budget and, when `requireBudget()` is on and none exists, rejects it at build time; `ReactiveFlow.requireBudget()` + `ReactiveConfig.withRequireBudget()`; every reactive step in `DefaultReactiveFlow`/`Step`/`Lane` routed through `budgetFor`. The lane path (`DefaultReactiveLane` + its condition/cases/branch mirrors, `Lanes.budgeted`) was unified from loose `(Duration budget, boolean preferAsync)` fields onto the `ReactiveConfig` record, which is what carries `requireBudget` into a branch and a fork. Tests: `ReactivePreferAsyncBudgetTest`. Docs: `ReactiveFlow.defaultBudget`/`requireBudget` javadoc.

## The finding

`defaultBudget` exists because a Mono that never completes parks its virtual
worker for the life of the JVM — the engine has no cancellation to take a parked
worker back. Its javadoc frames the hazard entirely in those terms: *"parks its
worker FOREVER on a Mono that never completes."*

`preferAsync` is the recommended escape from that parked-worker cost: it routes
reactive steps through `handleMonoAsync` (core's `AsyncStage`, a future rather
than a parked worker, ~489 B vs 3 173 B). The safety story is assumed to carry
over. **It does not, when no budget is declared.**

`handleMonoAsync(name, call, budget)` with a `null` budget delegates to
`handleAsync(name, fn, null)`, and core treats a null timeout as "no timeout":
the `AsyncStage` is armed with no `TimerWheel` budget, so nothing ever cancels a
hung call. The Mono, held as a `CompletableFuture` that never completes, pins:

- the **`Execution`** (retained for the life of the JVM — it never reaches a
  terminal), and
- the **connection** behind the call (nothing disposes the subscription).

There is no parked thread this time, so it is *quieter* than the blocking leak —
but it is equally permanent, and it leaks a socket the blocking form does not.
A developer who switched to `preferAsync` specifically to escape the
`defaultBudget` worry still has an unbounded leak, and the one warning in the
docs points only at the failure mode they just left.

Confirmed path: `DefaultReactiveStep.handleMonoAsync(name, call, budget)` →
`delegate.handleAsync(name, v -> call.apply(v).toFuture(), budget)`; core's
`AbstractChain` stores a null timeout as no-timeout on the `AsyncStage`.

## Why it blocks production

A leak that grows one `Execution` + one connection per hung upstream call,
without bound and without a parked thread to make it visible, is a slow-motion
outage: heap creeps, the connection pool exhausts, and the symptom (OOM or
"no available connections") appears far from the cause. It triggers under
exactly the conditions production hits and tests don't — an upstream that
accepts the connection and then never responds. And it lands on `preferAsync`,
the path the docs actively steer high-volume deployments toward, so the most
scale-conscious users are the most exposed.

## The fix

Two parts — one mechanical, one honest.

**1. Give the async path a default the blocking path already has.** The
blocking bridge routes every Mono through `Blocking.budgeted(mono, budget)`; the
async path must apply the *same* `config.budget()` fallback so a flow that
declared a `defaultBudget` cannot leak on either path. It already threads
`config.budget()` in the no-arg overloads — the gap is that a **null** budget
(no default declared *and* none on the step) silently means "no timeout" on the
async side, where on the blocking side it means "park, but a declared default
would have caught you." Make the async path honor the default identically:
`handleMonoAsync(name, call)` → `handleMonoAsync(name, call, config.budget())`
is already there; the fix is ensuring the *flow-level default* actually reaches
`handleAsync` as a timeout, and that this threading is not re-derived
incorrectly in the three near-duplicate copies (Flow/Step/Lane — see RFC 0030 on
the mirror duplication).

**2. Decide what a truly-absent budget means, and enforce it.** When *neither*
a step budget *nor* a `defaultBudget` is declared, the async path currently arms
no timeout — an unbounded wait. Options:

- **Recommended: make "network-facing async step with no budget anywhere" a
  build-time warning or an opt-in.** A reactive step whose call is a remote one
  and that resolves to a null budget is almost always a mistake. Since the
  facade cannot know "remote" from "`Mono.just`", surface it as a documented,
  opt-out `requireBudget()` mode on the flow (default off to preserve
  `Mono.just` ergonomics; on, it rejects a null-budget reactive step at build
  time where the caller's line number still exists — same stance
  `checkMaxItems` and `propagate()` already take).
- **At minimum**: a `defaultBudget` set on the flow MUST propagate to the async
  path, so declaring it once closes *both* leaks. This is the load-bearing half
  and is likely a small change (thread the default through, don't drop it to
  null).

**3. Fix the docs.** Rewrite `defaultBudget`'s javadoc so the hazard is stated
for **both** paths: the blocking path parks a worker forever; the async path
pins an `Execution` and a connection forever. Same root cause (a Mono that never
completes, and the engine has no cancellation to reclaim it), two footprints,
one cure (declare a budget). The current wording that names only the parked
worker is the thing that makes users think `preferAsync` is safe when it isn't.

## Testing

`ReactivePreferAsyncBudgetTest`:

- A `preferAsync` flow with a `defaultBudget`, a `handleMonoAsync` whose Mono
  never completes. Assert the execution fails with `TimeoutException` (the
  default reached the async path and cancelled the call) and the subscription
  was disposed (a `doOnCancel` latch fires) — proving no `Execution` and no
  connection leak.
- The same flow with **no** budget anywhere and `requireBudget()` on: assert the
  build **rejects** the network-facing step (an `IllegalStateException` at
  assembly, naming the step).
- Parity: `Mono.just` (a resolved Mono) needs no budget and still works with
  `requireBudget()` off — the ergonomic default is preserved.
- Regression: the blocking path's existing `defaultBudget` behaviour
  (`ReactiveDefaultBudgetTest`) is unchanged.

## Risks

- **`requireBudget()` friction.** Making it opt-in (default off) keeps
  `Mono.just` chains frictionless while giving network-facing flows a way to
  make the safe thing mandatory. Teams that want the guarantee turn it on; nobody
  is forced.
- **Threading the default through three copies.** The Flow/Step/Lane duplication
  means the fix must be applied identically in each; RFC 0030 proposes removing
  that duplication so this class of "fixed in two of three places" bug stops
  recurring. Until then, the test above must exercise all three entry points
  (main line, `just()` pipeline, inside a lane/fork).

## Results

Implementing the draft sharpened it in one important way, and did one piece of
RFC 0030's work as a prerequisite.

- **Part 1 ("thread the default to the async path") was already true.** Tracing
  the code showed `config.budget()` already reaches `handleMonoAsync` on every
  path — the async leak is *only* the case where no budget is declared anywhere,
  so the resolved budget is null and core reads that as "no timeout". So the
  load-bearing half ("declare `defaultBudget` once → both paths safe") needed no
  change; it needed a **test** (`preferAsyncWithADefaultBudgetCancelsAHungAsyncCallInsteadOfLeaking`)
  and the **doc correction** naming the async footprint, both of which shipped.
  The real new feature is the truly-absent-budget guard.

- **The guard is centralized, not copied.** `requireBudget()` sets one flag on
  `ReactiveConfig`, and every reactive step resolves its budget through the
  single `ReactiveConfig.budgetFor(step, stepBudget)` — which throws at build time
  when the effective budget is null and the flag is on. There is exactly one
  enforcement point, so the "fixed in two of three copies" risk the draft worried
  about does not apply to *this* logic: Flow, Step and Lane all call the same
  method. Verified in all four positions — main line, `just()` pipeline, a branch
  lane, a fork body — each rejected at assembly with the step named.

- **The lane path was unified onto `ReactiveConfig` (a down payment on RFC 0030).**
  The lane mirrors carried the budget as loose `(Duration budget, boolean
  preferAsync)` fields threaded by hand through `DefaultReactiveLane`, its
  condition/cases/branch subclasses, `Lanes.budgeted` and `Reactive.lane`. Adding
  `requireBudget` as a *third* loose field would have tripled exactly the
  duplication RFC 0030 exists to remove. Instead the pair was replaced with the
  `ReactiveConfig` record the flow/step side already carries, so `requireBudget`
  (and any future config knob) reaches a branch and a fork **correct by
  construction**, and the lane path stopped re-deriving what the flow already
  knew. The refactor added **zero** new SonarLint findings and left the whole
  reactive suite (including `ReactiveMirrorTest`) green.

`cd reactive && ./gradlew test` green; SonarLint diff over `reactive` is empty
(the refactor added nothing; the new test is clean); the Spring WebFlux example
still compiles against the changed facade (the only API change is the additive
`requireBudget()` on `ReactiveFlow`).
