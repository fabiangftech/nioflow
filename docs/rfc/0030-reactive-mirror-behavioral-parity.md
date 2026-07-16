# RFC 0030 — Guard the reactive mirror's *behaviour*, not just its *existence*

- **Status**: ✅ Implemented — behavioural parity harness across all four positions; layer 1 dedup already done by RFC 0027/0028
- **Target**: `reactive/` (a new behavioural parity test; the Flow/Step/Lane logic already single-sourced)
- **Depends on**: RFC 0008 (the reactive module split and the mirror contract)
- **Severity**: **Medium** — a structural source of "fixed in two of three copies" latent bugs; the maintenance tax that makes every future reactive fix risky
- **Sibling of**: RFC 0028 (a concrete instance — the budget threading it must be applied to in three places)
- **Realized by**: `ReactiveParityTest` — runs the same `handleMono` probe through the main line, a `just()` pipeline, a `when()` lane and a `fork()` body, and asserts identical outcomes for the load-bearing properties (a `defaultBudget` bounds a hung Mono; an empty Mono fails with `EmptyMonoException`), on both the block and the `preferAsync` path. Layer 1 (single-source the logic) was already delivered by RFC 0027 (`Blocking.required`/`awaitValue`/`requiredFuture`) and RFC 0028 (`ReactiveConfig.budgetFor`, plus unifying the lane onto `ReactiveConfig`).

## The finding

The reactive module is a subinterface mirror: every core step is re-declared and
re-implemented across up to seven files — `ReactiveFlow`/`ReactiveStep`/
`ReactiveLane` interfaces, their `Default*` implementations, and the branch/
condition/cases mirrors. `ReactiveMirrorTest` guards this by reflection: it fails
if a core method has no covariant override on its mirror, and if a reactive-only
step of `ReactiveStep` is missing from `ReactiveLane`.

That test guards **existence**, not **behaviour**. A copy that compiles and has
the right signature passes — even if its body is wrong. And the bodies are where
the risk lives, because the same non-trivial logic is hand-replicated in each
copy:

- The `preferAsync` branch (route to `handleMonoAsync` vs `Blocking.await`) —
  replicated in `DefaultReactiveStep`, `DefaultReactiveFlow`, `DefaultReactiveLane`.
- The `config.budget()` default threading and the `Blocking.budgeted` wrap —
  same three places, plus the `Lanes.budgeted` lane wrap.
- The three near-identical `handleMono(..., budget, retry)` blocks.

A copy that forgets the `config.preferAsync()` check, or drops `config.budget()`
to null (RFC 0028's leak is *exactly* this shape), or omits the `Lanes.budgeted`
wrap in a lane, **compiles and passes `ReactiveMirrorTest`**. Only a hand-written
integration test that happens to exercise that specific entry point catches the
divergence. With ~2 658 lines of mirror carrying zero engine behaviour, the
surface for a two-of-three-copies bug is large and permanent — and it is the
precise mechanism by which a *fixed* bug reappears in the copy nobody re-tested.

## Why it blocks production

Production hardening (RFCs 0027, 0028) means changing the reactive bridge's
behaviour, and every such change must be made identically in three-to-seven
places by hand today. That is how a fix ships to the main line and silently
misses the same step inside a `when` branch or a `fork` — the pipeline shape a
production flow actually uses. The duplication is not a style nit; it is an
active correctness hazard for exactly the changes this hardening series
introduces.

## The fix

Two layers — reduce the duplication, then guard the behaviour that remains.

**1. Collapse the replicated logic to one place.** The Flow/Step/Lane copies of
`handleMono`/`adaptMono`/`fanOutMono` differ only in the delegate they wrap and
the return type. Factor the decision logic (preferAsync? budget default? budgeted
wrap?) into a single shared helper the three facades call, so there is **one**
implementation of "how a reactive step becomes a core step" and the facades only
differ in `wrap`/`retyped`/lane-scoping. Options:

- A package-private `ReactiveSteps` helper holding the static `budgeted` /
  `preferAsync` decision, taking the core delegate and returning the built step;
  each facade method becomes a one-liner over it. This is the smallest change and
  removes the three-way copy of the load-bearing branch.
- Where the facades share enough shape, an abstract base that implements the
  reactive-step methods over an abstract `delegate()` — but the covariant return
  types (`ReactiveStep` vs `ReactiveFlow` vs `ReactiveLane`) limit how much a base
  can absorb; the helper is likelier to fit cleanly without fighting erasure.

**2. Extend `ReactiveMirrorTest` to assert parity, not presence.** Add a
behavioural pass that runs the *same* small pipeline through each entry point —
main line, `just()` pipeline, inside a `when` lane, inside a `fork` — and asserts
identical outcomes for the properties that are hand-replicated:

- `preferAsync` on: a reactive step routes through the async (future) path in
  every position, not just the main line (thread/allocation assertion or a
  `handleMonoAsync`-was-used probe).
- `defaultBudget` set: a hung Mono times out in every position (guards RFC 0028's
  leak across all four entry points at once).
- Empty-Mono semantics (RFC 0027) are identical in every position.

This is the RFC 0020 "two paths must agree" oracle applied to the four ways a
reactive step can be built: it needs no hand-computed expected value, and it
fails the instant one copy diverges — which is the guarantee the existence check
cannot give.

## Testing

- The parity harness above *is* the test: a table of (entry point) × (property)
  asserting agreement. It replaces the implicit hope that three copies stay in
  sync with a mechanical proof that they do.
- Regression: the existing covariance/existence checks stay — this adds to
  `ReactiveMirrorTest`, it does not replace it.
- After the helper refactor, the existing reactive suite
  (`ReactivePreferAsyncTest`, `ReactiveDefaultBudgetTest`, delegation tests) must
  stay green unchanged — the refactor is behaviour-preserving by construction.

## Risks

- **A refactor of load-bearing wiring.** The helper extraction touches every
  reactive step method; the safety net is that the full reactive suite plus the
  new parity harness must stay green, and the covariant-return constraint means
  the compiler catches most mistakes. Do it as a pure refactor commit,
  behaviour-preserving, *before* landing 0027/0028 on top — so those fixes are
  written once, not three times.
- **The parity harness adds runtime.** It runs a handful of tiny pipelines four
  ways; negligible against the existing reactive suite, and it is the cheapest
  insurance against the class of bug that made this RFC necessary.
- **Not everything can be deduplicated.** The covariant return types are
  irreducible (that is the mirror's whole design). The goal is to remove the
  *logic* duplication, not the *signature* duplication — the signatures stay,
  guarded by the existence check; the bodies converge, guarded by parity.

## Results

The two layers landed in the reverse of the drafted order, and it worked out
better that way.

- **Layer 1 (single-source the logic) was already done — by 0027 and 0028, not
  here.** The draft assumed this RFC would land *before* the hardening fixes so
  they were written once. In practice 0027 and 0028 shipped first and each
  centralized its own logic as it went: RFC 0027 put the empty→error decision in
  `Blocking.required`/`awaitValue`/`requiredFuture` (one place, called from all
  three facades), and RFC 0028 put the budget resolution in
  `ReactiveConfig.budgetFor` (one place) *and* replaced the lane's loose
  `(budget, preferAsync)` fields with the `ReactiveConfig` record the flow/step
  side already carried. So the load-bearing branch (`preferAsync`? budget
  default? empty guard?) is no longer copied — only the thin `if
  (config.preferAsync())` shell and the `delegate.handle`/`handleAsync` call
  differ per facade, and those differ *because the return types do*, which is the
  irreducible part. There was no risky refactor left to do.

- **Layer 2 (the parity harness) is the deliverable, and it is not vacuous.**
  `ReactiveParityTest` runs one probe through all four positions and asserts the
  budget and empty-Mono properties agree. Temporarily reverting the lane's
  `handleMono` to the pre-0027 `await(budgeted(...))` (dropping the empty guard)
  makes the harness go red at the lane and fork positions while the main line and
  pipeline stay green — precisely the "fixed in two of three copies" signature.
  Restored, all four agree. That is the mechanical proof the existence check
  could never give: a divergence in any one copy fails the build.

- **The check is future-proof, not just a snapshot.** It asserts *properties*
  (times out; fails on empty), so a *new* reactive step or a *new* hardening fix
  is covered the moment it is added to the probe — and any copy that forgets it
  diverges loudly. Combined with `ReactiveMirrorTest` (existence + covariance),
  the mirror is now guarded on both axes: every method exists everywhere, and the
  ones that carry logic behave identically everywhere.

`cd reactive && ./gradlew test` green; SonarLint diff over `reactive` is empty
(the RFC added only a test).
