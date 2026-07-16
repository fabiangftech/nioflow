# RFC 0030 — Guard the reactive mirror's *behaviour*, not just its *existence*

- **Status**: 🔵 Proposed
- **Target**: `reactive/` (the Flow/Step/Lane duplication; `ReactiveMirrorTest`'s scope)
- **Depends on**: RFC 0008 (the reactive module split and the mirror contract)
- **Severity**: **Medium** — a structural source of "fixed in two of three copies" latent bugs; the maintenance tax that makes every future reactive fix risky
- **Sibling of**: RFC 0028 (a concrete instance — the budget threading it must be applied to in three places)

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
