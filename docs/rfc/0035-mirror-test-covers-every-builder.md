# RFC 0035 — `ReactiveMirrorTest` must cover every builder pair, and one behaviour per family

- **Status**: 📋 Proposed
- **Target**: `reactive` (tests: `ReactiveMirrorTest`, `ReactiveParityTest`)
- **Depends on**: RFC 0008 (the module split that made the mirror cross-build), RFC 0030 (behavioural parity — this closes the hole 0030 left in the branch families)
- **Severity**: **Medium** — a coverage hole in the exact test that exists to stop the mirror rotting; a dropped reactive step in a branch family fails no test
- **Realized by**: driving `assertMirrors` over all 12 builder pairs (the `BUILDERS` set already exists), plus a behavioural parity probe position for `when`/`match` on the flow and step — not only lane and fork.

## The finding

`ReactiveMirrorTest` calls `assertMirrors` for exactly three types (`ReactiveMirrorTest.java:29,34,39`): `NioFlow`/`ReactiveFlow`, `NioStep`/`ReactiveStep`, `Lane`/`ReactiveLane`. The **nine branching contracts** — `Condition`/`Branch`/`Cases` and their `Step*` and `Lane*` variants — are checked only *transitively* (that `NioFlow.when` returns a subtype). Nothing verifies that `ReactiveBranch`/`ReactiveCases`/`ReactiveStepCondition`/… covariantly override **every** method of their base.

So the failure mode the test exists to prevent is uncovered in one whole family: add a method to `Cases` (say a second `is(...)` overload), and `DefaultReactiveCases` inherits core's base return type and **silently drops the reactive chain** for anyone who calls it mid-`match()`. The compile still succeeds; the chain just falls back to a `NioStep` and loses `handleMono`. That is precisely the "silent fallback to the base type" hazard `assertMirrors` was written to catch — checked for the flow/step/lane, unchecked for the nine branch builders.

A second, subtler hole: `assertMirrors` is purely *structural* — it checks method *existence and return type*, never *behaviour*. RFC 0030 already recognized this (it added `ReactiveParityTest` for behaviour) but only for a handful of positions. A new `handleMono` overload whose impl forgets `config.budgetFor(...)` — i.e. skips the budget/`requireBudget` enforcement — passes every structural check and every parity probe except the specific ones `ReactiveParityTest` happens to exercise. The mirror's maintenance tax is larger than the current test admits.

## Why it matters

The whole justification for the 30-class covariant mirror (RFC 0008) is that `ReactiveMirrorTest` makes it safe to grow core without silently breaking reactive. That guarantee is only as good as the test's coverage. A hole in the branch families means a core change to `Cases`/`Condition`/`Branch` can ship a reactive facade that compiles, passes CI, and drops reactive steps inside every `when`/`match`/nested branch — found only by a user whose branch mysteriously lost its `handleMono`. Closing the hole is cheap (the machinery exists) and restores the property the module was designed around.

## The options

1. **Loop `assertMirrors` over all 12 builder pairs (recommended, structural).** The set of builder simple-names already exists at `ReactiveMirrorTest.java:123` (`BUILDERS`). Iterate `assertMirrors(base, mirror)` over every `{core builder → reactive mirror}` pair — the three current ones plus the nine branch families. Pure test change; if a mirror is missing an override today, this turns it red immediately (and any red is a real gap to fix).

2. **Add behavioural parity probes for `when`/`match` on flow and step (behavioural).** `ReactiveParityTest` today probes lane/fork positions; extend it so a reactive step declared *inside* a `when(...).then(...)` and a `match().is(...)` on both the flow and the step is exercised end-to-end (its budget applied, its Mono awaited, its result routed) — the positions where a structurally-present-but-behaviourally-wrong override would still misbehave.

3. **A reflective budget-enforcement probe (defence in depth).** For every reactive step method that takes a budget or has a `defaultBudget`-eligible form, assert (reflectively or via a spy config) that the impl routes through `budgetFor`. This catches the "forgot the budget in the fourth copy" class directly rather than hoping a hand-written parity case covers it.

Recommended: **option 1 always** (it is nearly free and closes the structural hole today), plus **option 2** (the behavioural counterpart RFC 0030 started). Consider **option 3** if the mirror keeps growing — it attacks the behavioural-rot root, but is more machinery.

## Testing

- After option 1, `ReactiveMirrorTest` is green only if all 12 mirrors override everything; deliberately deleting one override in a scratch branch must turn it red (verify the test *can* fail).
- After option 2, deleting the budget wiring from a branch-scoped `handleMono` must fail a parity probe, not just a structural one.
- These are test-only changes; no `core`/`reactive` main source moves, so no hot-path gate is touched.

## Risks

- **Option 1 may surface a real, pre-existing gap** (a branch mirror already missing an override). That is the point — it is a latent bug, and a red test is how you find it before a user does. Fix the mirror, then the test stays a guard.
- **Behavioural probes are hand-maintained** and can themselves drift (the RFC 0030 concern recurses). Option 3 mitigates this but at complexity cost; weigh it against how fast the mirror is actually growing.
- **Reflection over generics erasure** (option 3) is fiddly; scope it to the budget-routing check where the payoff is concrete, not a general "impl does the right thing" oracle.
