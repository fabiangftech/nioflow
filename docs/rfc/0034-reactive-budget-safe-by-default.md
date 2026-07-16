# RFC 0034 — The reactive budget footgun ships armed: make safety the default

- **Status**: 📋 Proposed
- **Target**: `reactive` (`ReactiveConfig`, `ReactiveFlow` sealing, docs)
- **Depends on**: RFC 0003 (the thread-leak knobs), RFC 0006 (async stage), RFC 0028 (the preferAsync leak — the sibling this generalizes)
- **Severity**: **High** — the out-of-the-box behavior of the headline API is the documented forever-leak: a hung socket parks a worker (and pins an `Execution`, ~3.6 KB) for the life of the JVM
- **Realized by**: making `requireBudget` default-*on* (opt out for `Mono.just`/cache chains), or — at minimum — emitting a one-time startup warning when a flow seals with reactive network-shaped steps and no budget anywhere.

## The finding

`ReactiveConfig.NONE` has `requireBudget=false` (`ReactiveConfig.java:22`), and `budgetFor` only throws when `requireBudget` is on (`ReactiveConfig.java:67`). So the default behavior of the headline API —

```java
Reactive.flow(DefaultNioFlow.from(Order.class))
        .handleMono("call", client::get)   // no defaultBudget, no per-step budget
```

— is exactly the hazard the module's own comments describe as its primary danger (`Blocking.java:19-24`, `ReactiveConfig.java:59-72`): `handleMono` parks a virtual worker on `mono.block()`, and with no budget on the Mono there is **no cancellation path** — a hung upstream parks that worker, and pins its `Execution` (~3.6 KB), **for the life of the JVM**.

The safe configuration exists (`defaultBudget(d)` puts `mono.timeout(d)` on every step; `requireBudget()` turns "no budget" into a build-time error). But both are opt-in, and the two calls are separate. The library ships with its footgun armed and asks the user to remember to disarm it.

This is the *same* leak RFC 0028 closed for the `preferAsync` path — there the fix made the no-budget async case not leak a connection. This RFC generalizes the lesson to the default blocking path: an unbounded reactive stage should be a deliberate choice, not the path of least resistance.

## Why it blocks production

The first hung upstream in production — a socket that never returns, a downstream deadlock — is not an *if*. When it happens, an unbudgeted flow does not fail the request (which recover/timeout would catch and surface); it silently retires a worker and its `Execution` and never gets them back. Repeated, this is a slow leak that looks like a gradual memory climb with no error trail — the hardest failure to attribute, because the code path that leaked looks identical to the one that succeeded. Making the dangerous configuration the default means every team that doesn't read `Blocking.java`'s Javadoc ships it.

## The options

1. **Default `requireBudget` on; opt out explicitly (recommended).** Flip `ReactiveConfig.NONE` to `requireBudget=true`, so a reactive step with no budget (step-level or `defaultBudget`) is a **build-time** `IllegalStateException` at seal — the failure moves from a silent runtime leak to a loud, local build error with the step name. Add `allowUnbudgeted()` (or `defaultBudget(NONE)`) for the legitimate `Mono.just`/cache-lookup chains that genuinely need none. Safety becomes the default; "no budget" becomes a stated decision.

2. **Startup/seal warning instead of an error (softer).** Keep the default permissive but log a one-time WARN when a flow seals with reactive steps and no budget anywhere ("flow X has N reactive steps with no budget; a hung upstream will leak a worker — set defaultBudget or requireBudget"). Non-breaking, but a log line is easy to miss and does not stop the ship.

3. **A sensible default budget.** Give `defaultBudget` a non-null default (e.g. 30s). Simplest for users, but a wrong default is its own trap (too short breaks slow-but-valid calls; too long barely helps), and a silent timeout is arguably worse than a required decision. Least preferred.

Recommended: **option 1** — a required decision is the honest design; the `Mono.just` case that needs no budget is exactly the case where writing `allowUnbudgeted()` costs nothing and documents intent. Ship **option 2's warning** as the transitional aid for existing code (one release of WARN before flipping the default, so nobody's build breaks by surprise).

## Testing

- With the new default, a flow with a `handleMono` and no budget **fails to seal** with a message naming the step; adding `defaultBudget(d)` or `allowUnbudgeted()` makes it seal. (Mirror of the existing `requireBudget` test, now the default.)
- `Mono.just` chain with `allowUnbudgeted()` seals and runs, parks nothing meaningfully (it completes immediately), retains no worker.
- Regression: RFC 0028's preferAsync-no-budget path still cannot leak; the two safeties compose.

## Risks

- **Flipping the default is a breaking change** for any existing flow that relied on unbudgeted steps. Mitigation: ship option 2's WARN for one release first, document in the changelog (RFC 0037) and migration note, and make `allowUnbudgeted()` a one-line escape. Since the current default is the *leak*, this break is a fix.
- **Users will reflexively `allowUnbudgeted()` to make the build pass.** A required decision only helps if the message explains the stakes; make the exception text say *why* (worker leak on a hung upstream), not just *what*.
- **`Mono.just`/cache chains are common and legitimately budgetless.** The opt-out must be ergonomic and obviously correct for them, or option 1 becomes friction people route around blindly.
