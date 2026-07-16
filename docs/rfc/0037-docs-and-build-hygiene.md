# RFC 0037 — Docs & build hygiene: one benchmark source, reachable RFCs, a pinned JDK floor

- **Status**: ✅ Implemented — all five findings
- **Target**: `docs` (`_sidebar.md`, `_navbar.md`, `rfc/0000-index.md`, `webflux.md`, new `troubleshooting.md`, new `CHANGELOG.md`, README comparison), `core/build.gradle`, `reactive/build.gradle`, `CLAUDE.md`, two `.java` files (the Java-21 fix)
- **Depends on**: RFC 0018 (the docs refresh), RFC 0022 (the benchmarks page)
- **Severity**: **Medium-Low** — no runtime effect, but a cluster of drift and gaps that undercuts trust and blocks adopters
- **Realized by**: declaring `benchmarks.md` the single source and reconciling the loose figures in the index/CLAUDE.md/webflux.md; correcting the index's "cost the same" framing; linking the RFC index in the sidebar and navbar; pinning `options.release = 21` (which surfaced — and got fixed — a real Java-22 dependency); and adding the adopter docs (CHANGELOG, a when-NOT-to-use comparison; tuning is covered by the RFC 0036 `troubleshooting.md`).

## The findings

### 1. Benchmark numbers have drifted across three sources of truth

The regression gates are ratio/allocation-based, so no file pins the absolutes — and they disagree:

- Plain-chain allocation: `benchmarks.md:57` says **616 B/op**; `CLAUDE.md` (per-call discipline) says **~727 B/op**.
- Async throughput vs blocking: `benchmarks.md:47` = **74.9 vs 72.4**; `rfc/0000-index.md:149` = **74.4 vs 72.4**; `CLAUDE.md` (reactive section) = **57.1 vs 56.0**. Three different pairs for "the same" claim.

### 2. The RFC index contradicts its own evidence page

`rfc/0000-index.md:130-131` opens the throughput argument with "a 1-stage and a 32-stage chain **cost the same (~17.5 µs)**." The evidence page it points to (`benchmarks.md:44`) measures **88.6 vs 58.8 ops/ms** and reframes it correctly as "**~1.5× slower, not 32×**." The index carries the stale, overstated version of a claim the evidence page already corrected.

### 3. The 31 RFCs are unreachable from the docs site

`docs/_sidebar.md` and `docs/_navbar.md` contain **zero** link to `rfc/0000-index.md`. The design record — the strongest asset the project has — is discoverable only via hard-coded GitHub deep-links buried inside four docs. A reader on the site cannot find it.

### 4. "Java 21+" is promised but not enforced

`README.md:105`, `quickstart.md:5` and the badge say Java 21+. `core/build.gradle` and `reactive/build.gradle` have **no `toolchain`, no `options.release`, no `sourceCompatibility`** — they compile with whatever JDK runs Gradle (dev is on 25). Nothing stops a JDK-25-only API from silently landing and breaking the promised floor; a contributor on 21 gets no guardrail.

### 5. Adopter-facing docs are missing

No `CHANGELOG` / `MIGRATION` anywhere (despite the 2.0.0 reactive-module split, RFC 0008, being a real reorganization); no consolidated tuning reference (`-Dnioflow.bosses`, `dedicated()`, `OverflowPolicy`, batch windows, budgets are scattered); no "when NOT to use / vs `CompletableFuture` / `StructuredTaskScope` / Reactor" comparison for the non-reactive audience (the only "reach for plain X" line is one bullet at the bottom of `webflux.md`).

## Why it matters

Each is small; together they read as a project whose claims you have to cross-check. Drifting benchmark numbers make a *skeptical* reader (the one who matters) distrust all of them; an unreachable design record wastes the project's best evidence; an unenforced JDK floor is a latent "works on my machine" break; and the missing comparison/changelog/tuning docs are exactly what a production adopter needs to say yes. Fixing them is nearly all doc/build edits — high trust-per-line.

## The options

Mostly independent; do all, in this order of value:

1. **Single benchmark source of truth (recommended first).** Declare `benchmarks.md` (dated, machine-stamped) authoritative. Reconcile or delete the loose figures in `CLAUDE.md` and align `rfc/0000-index.md`'s async pair. Where CLAUDE.md wants illustrative numbers, quote `benchmarks.md`'s with its machine caveat rather than a separate figure.

2. **Fix the index's stale "cost the same" framing** to match `benchmarks.md` ("~1.5× slower over 32× more links"). One-line edit, removes a self-contradiction.

3. **Make the RFCs reachable.** Add a "Design record (RFCs)" entry to `_sidebar.md` (pointing at `rfc/0000-index.md`) and to `_navbar.md`.

4. **Pin the JDK floor.** Add `java { toolchain { languageVersion = JavaLanguageVersion.of(21) } }` (or `options.release = 21`) to both `core` and `reactive` builds so the documented floor is mechanical. (If the code already uses a 22+ API, either raise the documented floor to match or gate the API — the point is that docs and build agree.)

5. **Add the adopter docs.** A `CHANGELOG.md`; a one-page "Tuning & failure modes" reference consolidating the knobs and the failure catalogue (boss rejection, hung socket, batch bulk failure) that currently lives only in RFC prose; and a short "nioflow vs CompletableFuture / structured concurrency / workflow engine — and when NOT to use nioflow" table, promoted to `README.md` and the sidebar.

## Testing

- Doc changes need no unit test, but: verify the `release`/toolchain pin actually fails a build that uses a newer-than-floor API (a scratch commit using such an API must not compile) — otherwise the pin is decorative.
- If the pin reveals the code already needs > 21, that is a finding: reconcile the documented floor to reality in the same PR.
- Link-check the new nav entries resolve on the built site.

## Risks

- **Pinning the toolchain may break the build** if a JDK-25 API already slipped in (this RFC would then have *found* a real floor violation). That is the guardrail working; fix it by gating the API or honestly raising the floor.
- **Reconciling numbers invites re-running benchmarks** on a non-reference machine and re-drifting. Anchor to `benchmarks.md`'s stated machine and date; do not add fresh absolutes elsewhere.
- **A comparison table can read as defensive.** Keep it factual and lead with "use plain X when …" — the honesty is the selling point (it matches `benchmarks.md`'s "how to read these honestly" tone).

## Results

Shipped all five findings.

- **One benchmark source.** The index's async pair (`74.4 → 74.9`), `webflux.md`'s
  (`74.4 → 74.9`) and `CLAUDE.md`'s plain-chain allocation (`727 → 616 B/op`) are
  reconciled to `docs/benchmarks.md`, which each now cites as the single source;
  `CLAUDE.md`'s loose `57.1 vs 56.0` reactive pair became the machine-independent
  "at parity" claim. Historical RFC results tables (e.g. RFC 0013's `74.4`) are
  left as the record of what that RFC measured.

- **The index no longer contradicts its evidence.** "a 1-stage and a 32-stage
  chain cost the same (~17.5 µs)" became "nearly the same — about 1.5× apart over
  32× more links (88.6 vs 58.8 ops/ms; see benchmarks)", matching the page it
  points to.

- **The RFCs are reachable.** `_sidebar.md` gained a "Design record → RFC index"
  section and `_navbar.md` an "RFCs" link, both to `rfc/0000-index.md`.

- **The Java 21 floor is enforced — and the pin found a real violation.** Adding
  `options.release = 21` to both builds failed to compile: the code used Java-**22**
  unnamed variables (`case Type _`) in `ChainValidator` and `DefaultNioEngine`, so
  the advertised "Java 21+" was **false**. Rather than raise the floor off the LTS,
  the ten `_` patterns were named (`ignored`), and the build now compiles and tests
  green at `--release 21` — the promise is mechanical and true. No separate JDK 21
  install needed (`options.release`, not a toolchain).

- **Adopter docs.** A root `CHANGELOG.md` (Keep-a-Changelog style) records the
  second-audit cluster, leading with the RFC 0034 breaking change and its
  migration. `docs/README.md` gained a "When NOT to use it — and what to reach for
  instead" table (`StructuredTaskScope` / virtual threads / plain Reactor / a
  workflow engine), the value proposition the adopter review found missing. Tuning
  and failure modes are the RFC 0036 `troubleshooting.md`, linked from both.

- **Not separately done:** a standalone tuning page — `troubleshooting.md` (RFC
  0036) already consolidates the knobs (`-Dnioflow.bosses`, `dedicated()`,
  `OverflowPolicy`, `keyLaneCapacity`, budgets) and the failure catalogue, so a
  second page would duplicate it; the changelog and README link to it instead.
