# RFC 0036 — Examples overhaul: stop shipping the anti-pattern the docs warn against

- **Status**: ✅ Implemented — option 1 (typed beans) + option 2 (operability runbook); options 3 (reusable BlockHound) and 4 (thin-adapter example) deferred as follow-ups
- **Target**: `examples/springboot-with-nioflow`, `docs`
- **Depends on**: — (touches example modules and docs only; no core/reactive change)
- **Severity**: **Medium** — a credibility hole: the flagship example contradicts the central type-safety claim, and the operability story the examples should teach is absent
- **Realized by**: rewriting the primary Spring config to one typed `@Bean` per contract (the wildcard-bean form stays only in `WildcardFlowBeanTest`, labelled as the alternative), and adding a `docs/troubleshooting.md` operability runbook (hung-request playbook, virtual-thread dumps, the guardrails).

## The findings

### 1. The flagship Spring example ships the exact anti-pattern its own Javadoc condemns

`examples/springboot-with-nioflow/.../config/NioFlowConfig.java` declares a single prototype `NioFlow<?, ?>` bean (line 32), while its own class Javadoc (lines 16-20) argues **against** doing so: *"A single `NioFlow<?, ?>` bean injected everywhere would break that promise… Declare one bean per contract instead."* `SampleService` then injects a dozen differently-typed fields (`NioFlow<Integer, String> creditFlow`, `NioFlow<String, String> greetingFlow`, …) that all resolve from that one wildcard bean — the unchecked-cast hole that `DefaultNioFlow.from(Class)` was designed to close. There *is* a `WildcardFlowBeanTest` presenting this as a deliberate alternative, but the contradiction between the prose and the code **in the same file** undercuts the type-safety story sold hard in the docs.

### 2. The operability story the examples should teach is missing

The reactive example's `defaultBudget` is "the only thing between a hung socket and a leaked thread" and is opt-in and forgettable — yet no example *demonstrates* what a hung execution looks like, how to take a virtual-thread thread dump (`jcmd Thread.dump_to_file`, not `jstack`), or how to map a boss task back to a request. An adopter's first production incident is a hung request; the examples give them no map for it.

### 3. The BlockHound boss-non-blocking guarantee lives only in the example's test scope

The mechanical "the boss never blocks" check (RFC 0029) is reconstructed inside the example's suite. A team copying the `handleSync`/handler patterns into their own service inherits the footgun without the guard, because there is no library-provided way to run that check.

## Why it matters

Examples are where adopters learn the intended shape. If the flagship one demonstrates the wildcard-bean form the docs call a mistake, a reader reasonably concludes the type parameters are decorative — eroding trust in the one invariant (`from(Class)`) the library built to protect them. And the two things an operator most needs from an example — how to bound a hung reactive call and how to debug one — are exactly the two the examples omit.

## The options

1. **Primary config: one typed `@Bean` per contract (recommended).** Rewrite `NioFlowConfig` to declare `NioFlow<Integer, String> creditFlow()`, `NioFlow<String, String> greetingFlow()`, etc., each via `DefaultNioFlow.from(...)`, matching the file's own Javadoc. Move the prototype `NioFlow<?, ?>` form to a clearly-labelled `WildcardFlowConfig` (or keep it only as the existing test) documented as "possible, but the types become fiction — prefer typed beans." The main example then *is* the recommended pattern.

2. **Add an operability example + runbook.** A small controller/endpoint that deliberately hangs (a `handleMono` on a Mono that never completes) with and without `defaultBudget`, plus a `docs` runbook: how to take a virtual-thread dump, what a parked `Blocking.await` frame looks like, how boss task names map to requests, and the exact symptom of a leaked worker. This is the incident every adopter will have.

3. **Promote the BlockHound rule to reusable.** Extract the boss-non-blocking BlockHound integration into a small published test artifact (or a documented copy-paste `@BeforeAll` snippet in `docs`), so a consumer can assert "my `handleSync` / my handlers don't block" without reverse-engineering it from the example. Pairs naturally with RFC 0029.

4. **A thin-adapter entry-point example.** Show the common "I have a WebFlux controller and just want to call a nio-flow pipeline" case with `Mono.fromFuture(flow.just(x).executeCancellable().future())`, so adopters who don't need the full reactive mirror see the cheap path (ties to RFC's reactive thin-adapter suggestion).

Recommended: **option 1** (fix the credibility hole — do this first and on its own), then **option 2** (the operability gap), with **3** and **4** as follow-ups.

## Testing

- The rewritten typed config: an integration test that each typed bean resolves and `just()` rejects a wrong-typed input with the clear `IllegalArgumentException` (the `from(Class)` guarantee the wildcard form cannot give).
- Keep `WildcardFlowBeanTest` as the documented "here is the alternative and its limit" case — relabelled, not deleted.
- The operability example ships with a test that asserts a budgeted hung call fails with `TimeoutException` (worker released) and — in a controlled, separate test — that an *un*budgeted one is the leak (documented, not run in CI as a hang).

## Risks

- **Churn in the example most people copy.** Worth it: shipping the anti-pattern is a larger cost than the diff. Keep the wildcard form available (relabelled) so nobody who relied on it is stranded.
- **A published test artifact (option 3) is new surface to maintain.** If that is too much, a documented snippet in `docs` is an acceptable lighter form.
- **Operability docs date faster than code.** Anchor the runbook to observable facts (thread names, frame shapes) and link it from the reactive example so it stays near the thing it describes.

## Results

Shipped option 1 + option 2.

- **The flagship config is now the recommended pattern.** `NioFlowConfig`
  (springboot example) declares one typed `@Bean` per contract —
  `greetingFlow()`/`creditFlow()`/… each `DefaultNioFlow.from(Type.class)` with
  `destroyMethod = "close"` — matching what its own Javadoc always argued for.
  Spring disambiguates the five `NioFlow<String, String>` beans by injection-point
  name (Lombok's `@RequiredArgsConstructor` takes it from the field), so each bean
  method is named after the field it feeds. The wildcard `NioFlow<?, ?>` form
  stays only in `WildcardFlowBeanTest`, framed as the alternative and its limit,
  not the recommendation. The example compiles and its tests (including the
  context-loading ones) pass — the contradiction between the prose and the code is
  gone.

- **An operability runbook.** `docs/troubleshooting.md` (linked in the sidebar
  under Deep dive) is the hung-request playbook: the two thread kinds and what
  "hung" looks like on each, how to take a virtual-thread dump (`jcmd
  Thread.dump_to_file`, since `jstack` misses parked virtual workers), how to read
  the frames (`Blocking.await` = a reactive step waiting on a Mono), a
  symptom→cause→fix table (unbudgeted leak, keyed head-of-line, slow handler on a
  shared boss, drain that won't complete, wrong bound configured), and a
  consolidated view of the guardrails (`defaultBudget`/`requireBudget`, the
  BlockHound gate, the metrics, `dedicated()`). Anchored to observable facts
  (thread names, frame shapes) so it dates slowly.

- **Deferred:** option 3 (promote the BlockHound boss-non-blocking rule to a
  reusable published test artifact) and option 4 (a thin-adapter entry-point
  example, `Mono.fromFuture(flow.just(x).executeCancellable().future())`). Both
  are additive follow-ups; the runbook already tells a consumer to carry the
  BlockHound gate, and the WebFlux page already covers the thin-adapter decision.
