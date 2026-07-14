# RFC 0008 — `nioflow-reactive`: the reactive facade as its own artifact

- **Status**: Proposed
- **Target**: a new `reactive/` Gradle build (`dev.nioflow:nioflow-reactive`), `core/` (deletion + build), `tests/`, `examples/springwebflux-with-nioflow`, CI
- **Depends on**: RFC 0002 (the mirror design), and on 0005/0006/0007 having already pushed every core terminal the facade needs (`executeAsync(Map)`, `AsyncStage`, `Cancellable`) into reactor-free core API
- **Independent of**: every proposed RFC — this one moves files, it does not change behavior
- **Breaks**: the coordinate a WebFlux consumer declares. Nothing else.

## Summary

`infrastructure.reactive` is **29 of core's 86 main classes and 2 361 of its 8 231 main lines** (29 %), plus **11 test classes and 2 634 of the 8 866 test lines** (30 %). Roughly a third of the library — build time, test time, Sonar surface, review surface, `compileOnly` dependency, and the whole `CoreWithoutReactorTest` apparatus that exists to prove the other two thirds do not need it — serves consumers who bring Reactor. The ones who do not still download it in the jar.

Move it to its own Gradle build, publish it as `dev.nioflow:nioflow-reactive`, and let it depend on `nioflow-core` like any other consumer.

```gradle
// a WebFlux app, after
implementation 'dev.nioflow:nioflow-reactive:2.0.0'   // brings nioflow-core transitively

// a plain app, after
implementation 'dev.nioflow:nioflow-core:2.0.0'       // and there is no Reactor code in it at all
```

**The extraction is possible because the facade never earned a privilege.** Every `dev.nioflow` import in those 29 classes resolves to `core.facade` (the public contracts) or `core.model` (`Retry`, `RateLimit`) — **not one class from `application.*`, not one package-private member, not one widened method**:

```
$ grep -rhoE 'import dev\.nioflow\.[a-zA-Z0-9_.]+;' infrastructure/reactive | sort -u
dev.nioflow.core.facade.{Branch,Cancellable,Cases,Condition,Context,FlowResult,Lane,LaneBranch,
                         LaneCases,LaneCondition,NioFlow,NioStep,Segment,StepBranch,StepCases,StepCondition}
dev.nioflow.core.model.{RateLimit,Retry}
```

RFC 0002 called the reactive layer "a subinterface mirror, not a wrapper API", and `Reactive.flow` is one `new DefaultReactiveFlow<>(flow)` over a `NioFlow` interface. That design decision — taken for type-safety reasons, four RFCs ago — is what makes today's move a `git mv` and a build file. **This RFC adds no SPI to core and widens no visibility.** If it had needed to, that would have been the discovery that the facade was reaching into internals; it isn't.

## What moves

| | Where it lands | Why |
| --- | --- | --- |
| the 29 classes of `dev.nioflow.infrastructure.reactive` | `reactive/src/main/java/…` — **same package, byte-identical content** | see *The package name stays* |
| 10 of the 11 reactive test classes (`ReactiveFlowTest`, `ReactiveMirrorTest`, `ReactiveStreamingTest`, `ReactivePipeTest`, `ReactiveContextTest`, `ReactiveCancellationTest`, `ReactiveAsyncStageTest`, `ReactiveDefaultBudgetTest`, `ReactiveMonoSemanticsTest`, `ReactiveDelegationTest`) | `reactive/src/test/java/…` | they already import nothing but public API (`DefaultNioEngine`, `DefaultNioFlow`, `NioFlowMetrics`, `Segment`, …) and no core test base class — `EngineTestSupport` is not among their imports |
| `CoreWithoutReactorTest` | **deleted** | see below |
| `compileOnly 'io.projectreactor:reactor-core'`, `testImplementation reactor-core`, `reactor-test` in `core/build.gradle` | **deleted** | this is the headline: core stops declaring Reactor anywhere |
| `ReactiveBenchmark` (`tests/src/main`), `ReactiveHeapProbeTest` (`tests/src/test`) | stay in `tests/`, now compiled against `nioflow-reactive` | they are the numbers that keep the facade honest; they belong with the other benchmarks |

**`CoreWithoutReactorTest` dies, and its death is the point.** It runs a flow under a classloader that hides `reactor.**` to prove core's zero-required-dependency promise cannot rot into a consumer's `NoClassDefFoundError`. After the split there is no Reactor on core's compile classpath *or* its test classpath, so the test would be asserting something the compiler already refuses to let anyone violate. **The promise stops being a test and becomes a property of the build** — which is strictly stronger, and is the best argument for this RFC that exists.

The counter-argument, stated fairly: keep `testImplementation reactor-core` in core *only* so the test still has something to hide, and the guard survives. It is rejected because the guard would be guarding nothing — core's main compile classpath has no Reactor, so no core class *can* reference one; the classloader is re-proving what javac already enforced. If the underlying worry (an optional dependency leaking into a required one) is worth a mechanical guard after the split, the honest version of that guard is a **generalized** one — hide `reactor.**`, `io.github.resilience4j.**` *and* `io.opentelemetry.**` at once, and run a flow. That is a different test, about `infrastructure` as a category, and it is future work, not this RFC.

What does **not** move: the javadoc in `NioFlow:107` and `NioStep:200-228` that names `Mono`, `executeMono` and `ReactiveFlow.propagate` in prose. Those are the paragraphs explaining *why* `executeAsync(Map)` and `AsyncStage`'s cancellation are shaped as they are; the caller they were written for now ships from another jar, and the words are still true. They are prose — core's bytecode after this RFC contains the string "Mono" only in a comment.

## The package name stays: `dev.nioflow.infrastructure.reactive`

The move is `git mv`; the diff of the 29 moved files is **empty**. That is a proposed acceptance criterion, not a coincidence:

- **Migration is one line in a build file and zero lines of Java.** Every `import dev.nioflow.infrastructure.reactive.Reactive;` in every consumer keeps compiling.
- **It is not a split package, and the objection deserves to be answered in writing** because it is the first thing anyone will raise. A split package is *the same package present in two jars*. After this RFC core does not contain `dev.nioflow.infrastructure.reactive` at all — it is deleted, not duplicated (an overlap release is impossible anyway; see *Non-goals*). Core keeps `dev.nioflow.infrastructure` (`OpenTelemetryMetrics`, `Resilience4jStages`) and the new jar owns `dev.nioflow.infrastructure.reactive`: two distinct packages, because Java packages do not nest — a parent package name grants nothing to its children. Legal on the classpath, and legal under JPMS if either build ever grows a `module-info` (neither has one today).
- The name still reads right: the architecture note defines `infrastructure` as *"optional adapters over `compileOnly` dependencies"*. Reactor is exactly that; it just now lives in the artifact that owns it.

The alternative — rename to `dev.nioflow.reactive` while we are breaking things anyway — buys tidiness and charges every consumer a second break in the same release. Rejected: one break per release, and the one we cannot avoid is the coordinate.

## Build wiring

```gradle
// reactive/settings.gradle
rootProject.name = 'reactive'

includeBuild('../core') {
    // Same substitution the WebFlux example uses, for the same reason: the included
    // build advertises 'dev.nioflow:core' (its project name), the dependency names the
    // PUBLISHED artifact. Without this, Gradle silently resolves the last RELEASED core
    // from Maven Central — and ReactiveMirrorTest would be checking the mirror against
    // yesterday's interfaces, which is the one thing it exists not to do.
    dependencySubstitution {
        substitute module('dev.nioflow:nioflow-core') using project(':')
    }
}
```

```gradle
// reactive/build.gradle — the essentials
dependencies {
    api 'dev.nioflow:nioflow-core:2.0.0'
    api 'io.projectreactor:reactor-core:3.7.0'
    testImplementation 'io.projectreactor:reactor-test:3.7.0'
    // junit-bom 6.0.0, as in core
}

mavenPublishing {
    coordinates('dev.nioflow', 'nioflow-reactive', version.toString())
    // publishToMavenCentral(), signAllPublications(), the same POM block as core
}
```

**Reactor is `api`, not `compileOnly`.** In core it was optional because a core user may legitimately not have it. In *this* artifact it is the reason the artifact exists — a `compileOnly` Reactor here would ship a jar whose every public signature mentions a type the POM never mentions, and whose only working consumer is one that happens to bring Reactor for other reasons (a WebFlux app does; a Kafka consumer using `pipeResilient` may not). `api`, not `implementation`, because `Mono` and `Flux` are in the exported signatures. The declared `3.7.0` is a floor, not a pin: a consumer under Spring Boot's BOM keeps resolving the BOM's Reactor, exactly as `examples/springwebflux-with-nioflow` does today.

The other three builds keep their own `includeBuild` graph. `tests/` will include **both** `../core` and `../reactive`, while `../reactive` itself includes `../core` — Gradle dedups included builds by directory, so this is fine, but note the coordinate mismatch that already bites in this repo: an included build advertises its **project** name (`dev.nioflow:core`, and now `dev.nioflow:reactive`) while the published artifact is `nioflow-core` / `nioflow-reactive`. `tests/` names the project coordinates directly (no substitution); the examples name the published ones and substitute. Both styles keep working; whichever a build already uses, it uses the same one for reactive.

**Versions are lockstep.** `nioflow-reactive:X` is compiled and tested against `nioflow-core:X` and depends on it transitively, so "add `nioflow-reactive`" is the whole instruction — a consumer never names core. A three-artifact future (an OTel or Resilience4j split, see *Non-goals*) is when a `nioflow-bom` starts earning its keep; two artifacts in lockstep do not need one.

## The one thing that gets worse — and the mitigation is CI, not hope

`ReactiveMirrorTest` reflects over `NioFlow`/`NioStep`/`Lane` and fails the build when a core step has no covariant override on its mirror, or when a reactive-only step of `ReactiveStep` is missing from `ReactiveLane`. RFC 0002 records that this test caught a real drift exactly once (`adaptMono(call, budget)` on the step but not on the lane — a remote call inside a branch or a fork could not take a budget).

Today that test runs inside `cd core && ./gradlew build`, so **you cannot add a step to `NioFlow` and get a green core build without the mirror**. After this RFC, you can: the test now lives in `reactive/`, and core's build knows nothing about it. That is a genuine loss of feedback, and it is the price of the split.

Two mitigations, both mandatory parts of this RFC:

1. **A `build.yml` that builds every module on every PR** — `core`, `reactive`, `tests`, both examples. The repo has `publish.yml` and `sonar.yml` and **no CI build workflow at all** today; core's own suite has been standing in for one. The split is what forces the repo to grow the matrix it should already have had.
2. **CLAUDE.md's feature workflow gains a line**: a step added to `NioFlow`/`NioStep`/`Lane` is not done until `cd reactive && ./gradlew test` passes. The mirror is a cross-module contract now, and the contributor doc has to say so.

## Publishing

`publish.yml`, on a `v*` tag, grows a second half:

```
test core → publish core → test reactive → publish reactive
```

Sequential, one tag, two artifacts. The reactive build compiles against the **local** core through `includeBuild`, so the release does not wait for Central to index core first; dependency substitution replaces the *resolution*, not the *declaration*, so the POM it writes still says `dev.nioflow:nioflow-core:<version>` — which is what consumers resolve.

## Sonar

- **SonarLint (local, pre-commit)**: `tools/sonarlint/run.sh` already takes a module directory and drives that module's Gradle wrapper. `tools/sonarlint/run.sh reactive` works unchanged — reactive is a standalone build with `compileJava`/`compileTestJava`, which is all the script asks for. CLAUDE.md's static-analysis section gains the third invocation.
- **SonarQube (CI)**: `sonar.yml` analyzes `core/` only, because "the library lives in core". After the split that sentence is false for a third of the library. Recommended: a second step, `working-directory: reactive`, with its own project key (`fabiangftech_nioflow-reactive`) and its own JaCoCo XML. Side benefit: core's coverage number stops being an average over a facade with very different testing economics.

## Docs that become false the day this lands

Not a chore list — these sentences currently *promise* the thing this RFC changes, and each is load-bearing somewhere:

- `docs/webflux.md:28` — *"Reactor is `compileOnly` in core … core keeps its zero required runtime dependencies"*. It becomes an install line: `implementation 'dev.nioflow:nioflow-reactive'`.
- `docs/api-reference.md:61` — *"in `dev.nioflow.infrastructure.reactive`; `reactor-core` is `compileOnly`"*. The package is right; the scope is now another artifact's.
- `CLAUDE.md:24, :66, :122-140` — the architecture section lists `infrastructure.reactive` as a core adapter and names `ReactiveMirrorTest`/`CoreWithoutReactorTest` as core build guards. Both claims expire; the WebFlux section becomes a module of its own, and the feature workflow grows the `cd reactive && ./gradlew test` line.
- `.github/workflows/sonar.yml:16-17` carries the comment *"the library (and the sonarqube plugin) live in core/: it is a standalone Gradle build, there is no root build to analyze"* — the assumption the split invalidates. Left alone, **29 main classes silently drop out of SonarCloud** and core's coverage percentage shifts for a reason nobody will remember.
- Prose in `Cancellable`, `NioFlow`, `NioStep`, `NioFlowMetrics`, `AsyncStage`, `DefaultNioEngine`, `ExecutionNioFlow` names `Mono`/`executeMono`/`handleMonoAsync`/`ReactiveFlow.propagate` — all `{@code}`, none `{@link}`, so **no javadoc breaks**. The words stay: they explain why those core APIs are shaped the way they are, and the caller they were written for is one coordinate away.
- RFCs 0002–0007 name `infrastructure.reactive` in their **Target** lines. They are historical records — they are not edited. This RFC is the one that says where the package went.

## Consumers, and the acceptance test that matters

- **`examples/springwebflux-with-nioflow`** adds `implementation 'dev.nioflow:nioflow-reactive'` and the substitution in `settings.gradle` — **and changes not one line of Java**. That is the acceptance test for the whole RFC: if a WebFlux app needs anything more than one coordinate, the split leaked.
- **`examples/springboot-with-nioflow`** changes nothing, and *that* is the payoff: it now resolves a core jar with no reactive classes in it.
- **`tests/`** adds `includeBuild('../reactive')`; `ReactiveBenchmark` and `ReactiveHeapProbeTest` compile against `nioflow-reactive`.

## Numbers

This is a packaging change: the bytecode that runs is the same bytecode, in a different jar. So the benchmark section is a *falsification* section, not a promise.

| Must be flat (before / after) | Why it is here |
| --- | --- |
| `NioFlowBenchmark` (throughput + `-prof gc`) | a moved class cannot cost the engine anything. If it does, something was not a pure move. |
| `ReactiveBenchmark` — `monoOverhead`, `contextPropagated`, the four-`handleMono` fusion score (57.1 ops/ms) | same, for the facade |
| `ReactiveHeapProbeTest` — 489 B (async stage) / 3 173 B (parked `handleMono`) per in-flight request | the RFC 0006 numbers must survive the move verbatim |

What genuinely changes, and is worth measuring once: **core's own build and test time** drops by the ~2 600 test lines and ~2 400 main lines it stops compiling, and the core jar stops carrying 29 classes a non-reactive consumer never loads.

## What this deliberately does NOT do

- **It does not split `OpenTelemetryMetrics` or `Resilience4jStages`.** The same argument applies to them in principle — they are `compileOnly` adapters over optional dependencies — and it does not apply *in practice*: they are two classes. Extraction has to earn its keep by size and by the weight of the dependency it removes; Reactor clears that bar by 29 classes, OTel does not clear it by two. Revisit if `infrastructure` grows a third heavy adapter.
- **It does not change one signature.** The moved diff is empty. Anything else in that diff is a bug in the execution of this RFC.
- **It does not add a `module-info.java`.** No split package is created, so nothing forces the question now.
- **It does not ship an overlap release** where the reactive classes exist in both jars. It cannot: the same fully-qualified classes in two jars on one classpath is a duplicate-class hazard, not a deprecation path. The cut is atomic.

## Risks

| Risk | Mitigation |
| --- | --- |
| **Mirror drift**: core builds green with a step the reactive facade never got. *This is the risk of this RFC.* | The `build.yml` matrix above (every module, every PR) plus the CLAUDE.md workflow line. `ReactiveMirrorTest` is unchanged and still fails the build — it just fails a different build. |
| **A consumer upgrades core and gets `NoClassDefFoundError: …reactive.Reactive`**, because the atomic cut gives no overlap release. | The break is real and unavoidable, so it goes in the version number: release the split as **2.0.0**, with a release note whose fix is one line. It is source-compatible (imports unchanged) but not a binary drop-in, and the version has to say so. |
| **Version skew**: someone pins `nioflow-reactive:2.1.0` against `nioflow-core:2.0.0`. An *upgrade* is resolved for them (the transitive dependency wins); a deliberate *downgrade* pin can produce a `NoSuchMethodError` on a step the mirror added. | Lockstep versions, documented on the install snippet: the two coordinates carry the same number. A `nioflow-bom` is the real answer and is future work — at two artifacts it would be ceremony. |
| **The Reactor version is now declared in a published POM** (`api 'io.projectreactor:reactor-core:3.7.0'`), where core declared none. A consumer could end up resolving a Reactor the app did not choose. | It is a floor, not a pin: a Spring Boot app's BOM constraint wins, and Gradle/Maven pick the higher version otherwise — the resolution a WebFlux consumer gets today is the one it keeps. The alternative (`compileOnly`, i.e. a POM that never mentions the dependency every signature is made of) hides the requirement instead of managing it. The WebFlux example is the test: it must still resolve the BOM's Reactor after the split. |
| **A fifth Gradle build** to keep in sync (JDK, JUnit BOM, plugin versions). | The repo already runs four. The duplication is the same duplication `tests/` and the examples already carry, and the alternative (a root aggregator build) is a much larger change than this RFC. |
| The facade later needs something from core that is not public, and the module boundary turns a one-line change into an SPI negotiation. | That is the boundary working. Today the facade needs nothing (grep, above); if tomorrow it does, the right response is to ask why a *decorator over the public API* needs a private door — the answer is usually that the feature belongs in core. |

## Testing

- The 10 moved test classes pass **unchanged** in `reactive/` (`cd reactive && ./gradlew test`) — no rewrite, no new base class, no widened access. If any of them needs an edit beyond its file path, the extraction is not clean and the RFC's premise is wrong.
- `cd core && ./gradlew test` passes with Reactor **absent from every configuration** — the replacement for `CoreWithoutReactorTest`, enforced by javac instead of by a classloader.
- `cd tests && ./gradlew test jmh` — the stress tests and the benchmarks above, against the two-artifact composite.
- `cd examples/springwebflux-with-nioflow && ./gradlew test` — the BlockHound-guarded suite, unchanged, proving a real consumer needs only the new coordinate.
