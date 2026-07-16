# Changelog

All notable changes to nio-flow are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/); the project uses
[Semantic Versioning](https://semver.org/). Two artifacts version together:
`dev.nioflow:nioflow-core` and `dev.nioflow:nioflow-reactive` (write both
coordinates on the same version — see the [quickstart](docs/quickstart.md)).

## [Unreleased]

The second design-audit cluster (RFCs 0031–0041). See the
[RFC index](docs/rfc/0000-index.md) for the full record.

### Changed — BREAKING

- **Reactive: an unbudgeted reactive step is now a build error by default**
  (RFC 0034). `requireBudget` is on in the default reactive config, so a
  `handleMono`/`adaptMono`/`handleMonoAsync`/`adaptMonoAsync`/`adaptFlux`/
  `fanOutMono` with no per-step budget and no `defaultBudget` fails at assembly,
  naming the step. This closes the forever-leak (a hung upstream parking a worker
  for the life of the JVM). **Migration:** declare a `defaultBudget(...)` on the
  flow, give the step its own budget, or — for a chain that genuinely parks on
  nothing (`Mono.just`, an in-memory cache) — waive it with `allowUnbudgeted()`.

### Added

- **`call()`/`callCancellable()` honor the engine's capacity + `OverflowPolicy`**
  (RFC 0031). The request/response path — everything the reactive facade runs on —
  is now admission-bounded, not only the `inject`/`await` queue.
- **The reactive context bridge reads ThreadLocal-backed trace context** (RFC
  0033): `propagate(KEY)` now finds a value a registered Micrometer
  `ThreadLocalAccessor` of the same name holds, so Micrometer Tracing / Sleuth /
  MDC actually cross. Optional (`io.micrometer:context-propagation`, probed once).
- **Per-key lane observability and bounding** (RFC 0039): `keyLaneDepth` /
  `keyLanesActive` metrics, and `DefaultNioEngine.keyLaneCapacity(maxDepth,
  policy)` to bound a hot key's backlog (off by default).
- **Operability runbook** (`docs/troubleshooting.md`) and this changelog.

### Fixed

- **Per-request `when`/`match` no longer falls off the decision bitset** (RFC
  0038): decision ids are compacted so long-running per-request branching never
  drops onto the per-execution overflow map.
- **The "Java 21+" floor is now enforced and true** (RFC 0037): the build targets
  `--release 21`, and two Java-22 unnamed-variable (`_`) uses were replaced, so
  the library really does compile on JDK 21 (LTS). The RFC index is reachable
  from the docs site, and benchmark numbers are reconciled to
  `docs/benchmarks.md` as the single source.

### Docs / tests

- The Spring Boot example wires one typed `@Bean` per contract instead of the
  wildcard `NioFlow<?, ?>` bean its own Javadoc warned against (RFC 0036).
- `ReactiveMirrorTest` now covers all twelve builder pairs, and `ReactiveParityTest`
  probes `match()` and flow-level `when()` behaviour (RFC 0035).
- `DefaultNioEngine` began shedding its nested types (RFC 0032 phase A):
  `CompiledChain`, `ChainVersion`, `Region`, `Prepared`, `RejectedCall` and
  `SharedExecutors` are now top-level.

## [2.1.0]

Documentation refresh and the throughput/observability series (RFCs 0009–0022):
the boss event loop, per-request dispatch plan, lock-free fan-out, async-stage
fusion, the `pipe` family, streaming out, the benchmarks evidence page, and the
first production-hardening cluster (RFCs 0023–0030).

## [2.0.0]

**The reactive facade became its own artifact** (RFC 0008):
`dev.nioflow:nioflow-reactive`. Core carries no Reactor in any configuration; a
reactive consumer writes both coordinates on the same version. See the
[quickstart](docs/quickstart.md) for the two-artifact setup.
