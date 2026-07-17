# nio<span class="flow-accent">flow</span>

> Typed pipelines on an event loop, for Java business logic that changes at runtime.

- Zero required runtime dependencies · virtual-thread workers · one event loop
- Edit live chains with `splice` and named regions — in-flight requests never notice
- Returns a `Mono` in WebFlux, and blocking code is safe inside it

```groovy
implementation 'dev.nioflow:nioflow-core:2.3.0'
implementation 'dev.nioflow:nioflow-reactive:2.3.0'  // WebFlux only
```

<p class="cover-actions"><a href="#/quickstart">Quickstart</a> <a href="#/overview">What is nioflow</a> <a href="https://central.sonatype.com/artifact/dev.nioflow/nioflow-core" target="_blank" rel="noopener">Maven Central</a></p>
