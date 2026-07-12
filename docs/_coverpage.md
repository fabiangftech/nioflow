# nio<span class="flow-accent">—</span>flow

> Typed pipelines on an event loop, for Java business logic that changes at runtime.

<div class="pipeline" aria-hidden="true">
  <span class="stage">just(order)</span><span class="link" style="--d:0s"></span><span class="stage worker">handle("price")</span><span class="link" style="--d:.7s"></span><span class="stage boss">when(fraud?)</span><span class="link" style="--d:1.4s"></span><span class="stage worker">batch(16)</span><span class="link" style="--d:2.1s"></span><span class="stage">receipt</span>
</div>

- Zero required runtime dependencies · virtual-thread workers · one event loop
- Edit live chains with `splice` and named regions — in-flight requests never notice

[Quickstart](#/quickstart)
[GitHub](https://github.com/fabiangftech/nioflow)
[Maven Central](https://central.sonatype.com/artifact/dev.nioflow/nioflow-core)
