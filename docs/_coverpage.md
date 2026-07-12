# nio<span class="flow-accent">—</span>flow

> Typed pipelines on an event loop, for Java business logic that changes at runtime.

<div class="pipeline" aria-hidden="true">
  <span class="stage">just(order)</span><span class="link" style="--d:0s"></span><span class="stage worker">handle("price")</span><span class="link" style="--d:.9s"></span><span class="stage boss">receipt</span>
</div>

- Zero required runtime dependencies · virtual-thread workers · one event loop
- Edit live chains with `splice` and named regions — in-flight requests never notice

[Quickstart](#/quickstart)
[What is nio-flow](#/overview)
