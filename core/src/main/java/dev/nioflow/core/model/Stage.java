package dev.nioflow.core.model;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

public record Stage(String name, Function<Object, Object> function, boolean async, Duration timeout,
                    Retry retry, List<Guard> guards) implements Link {
}
