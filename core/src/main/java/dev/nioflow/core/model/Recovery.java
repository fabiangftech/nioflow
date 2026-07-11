package dev.nioflow.core.model;

import java.util.List;
import java.util.function.Function;

public record Recovery(String name, Function<Throwable, Object> function,
                       List<Guard> guards) implements Link {
}
