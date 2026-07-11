package dev.nioflow.core.model;

import java.util.List;
import java.util.function.Consumer;

public record Background(String name, Consumer<Object> effect, List<Guard> guards) implements Link {
}