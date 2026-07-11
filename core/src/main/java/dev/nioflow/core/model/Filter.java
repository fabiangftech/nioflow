package dev.nioflow.core.model;

import java.util.List;
import java.util.function.Predicate;

public record Filter(Predicate<Object> predicate, List<Guard> guards) implements Link {
}
