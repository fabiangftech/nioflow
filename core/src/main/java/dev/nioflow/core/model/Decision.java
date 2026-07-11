package dev.nioflow.core.model;

import java.util.List;
import java.util.function.Predicate;

public record Decision(Predicate<Object> predicate, int id, List<Guard> guards) implements Link {
}

