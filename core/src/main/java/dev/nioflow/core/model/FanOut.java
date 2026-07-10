package dev.nioflow.core.model;

import java.util.List;
import java.util.function.Function;

/**
 * A splitting link: the parent value is consumed and every element of the returned
 * list continues down the chain as its own independent value, inheriting the
 * parent's lane decisions and injection sequence. An empty list drops the value
 * like a filter; a throwing function is a normal, recoverable stage failure.
 *
 * @param function maps the parent value to its children; must not return null
 * @param guards   the lane markers deciding which values reach this link
 */
public record FanOut(Function<Object, List<Object>> function, List<Guard> guards) implements Link {
}
