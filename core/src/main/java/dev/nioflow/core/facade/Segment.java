package dev.nioflow.core.facade;

/**
 * Reusable piece of pipeline: defines a chain segment over a Lane<T> leaving
 * it at Lane<R>. Embedded with use(segment), its links are appended inline
 * with the caller's guards (a segment used inside a fork lane is lane-scoped),
 * segments compose (a segment can use() another) and they are independently
 * testable by embedding them in a small test flow. Build-time only: zero
 * runtime footprint.
 */
@FunctionalInterface
public interface Segment<T, R> {

    Lane<R> define(Lane<T> lane);
}
