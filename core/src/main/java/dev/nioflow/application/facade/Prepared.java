package dev.nioflow.application.facade;

import dev.nioflow.core.facade.PreparedChain;

/**
 * The opaque handle {@code prepare()}/{@code planFor()} hand out: a compiled
 * plan and nothing else (RFC 0032, extracted from {@code DefaultNioEngine}).
 */
record Prepared(CompiledChain plan) implements PreparedChain {
}
