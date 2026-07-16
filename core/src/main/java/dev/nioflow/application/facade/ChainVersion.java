package dev.nioflow.application.facade;

import dev.nioflow.core.model.Link;

import java.util.List;

/**
 * The engine's chain and the dispatch plan compiled for it, published as one
 * atomic value (RFC 0032, extracted from {@code DefaultNioEngine}): a single
 * {@code version.set(...)} means no call can ever see a new chain paired with an
 * old plan. A sealed chain carries a non-null plan; an unsealed one is
 * interpreted ({@code plan == null}).
 */
record ChainVersion(List<Link> links, CompiledChain plan) {
}
