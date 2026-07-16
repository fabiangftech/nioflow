package dev.nioflow.application.facade;

import dev.nioflow.core.model.Link;

/**
 * A named region's boundaries, remembered by LINK IDENTITY (not index) so edits
 * elsewhere in the chain never stale them (RFC 0032, extracted from
 * {@code DefaultNioEngine}). Guarded by the engine's synchronized edit methods.
 */
record Region(Link first, Link last) {
}
