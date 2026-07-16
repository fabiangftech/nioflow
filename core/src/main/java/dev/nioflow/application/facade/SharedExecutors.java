package dev.nioflow.application.facade;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Executors shared by every engine in the JVM (commonPool style): a pool of
 * daemon boss threads plus one virtual-thread worker pool, no matter how many
 * DefaultNioEngine/DefaultNioFlow instances exist. Each execution is pinned to
 * ONE boss (EventLoopGroup-style affinity), which keeps its orchestration state
 * single-threaded while letting concurrent executions spread across bosses
 * instead of queueing behind a single thread.
 *
 * <p>A lazily-initialized holder (its statics are built on first touch): the
 * shared pools exist only if some engine used the default constructor. Extracted
 * from {@code DefaultNioEngine} by RFC 0032.
 */
final class SharedExecutors {

    // Tunable at JVM level: -Dnioflow.bosses=N (default: available cores,
    // floor 2). Read once — the shared pool is a JVM-wide singleton.
    private static final int BOSS_COUNT = Integer.getInteger("nioflow.bosses",
            Math.max(2, Runtime.getRuntime().availableProcessors()));
    static final ExecutorService[] BOSSES = DefaultNioEngine.createBossPool(BOSS_COUNT, DefaultNioEngine.NIO_FLOW_BOSS);
    static final ExecutorService WORKERS = Executors.newVirtualThreadPerTaskExecutor();

    private SharedExecutors() {
    }
}
