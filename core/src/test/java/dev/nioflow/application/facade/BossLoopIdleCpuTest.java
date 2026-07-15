package dev.nioflow.application.facade;

import dev.nioflow.core.model.Decision;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The one risk RFC 0009 names: a spin-then-park loop that never reaches the park
 * would burn a core on an idle server. This is the mechanical proof it does park
 * — an idle boss must consume almost no CPU — and a regression guard against a
 * future edit that spins forever (e.g. a park guarded by a condition that is
 * always false, or a spin budget that never decrements).
 *
 * <p>Measures the DEDICATED engine's boss threads by name via
 * {@link ThreadMXBean#getThreadCpuTime}, so the JVM's other threads (GC, JIT,
 * the shared boss pool, the timer wheel) never pollute the reading.
 */
class BossLoopIdleCpuTest {

    // A parked thread consumes ~0; a spinning one consumes ~100% of wall time.
    // The 10% ceiling is far above the former and far below the latter — no
    // machine-speed flake, but a spins-forever regression trips it instantly.
    private static final double MAX_IDLE_CPU_FRACTION = 0.10;
    private static final Duration IDLE_WINDOW = Duration.ofSeconds(2);

    @Test
    void anIdleBossParksInsteadOfSpinningACore() throws InterruptedException {
        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        Assumptions.assumeTrue(threads.isThreadCpuTimeSupported(),
                "per-thread CPU time not supported on this JVM");
        threads.setThreadCpuTimeEnabled(true);

        var engine = DefaultNioEngine.dedicated(2);
        try {
            // Create and warm the boss threads, then let them drain and park.
            engine.append(new Decision(value -> true, engine.nextDecision(), List.of()));
            for (int i = 0; i < 50; i++) {
                engine.call(i, new ConcurrentHashMap<>()).join();
            }
            Thread.sleep(100); // settle into PARKED after the last drain

            long cpuBefore = dedicatedBossCpuNanos(threads);
            long wallBefore = System.nanoTime();
            Thread.sleep(IDLE_WINDOW.toMillis());
            long cpuAfter = dedicatedBossCpuNanos(threads);
            long wallAfter = System.nanoTime();

            double cpuFraction = (double) (cpuAfter - cpuBefore) / (wallAfter - wallBefore);
            System.out.printf("idle boss CPU over %s: %.4f cores (2 bosses)%n",
                    IDLE_WINDOW, cpuFraction);
            assertTrue(cpuFraction < MAX_IDLE_CPU_FRACTION,
                    "idle bosses burned " + cpuFraction + " cores — the spin loop is not parking");
        } finally {
            engine.shutdown(Duration.ofMillis(500));
        }
    }

    /**
     * The RFC's specific worry: a workload that trickles tasks "just faster than
     * the budget" keeping a core hot. It cannot, and this shows why — the spin
     * budget is ~µs, so at any inter-arrival above that the boss parks between
     * tasks and spends CPU only on the (tiny) work, not on spinning. A 5 ms drip
     * is far above the spin window; the boss should stay almost entirely parked.
     */
    @Test
    void aLightlyLoadedBossStaysMostlyParked() throws InterruptedException {
        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        Assumptions.assumeTrue(threads.isThreadCpuTimeSupported(),
                "per-thread CPU time not supported on this JVM");
        threads.setThreadCpuTimeEnabled(true);

        var engine = DefaultNioEngine.dedicated(2);
        try {
            engine.append(new Decision(value -> true, engine.nextDecision(), List.of()));
            for (int i = 0; i < 50; i++) {
                engine.call(i, new ConcurrentHashMap<>()).join();
            }

            long cpuBefore = dedicatedBossCpuNanos(threads);
            long wallBefore = System.nanoTime();
            // ~400 tasks over 2s: a 5 ms drip, comfortably slower than the spin.
            for (int i = 0; i < 400; i++) {
                engine.call(i, new ConcurrentHashMap<>()).join();
                Thread.sleep(5);
            }
            long cpuAfter = dedicatedBossCpuNanos(threads);
            long wallAfter = System.nanoTime();

            double cpuFraction = (double) (cpuAfter - cpuBefore) / (wallAfter - wallBefore);
            System.out.printf("lightly-loaded boss CPU (5ms drip): %.4f cores (2 bosses)%n", cpuFraction);
            // Loose ceiling: real work makes this non-zero, but a spins-forever
            // boss would sit near 1.0 (or 2.0 across both). This only catches that.
            assertTrue(cpuFraction < 0.30,
                    "a lightly-loaded boss burned " + cpuFraction + " cores — it is spinning, not parking");
        } finally {
            engine.shutdown(Duration.ofMillis(500));
        }
    }

    // Sum of CPU time (nanos) across the dedicated engine's boss threads. -1
    // means the platform could not measure a thread; those are dropped.
    private static long dedicatedBossCpuNanos(ThreadMXBean threads) {
        Map<Thread, StackTraceElement[]> all = Thread.getAllStackTraces();
        List<Long> ids = all.keySet().stream()
                .filter(thread -> thread.getName().startsWith("nio-flow-boss-dedicated-"))
                .map(Thread::threadId)
                .toList();
        long total = 0;
        for (long id : ids) {
            long cpu = threads.getThreadCpuTime(id);
            if (cpu > 0) {
                total += cpu;
            }
        }
        return total;
    }
}
