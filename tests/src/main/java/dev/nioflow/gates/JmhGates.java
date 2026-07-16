package dev.nioflow.gates;

import dev.nioflow.benchmark.CompiledChainBenchmark;
import dev.nioflow.benchmark.NioFlowBenchmark;
import dev.nioflow.benchmark.ReactiveBenchmark;
import dev.nioflow.benchmark.SyncStageBenchmark;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * RFC 0021 — JMH regression gates. This is NOT a benchmark; it RUNS the existing
 * benchmarks (via the JMH Runner API, which hands back {@link RunResult} with the
 * throughput score AND {@code ·gc.alloc.rate.norm} bytes/op from the GC profiler)
 * and asserts the performance invariants the throughput series won.
 *
 * <p>Two gate kinds, both machine-independent so they survive a slow CI runner
 * (see the RFC): RATIO gates compare two benchmarks measured in the same run
 * (machine speed cancels), and ALLOCATION gates assert bytes/op (near-
 * deterministic — it counts allocations, not time).
 *
 * <p>{@code ./gradlew jmhGates} runs the fast canary (catches a ~2x regression);
 * {@code ./gradlew jmhGates -PgatesMode=full} runs the tighter full pass. Exit
 * code is non-zero if any gate trips, so CI fails the build.
 */
public final class JmhGates {

    private static final String ALLOC = "gc.alloc.rate.norm";
    private static final String STAGES = "stages";
    private static final String ENGINE_CALL_1 = "NioFlowBenchmark.engineCall[stages=1]";
    private static final String ENGINE_CALL_32 = "NioFlowBenchmark.engineCall[stages=32]";

    private JmhGates() {
    }

    public static void main(String[] args) throws RunnerException {
        boolean full = args.length > 0 && "full".equals(args[0]);
        // Reactor's async path JIT-compiles slowly, so the canary still needs a
        // few warmup iterations of real length or fourAsyncReactiveStages reads cold.
        int warmup = full ? 5 : 3;
        int iterations = full ? 6 : 3;

        Map<String, Double> throughput = new LinkedHashMap<>();
        Map<String, Double> allocation = new LinkedHashMap<>();

        // Run A — NioFlowBenchmark scaling + contention, restricted to stages 1 and 32.
        collect(run(warmup, iterations, builder -> builder
                .include(method(NioFlowBenchmark.class, "engineCall"))
                .include(method(NioFlowBenchmark.class, "engineCallContended"))
                .param(STAGES, "1", "32")), throughput, allocation);

        // Run B — the no-param benchmarks the other gates read.
        collect(run(warmup, iterations, builder -> builder
                .include(method(SyncStageBenchmark.class, "syncSingle"))
                .include(method(SyncStageBenchmark.class, "workerSingle"))
                .include(method(CompiledChainBenchmark.class, "plain8Compiled"))
                .include(method(CompiledChainBenchmark.class, "plain8Interpreted"))
                .include(method(ReactiveBenchmark.class, "fourAsyncReactiveStages"))
                .include(method(ReactiveBenchmark.class, "fourReactiveStages"))), throughput, allocation);

        report(throughput, allocation);

        List<String> failures = new ArrayList<>();

        // ── RATIO gates (throughput, machine-independent) ──
        ratioGate(failures, "links-are-free (32 stages cost < 2x of 1)", throughput,
                ENGINE_CALL_32, ENGINE_CALL_1, 0.5);
        // Real speedup is ~2.2x; a broken inlining collapses it to ~1.0x, so a
        // wide canary margin catches the regression while tolerating load noise
        // (throughput ratios compress toward 1.0 on a busy machine).
        ratioGate(failures, "boss-inlining is a speedup", throughput,
                "SyncStageBenchmark.syncSingle", "SyncStageBenchmark.workerSingle", full ? 1.6 : 1.15);
        ratioGate(failures, "compiled plan keeps parity", throughput,
                "CompiledChainBenchmark.plain8Compiled", "CompiledChainBenchmark.plain8Interpreted",
                full ? 0.85 : 0.6);
        ratioGate(failures, "async-stage fusion within band of blocking", throughput,
                "ReactiveBenchmark.fourAsyncReactiveStages", "ReactiveBenchmark.fourReactiveStages",
                full ? 0.9 : 0.75);
        // Contention is a MULTI-CORE claim: 8 threads only spread if there are
        // cores to spread onto, so this gate runs in the full pass (a real
        // multi-core box), not the canary (which may run on a 2-core CI runner
        // where 8 threads cannot scale and the gate would be meaningless). At 32
        // stages the denominator does real work per op, so it is far more stable
        // than the 1-stage one, which JMH leaves nearly cold.
        if (full) {
            ratioGate(failures, "boss is not a JVM-wide ceiling", throughput,
                    "NioFlowBenchmark.engineCallContended[stages=32]", ENGINE_CALL_32, 1.5);
        }

        // ── ALLOCATION gates (bytes/op, near-deterministic) ──
        allocGate(failures, "plain sealed chain allocation", allocation,
                ENGINE_CALL_1, full ? 850 : 1000);
        allocGate(failures, "boss-inlined chain allocation", allocation,
                "SyncStageBenchmark.syncSingle", full ? 320 : 420);
        // Fusion keeps allocation FLAT across links: a 32-stage chain must not
        // allocate materially more than a 1-stage one (no per-link allocation).
        allocRatioGate(failures, "fusion keeps allocation flat over links", allocation,
                ENGINE_CALL_32, ENGINE_CALL_1, 1.3);

        if (failures.isEmpty()) {
            System.out.println("\nAll JMH gates passed (" + (full ? "full" : "canary") + ").");
            return;
        }
        System.out.println("\nFAILED gates:");
        failures.forEach(failure -> System.out.println("  ✗ " + failure));
        System.exit(1);
    }

    private static Collection<RunResult> run(int warmup, int iterations, Consumer<OptionsBuilder> configure)
            throws RunnerException {
        OptionsBuilder builder = new OptionsBuilder();
        builder.forks(1)
                .warmupIterations(warmup)
                .measurementIterations(iterations)
                .warmupTime(TimeValue.seconds(2))
                .measurementTime(TimeValue.seconds(1))
                .addProfiler(GCProfiler.class)
                .shouldFailOnError(true);
        configure.accept(builder);
        Options options = builder.build();
        return new Runner(options).run();
    }

    private static void collect(Collection<RunResult> results, Map<String, Double> throughput,
                                Map<String, Double> allocation) {
        for (RunResult result : results) {
            String key = keyOf(result);
            throughput.put(key, result.getPrimaryResult().getScore());
            // The GC profiler key carries an interpunct prefix that varies across
            // JMH versions ("·gc.alloc.rate.norm"); match on the stable suffix.
            for (var secondary : result.getSecondaryResults().entrySet()) {
                if (secondary.getKey().endsWith(ALLOC)) {
                    allocation.put(key, secondary.getValue().getScore());
                    break;
                }
            }
        }
    }

    private static String keyOf(RunResult result) {
        String benchmark = result.getParams().getBenchmark();      // fqcn.method
        int method = benchmark.lastIndexOf('.');
        int type = benchmark.lastIndexOf('.', method - 1);
        String name = benchmark.substring(type + 1);               // Class.method
        Collection<String> params = result.getParams().getParamsKeys();
        if (params.contains(STAGES)) {
            name += "[stages=" + result.getParams().getParam(STAGES) + "]";
        }
        return name;
    }

    private static void ratioGate(List<String> failures, String name, Map<String, Double> scores,
                                  String numerator, String denominator, double minRatio) {
        Double top = scores.get(numerator);
        Double bottom = scores.get(denominator);
        if (top == null || bottom == null || bottom == 0) {
            failures.add(name + " — missing benchmark (" + numerator + " / " + denominator + ")");
            return;
        }
        double ratio = top / bottom;
        if (ratio < minRatio) {
            failures.add(String.format("%s — ratio %.2f < %.2f (%s / %s)",
                    name, ratio, minRatio, numerator, denominator));
        }
    }

    private static void allocGate(List<String> failures, String name, Map<String, Double> allocation,
                                  String benchmark, double maxBytesPerOp) {
        Double bytes = allocation.get(benchmark);
        if (bytes == null) {
            failures.add(name + " — no allocation reading for " + benchmark);
            return;
        }
        if (bytes > maxBytesPerOp) {
            failures.add(String.format("%s — %.0f B/op > %.0f B/op (%s)",
                    name, bytes, maxBytesPerOp, benchmark));
        }
    }

    private static void allocRatioGate(List<String> failures, String name, Map<String, Double> allocation,
                                       String numerator, String baseline, double maxRatio) {
        Double top = allocation.get(numerator);
        Double base = allocation.get(baseline);
        if (top == null || base == null || base == 0) {
            failures.add(name + " — missing allocation reading (" + numerator + " vs " + baseline + ")");
            return;
        }
        double ratio = top / base;
        if (ratio > maxRatio) {
            failures.add(String.format("%s — %.0f/%.0f = %.2f B/op ratio > %.2f (%s vs %s)",
                    name, top, base, ratio, maxRatio, numerator, baseline));
        }
    }

    private static String method(Class<?> benchmark, String name) {
        return benchmark.getName().replace(".", "\\.") + "\\." + name + "$";
    }

    private static void report(Map<String, Double> throughput, Map<String, Double> allocation) {
        System.out.println("\n── measured (throughput ops/ms, allocation B/op) ──");
        for (var entry : throughput.entrySet()) {
            Double bytes = allocation.get(entry.getKey());
            System.out.printf("  %-48s %10.2f ops/ms  %s%n",
                    entry.getKey(), entry.getValue(),
                    bytes == null ? "" : String.format("%8.0f B/op", bytes));
        }
    }
}
