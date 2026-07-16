package dev.nioflow.application.facade;

import dev.nioflow.core.model.AsyncStage;
import dev.nioflow.core.model.Decision;
import dev.nioflow.core.model.Filter;
import dev.nioflow.core.model.Fork;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.Link;
import dev.nioflow.core.model.Recovery;
import dev.nioflow.core.model.Stage;

import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Dispatch plan precomputed once per chain version (at seal() and after
 * each splice) instead of rescanned per execution. For every no-timeout
 * Stage it records the static fusion window [i, runEnds[i]) — bounded
 * conservatively at the first link that can never fuse (Decision,
 * Background, timeout Stage) — and, when NO link in the window carries
 * guards, the precollected runs[i] array: those dispatches do zero
 * scanning and zero allocation. Windows containing guarded links keep the
 * per-execution guard selection, just bounded to the window. It also
 * records the chain's highest Decision id so executions size their
 * decision bitset without rescanning.
 *
 * <p>Extracted from {@code DefaultNioEngine} by RFC 0032; a self-contained
 * chain-analysis value with no engine coupling.
 */
record CompiledChain(List<Link> links, Link[][] runs, int[] runEnds, AsyncStage[][] asyncRuns,
                     int maxDecisionId, Map<Fork, CompiledChain> forkPlans) {

    static CompiledChain compile(List<Link> links) {
        int size = links.size();
        Link[][] runs = new Link[size][];
        int[] runEnds = new int[size];
        for (int i = 0; i < size; i++) {
            // No window starts at a sync stage: advance inlines it on the
            // boss and never dispatches there (validated chains can't
            // carry sync+timeout/retry). It still FUSES into a preceding
            // stage's window like any other no-timeout stage.
            if (!(links.get(i) instanceof Stage stage) || stage.timeout() != null || stage.sync()) {
                continue;
            }
            int end = i + 1;
            while (end < size && extendsWindow(links.get(end))) {
                end++;
            }
            runEnds[i] = end;
            if (unguarded(links, i, end)) {
                runs[i] = links.subList(i, end).toArray(Link[]::new);
            }
        }
        return new CompiledChain(links, runs, runEnds, asyncRuns(links),
                maxDecisionId(links), compileForks(links));
    }

    // Highest Decision id in the chain, -1 with none: sizes the per-execution
    // decision bitset by chain content, not by the engine-wide id counter
    // (which grows forever under per-request forks).
    static int maxDecisionId(List<Link> links) {
        int max = -1;
        for (int i = 0; i < links.size(); i++) {
            if (links.get(i) instanceof Decision decision && decision.id() > max) {
                max = decision.id();
            }
        }
        return max;
    }

    /**
     * The async fusion windows (RFC 0013): a run of consecutive UNGUARDED
     * AsyncStages is driven from the worker side, touching the boss once for
     * the whole run instead of twice per stage. Only unguarded runs of
     * length >= 2 are precollected — a guarded async stage (in a lane) and a
     * lone one fall back to single dispatch, identically. asyncRuns[i] is set
     * only at the START of a maximal run, which is the only index the boss
     * ever reaches (it drives the whole run and resumes past it).
     */
    private static AsyncStage[][] asyncRuns(List<Link> links) {
        int size = links.size();
        AsyncStage[][] asyncRuns = new AsyncStage[size][];
        for (int i = 0; i < size; i++) {
            // The START of a maximal run of unguarded async stages — the only
            // index the boss ever reaches (it drives the whole run past it).
            boolean runStart = isUnguardedAsync(links.get(i))
                    && !(i > 0 && isUnguardedAsync(links.get(i - 1)));
            if (runStart) {
                int end = i + 1;
                while (end < size && isUnguardedAsync(links.get(end))) {
                    end++;
                }
                if (end - i >= 2) {
                    asyncRuns[i] = links.subList(i, end).toArray(AsyncStage[]::new);
                }
            }
        }
        return asyncRuns;
    }

    private static boolean isUnguardedAsync(Link link) {
        List<Guard> guards = link.guards();
        return link instanceof AsyncStage && (guards == null || guards.isEmpty());
    }

    /**
     * A fork's sub-chain is immutable, so it compiles once with the chain
     * that carries it: the child dispatches off a real plan instead of
     * interpreting. Keyed by link IDENTITY (two structurally equal forks
     * are still two different links).
     */
    private static Map<Fork, CompiledChain> compileForks(List<Link> links) {
        Map<Fork, CompiledChain> plans = null;
        for (int i = 0; i < links.size(); i++) {
            if (links.get(i) instanceof Fork fork) {
                if (plans == null) {
                    plans = new IdentityHashMap<>();
                }
                plans.put(fork, compile(fork.chain()));
            }
        }
        return plans == null ? Map.of() : plans;
    }

    // The record's generated equals/hashCode/toString would compare the two
    // array components by reference, which never matches the value semantics
    // a record promises. The engine only ever compares plans by identity
    // (plan.links() != chain), so these exist to keep the contract honest.
    @Override
    public boolean equals(Object other) {
        return this == other
                || other instanceof CompiledChain(var otherLinks, var otherRuns, var otherRunEnds,
                var otherAsyncRuns, var otherMaxId, var otherForkPlans)
                && maxDecisionId == otherMaxId
                && links.equals(otherLinks)
                && Arrays.deepEquals(runs, otherRuns)
                && Arrays.equals(runEnds, otherRunEnds)
                && Arrays.deepEquals(asyncRuns, otherAsyncRuns)
                && forkPlans.equals(otherForkPlans);
    }

    @Override
    public int hashCode() {
        int result = links.hashCode();
        result = 31 * result + Arrays.deepHashCode(runs);
        result = 31 * result + Arrays.hashCode(runEnds);
        result = 31 * result + Arrays.deepHashCode(asyncRuns);
        result = 31 * result + maxDecisionId;
        return 31 * result + forkPlans.size();
    }

    @Override
    public String toString() {
        return "CompiledChain[links=" + links
                + ", runs=" + Arrays.deepToString(runs)
                + ", runEnds=" + Arrays.toString(runEnds)
                + ", asyncRuns=" + Arrays.deepToString(asyncRuns)
                + ", maxDecisionId=" + maxDecisionId
                + ", forkPlans=" + forkPlans.size() + "]";
    }

    private static boolean staticallyFusable(Link link) {
        return link instanceof Filter
                || link instanceof Recovery
                || (link instanceof Stage stage && stage.timeout() == null);
    }

    // Fusion across recorded decisions: a GUARDED non-fusable link (a
    // match() case's Decision, a lane's Background/FanOut) might be
    // skipped at runtime — its guards depend on decisions already
    // recorded — so the window extends through it and the per-execution
    // scan decides: guard-failed links are stepped over (a skipped
    // Decision records nothing), a passing one still ends the run there.
    // An UNGUARDED non-fusable link always executes: hard boundary, same
    // as before. This matches what the interpreted scan (no plan) already
    // does with its links.size() bound.
    private static boolean extendsWindow(Link link) {
        if (staticallyFusable(link)) {
            return true;
        }
        List<Guard> guards = link.guards();
        return guards != null && !guards.isEmpty();
    }

    private static boolean unguarded(List<Link> links, int from, int to) {
        for (int i = from; i < to; i++) {
            List<Guard> guards = links.get(i).guards();
            if (guards != null && !guards.isEmpty()) {
                return false;
            }
        }
        return true;
    }
}
