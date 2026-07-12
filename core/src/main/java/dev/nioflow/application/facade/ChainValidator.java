package dev.nioflow.application.facade;

import dev.nioflow.core.model.Background;
import dev.nioflow.core.model.Batch;
import dev.nioflow.core.model.Decision;
import dev.nioflow.core.model.FanOut;
import dev.nioflow.core.model.Filter;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.Link;
import dev.nioflow.core.model.Recovery;
import dev.nioflow.core.model.Stage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Structural validation, run at seal() and on every splice over a sealed
 * chain (build-time only — zero runtime footprint):
 *
 * - dangling guards: a link guarded by a decision that is never declared
 *   upstream can NEVER run (an absent decision fails every guard);
 * - contradictory guards: requiring the same decision to be both true and
 *   false makes the link unreachable;
 * - duplicate anchor names: splice targets the first match, so duplicates
 *   make runtime edits ambiguous;
 * - dead recoveries: a Recovery with no fallible link upstream can never
 *   catch anything;
 * - sync stages with a timeout or retry: a boss-inlined call cannot be cut,
 *   and retry backoff would park the boss.
 */
final class ChainValidator {

    private ChainValidator() {
    }

    static List<String> validate(List<Link> links) {
        List<String> problems = new ArrayList<>();
        Set<Integer> declaredDecisions = new HashSet<>();
        Map<String, Integer> anchorNames = new HashMap<>();
        boolean fallibleUpstream = false;

        for (int i = 0; i < links.size(); i++) {
            Link link = links.get(i);
            String label = describe(link, i);

            List<Guard> guards = link.guards();
            if (guards != null) {
                Map<Integer, Boolean> expectations = new HashMap<>();
                for (Guard guard : guards) {
                    if (!declaredDecisions.contains(guard.decision())) {
                        problems.add(label + " is guarded by decision " + guard.decision()
                                + " which is not declared upstream (the link can never run)");
                    }
                    Boolean previous = expectations.put(guard.decision(), guard.expected());
                    if (previous != null && previous != guard.expected()) {
                        problems.add(label + " has contradictory guards on decision " + guard.decision()
                                + " (the link can never run)");
                    }
                }
            }

            String anchor = anchorName(link);
            if (anchor != null) {
                Integer firstIndex = anchorNames.putIfAbsent(anchor, i);
                if (firstIndex != null) {
                    problems.add(label + " duplicates the anchor name '" + anchor
                            + "' already used at index " + firstIndex + " (splice would be ambiguous)");
                }
            }

            if (link instanceof Recovery && !fallibleUpstream) {
                problems.add(label + " has nothing fallible upstream to recover from");
            }

            if (link instanceof Stage stage && stage.sync()) {
                if (stage.timeout() != null) {
                    problems.add(label + " is sync (boss-inlined) but declares a timeout"
                            + " (an inlined call cannot be cut; drop the timeout or the sync marker)");
                }
                if (stage.retry() != null) {
                    problems.add(label + " is sync (boss-inlined) but declares a retry policy"
                            + " (backoff would park the boss; drop the retry or the sync marker)");
                }
            }

            if (link instanceof Decision decision) {
                declaredDecisions.add(decision.id());
            }
            fallibleUpstream = fallibleUpstream || isFallible(link);
        }
        return problems;
    }

    private static boolean isFallible(Link link) {
        // Background never fails the flow; Recovery only runs on the error path.
        return switch (link) {
            case Stage ignored -> true;
            case FanOut ignored -> true;
            case Batch ignored -> true; // the bulk call can fail every batched value
            case Decision ignored -> true; // predicates can throw
            case Filter ignored -> true;
            case Background ignored -> false;
            case Recovery ignored -> false;
        };
    }

    private static String anchorName(Link link) {
        return switch (link) {
            case Stage stage -> stage.name();
            case Background background -> background.name();
            case Recovery recovery -> recovery.name();
            case FanOut fanOut -> fanOut.name();
            case Batch batch -> batch.name();
            default -> null;
        };
    }

    private static String describe(Link link, int index) {
        String type = link.getClass().getSimpleName().toLowerCase();
        String anchor = anchorName(link);
        return anchor != null ? type + " '" + anchor + "' (index " + index + ")" : type + " (index " + index + ")";
    }
}
