package dev.nioflow.unit;

import dev.nioflow.application.facade.DefaultNioFlow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

class DefaultNioFlowEditTest {

    @Test
    void removeTakesTheStageOutForValuesInjectedAfterwards() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle("increment", x -> x + 1)
                    .handle("tenfold", x -> x * 10);

            assertEquals(20, defaultNioFlow.just(1).join());

            defaultNioFlow.remove("tenfold");

            assertEquals(2, defaultNioFlow.just(1).join());
        }
    }

    @Test
    void replaceSwapsANamedStageForAMultiLinkSegment() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle("core", x -> x + 1);

            assertEquals(2, defaultNioFlow.just(1).join());

            defaultNioFlow.replace("core", segment -> segment
                    .handle(x -> x * 10)
                    .submit(x -> x + 5));

            assertEquals(15, defaultNioFlow.just(1).join());
        }
    }

    @Test
    void insertBeforeAndAfterSpliceAroundTheAnchor() {
        try (DefaultNioFlow<String> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle("mid", x -> x + "M");

            defaultNioFlow.insertBefore("mid", segment -> segment.handle(x -> x + "B"));
            defaultNioFlow.insertAfter("mid", segment -> segment.handle(x -> x + "A"));

            assertEquals("BMA", defaultNioFlow.just("").join());
        }
    }

    @Test
    void valuesInFlightFinishOnTheVersionTheyWereInjectedInto() throws Exception {
        try (DefaultNioFlow<String> defaultNioFlow = new DefaultNioFlow<>()) {
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch gate = new CountDownLatch(1);
            List<String> completed = new CopyOnWriteArrayList<>();
            defaultNioFlow.submit("gate", x -> {
                        entered.countDown();
                        try {
                            gate.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return x;
                    })
                    .handle("suffix", x -> x + "-old")
                    .onComplete(completed::add);

            defaultNioFlow.just("v1");
            entered.await();

            defaultNioFlow.replace("suffix", segment -> segment.handle(x -> x + "-new"));
            gate.countDown();
            defaultNioFlow.join();

            defaultNioFlow.just("v2");
            defaultNioFlow.join();

            assertEquals(List.of("v1-old", "v2-new"), completed);
        }
    }

    @Test
    void anEditReleasesParkedValuesSoAppendsNoLongerResumeThem() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            defaultNioFlow.handle("increment", x -> x + 1)
                    .onComplete(completed::add);

            defaultNioFlow.just(1).join();
            assertEquals(1, defaultNioFlow.diagnostics().parked());

            defaultNioFlow.remove("increment");
            assertEquals(0, defaultNioFlow.diagnostics().parked());

            defaultNioFlow.handle(x -> x * 10); // must not resume the released value
            defaultNioFlow.just(5).join();

            assertEquals(List.of(2, 50), completed);
        }
    }

    @Test
    void editingInsideALaneStaysInsideTheLane() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.when(x -> x > 10)
                    .then(lane -> lane.handle("big", x -> x + 1_000))
                    .handle(x -> x);

            defaultNioFlow.replace("big", segment -> segment.handle(x -> x + 2_000));

            assertEquals(2_020, defaultNioFlow.just(20).join());
            assertEquals(5, defaultNioFlow.just(5).join(), "the other lane must not see the edit");
        }
    }

    @Test
    void replacingARegionAgainSwapsTheWholePreviousSegment() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle("routes", x -> x)
                    .handle("audit", x -> x + 1);

            defaultNioFlow.replace("routes", segment -> segment
                    .handle(x -> x * 2)
                    .handle(x -> x * 3));
            assertEquals(7, defaultNioFlow.just(1).join()); // 1*2*3 + 1

            defaultNioFlow.replace("routes", segment -> segment.handle(x -> x * 10));
            assertEquals(11, defaultNioFlow.just(1).join(), "the old segment must be gone entirely");

            defaultNioFlow.remove("routes");
            assertEquals(2, defaultNioFlow.just(1).join(), "remove must take out the whole region");
        }
    }

    @Test
    void aReplacedSegmentMayForkIntoRoutes() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle("routes", x -> x);

            defaultNioFlow.replace("routes", segment -> segment.match()
                    .is(x -> x > 100, lane -> lane.handle(x -> x - 100))
                    .is(x -> x > 10, lane -> lane.handle(x -> x - 10)));

            assertEquals(1, defaultNioFlow.just(101).join());
            assertEquals(1, defaultNioFlow.just(11).join());
            assertEquals(5, defaultNioFlow.just(5).join(), "unmatched values pass through");

            defaultNioFlow.replace("routes", segment -> segment.match()
                    .is(x -> x > 100, lane -> lane.handle(x -> x + 100)));

            assertEquals(201, defaultNioFlow.just(101).join(), "the new routes must apply");
            assertEquals(11, defaultNioFlow.just(11).join(), "the retired route must be gone");
        }
    }

    @Test
    void anEditKeepsUpstreamRecoveriesForValuesInjectedAfterwards() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle("boom", x -> {
                        throw new IllegalStateException("boom");
                    })
                    .onErrorResume(error -> -1)
                    .handle("suffix", x -> x * 10);

            defaultNioFlow.replace("suffix", segment -> segment.handle(x -> x * 100));

            assertEquals(-100, defaultNioFlow.just(1).join());
        }
    }

    @Test
    void editingAnUnknownAnchorThrows() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle("known", x -> x);

            assertThrows(IllegalArgumentException.class, () -> defaultNioFlow.remove("missing"));
            assertThrows(IllegalArgumentException.class,
                    () -> defaultNioFlow.insertAfter("missing", segment -> segment.handle(x -> x)));
        }
    }

    @Test
    void editingASealedFlowThrows() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle("stage", x -> x).seal();

            assertThrows(IllegalStateException.class, () -> defaultNioFlow.remove("stage"));
            assertThrows(IllegalStateException.class,
                    () -> defaultNioFlow.replace("stage", segment -> segment.handle(x -> x)));
        }
    }

    @Test
    void aSegmentBuilderMayOnlyDeclareLinks() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle("stage", x -> x);

            assertThrows(IllegalStateException.class, () -> defaultNioFlow.replace("stage",
                    segment -> segment.just(1)));
            assertThrows(IllegalStateException.class, () -> defaultNioFlow.replace("stage",
                    segment -> segment.onComplete(x -> {
                    })));
        }
    }
}
