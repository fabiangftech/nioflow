package dev.nioflow.springwebfluxwithnioflow;

import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.springwebfluxwithnioflow.model.Receipt;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.DEFINED_PORT;

/**
 * The central promise of the integration, proved MECHANICALLY: your code never
 * blocks a non-blocking thread.
 *
 * <p>{@code OrderEndpointsTest} makes the same claim by reading thread names — a
 * good test, but it observes where the code says it is, not what it does.
 * BlockHound (installed by its JUnit platform module, over the whole JVM this
 * suite runs in) instruments {@code Thread.sleep}, {@code park}, socket reads and
 * the rest, and throws on the actual park. The two tests below are its control
 * and its experiment.
 */
@SpringBootTest(webEnvironment = DEFINED_PORT, properties = {
        "server.port=8090",
        "stub.base-url=http://localhost:8090"
})
class BlockHoundTest {

    private final WebTestClient client = WebTestClient.bindToServer()
            .baseUrl("http://localhost:8090")
            .responseTimeout(Duration.ofSeconds(10))
            .build();

    @Test
    void blockHoundIsArmed() {
        // The control experiment. Without it, the test below would pass just as
        // happily with BlockHound absent — which is how a proof rots into a
        // decoration. A Reactor scheduler thread is a NonBlocking thread, exactly
        // like Netty's event loop: a Thread.sleep on it must blow up.
        AtomicReference<Throwable> caught = new AtomicReference<>();

        Mono.fromRunnable(() -> {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                })
                .subscribeOn(Schedulers.parallel())
                .onErrorResume(error -> {
                    caught.set(error);
                    return Mono.empty();
                })
                .block(Duration.ofSeconds(5));

        assertNotNull(caught.get(), "BlockHound is not installed: a Thread.sleep on a"
                + " non-blocking scheduler went through unnoticed");
        assertInstanceOf(BlockingOperationError.class, caught.get());
    }

    @Test
    void aBlockingCallPlantedOnABossTripsIt() throws Exception {
        // The engine's own event-loop rule, now mechanical: a boss orchestrates
        // every execution pinned to it and must never block — the same rule as
        // Netty's. handleSync is the one step that runs user code ON the boss, so
        // a sleep in there is the nioflow equivalent of blocking an event loop.
        // BlockHound catches the park, and the engine routes it to recover() like
        // any other stage failure (it catches Throwable, not just Exception —
        // which is why an Error here cannot kill the boss task).
        AtomicReference<Throwable> seen = new AtomicReference<>();

        try (DefaultNioFlow<String, String> flow = DefaultNioFlow.from(String.class)) {
            String result = flow.just("order-1")
                    .handleSync("pretends-to-be-cheap", value -> {
                        try {
                            Thread.sleep(5);        // on the boss: forbidden
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                        }
                        return value;
                    })
                    .recover(error -> {
                        seen.set(error);
                        return "caught";
                    })
                    .execute();

            assertEquals("caught", result);
        }
        assertInstanceOf(BlockingOperationError.class, rootOf(seen.get()),
                "the boss threads are not instrumented: " + seen.get());
    }

    @Test
    void theBlockingStagesOfTheExampleNeverTripIt() {
        // The experiment. /orders/1/pay is a BLOCKING JDBC-style repository call, a
        // WebClient call parked on with Mono.block() inside the engine, and a
        // detached fork — the full chain, over real HTTP, with BlockHound armed.
        // Every one of those parks happens on a VIRTUAL worker, so none of them is
        // on a non-blocking thread, so none of them trips. That is the promise:
        // the engine keeps the blocking off Netty's loops, and nobody had to write
        // subscribeOn(boundedElastic()) to get it.
        client.post().uri("/orders/{id}/pay", "1")
                .exchange()
                .expectStatus().isOk()
                .expectBody(Receipt.class)
                .value(receipt -> assertEquals("PAID", receipt.status()));

        // The Flux endpoint too: N executions in flight, all of them blocking on
        // workers while the event loop stays free to serve the response.
        client.post().uri("/orders/pay-all?ids=1,2,3&concurrency=8")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(Receipt.class)
                .hasSize(3);
    }

    private static Throwable rootOf(Throwable error) {
        Throwable cause = error;
        while (cause != null && cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }
}
