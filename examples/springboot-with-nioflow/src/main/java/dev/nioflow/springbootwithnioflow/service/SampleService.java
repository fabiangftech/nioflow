package dev.nioflow.springbootwithnioflow.service;

import dev.nioflow.core.facade.Context;
import dev.nioflow.core.facade.FlowResult;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.Segment;
import dev.nioflow.core.model.RateLimit;
import dev.nioflow.core.model.Retry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * A ladder of nio-flow examples, from the simplest step to the features you
 * cannot get from a plain chain of service calls. Read it top to bottom:
 * every level adds exactly one idea.
 *
 * <ol>
 *   <li>handle + background — transform, then fire and forget</li>
 *   <li>filter — cut the flow on purpose, and tell a cut from a null</li>
 *   <li>adapt + handleSync + segments — re-type the value, reuse a piece</li>
 *   <li>when / match — branch into lanes</li>
 *   <li>timeout + retry + recover — resilience in layers</li>
 *   <li>fanOut — call three services at once, join the results</li>
 *   <li>rate limit + context — pace a dependency, carry scratch state</li>
 *   <li>batch — many callers, one downstream call</li>
 *   <li>key — order the executions of one entity</li>
 *   <li>executeAsync — hand the future to the controller, block nothing</li>
 * </ol>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SampleService {

    /** Reusable piece of pipeline: trim on the event loop, then normalize on a worker. */
    private static final Segment<String, String> NORMALIZE = lane -> lane
            .handleSync("trim", String::trim)
            .handle("lowercase", String::toLowerCase);

    /** Typed per-execution scratch state — never threaded through the value. */
    private static final Context.Key<String> TRACE_ID = Context.Key.of("traceId");

    /** One downstream dependency, one bucket: 5 calls per second, shared by every stage that uses it. */
    private static final RateLimit PROVIDER_LIMIT = RateLimit.of(5, Duration.ofMillis(200));

    // One shared definition per contract (see NioFlowConfig): I is what just()
    // takes, O is what the pipeline must return. The per-request pipeline starts
    // at I, and adapt is what moves the value towards O.
    private final NioFlow<String, String> greetingFlow;
    private final NioFlow<String, String> textFlow;
    private final NioFlow<String, String> gatewayFlow;
    private final NioFlow<String, String> enrichFlow;
    private final NioFlow<String, String> bulkFlow;
    private final NioFlow<Integer, Integer> numberFlow;
    private final NioFlow<Integer, Integer> orderedFlow;
    private final NioFlow<Integer, Integer> reportFlow;
    // Input and output of different types: just() takes cents, and the pipeline
    // adapts them into the String the contract promises.
    private final NioFlow<Integer, String> invoiceFlow;
    private final NioFlow<Integer, String> creditFlow;

    // Observability for the tests and the demo endpoints.
    private final AtomicInteger bulkCalls = new AtomicInteger();
    private final List<Integer> processedInOrder = new CopyOnWriteArrayList<>();

    /**
     * Level 8 needs a SHARED definition: the batch link must be the same one
     * for every request, otherwise there is nothing for callers to pool into.
     * Declared once at startup — from here on, every execution runs it.
     */
    @PostConstruct
    void defineSharedBulkPipeline() {
        bulkFlow.batch("bulk-store", 8, Duration.ofMillis(50), values -> {
            bulkCalls.incrementAndGet();
            log.info("one bulk call for {} values", values.size());
            return values.stream().map(String::toUpperCase).toList();
        });
    }

    /**
     * The contract in one line: the flow takes an Integer and answers a String.
     * just() starts the pipeline at the INPUT type, so "charge" receives the
     * cents; adapt is what turns them into the String the method promises —
     * leave it out and this does not compile.
     */
    public String credit(int cents) {
        return creditFlow.just(cents)
                .handle("charge", item -> item * 2)      // still cents here
                .adapt(item -> "EUR " + item)            // Integer -> String
                .execute();                              // String
    }

    /** 1. handle transforms the value; background runs after it, and nobody waits for it. */
    public String greeting(String value) {
        return greetingFlow.just(value)
                .handle("greet", item -> item.concat(" World!"))
                .background("audit", item -> {
                    sleep(1_000);                       // slow on purpose: the caller is long gone
                    log.info("audited late: {}", item);
                })
                .execute();
    }

    /**
     * 2. filter cuts the execution deliberately. executeResult tells a cut apart
     * from a value — even a null one — which execute() cannot.
     */
    public Optional<Integer> evenOnly(int value) {
        FlowResult<Integer> result = numberFlow.just(value)
                .filter(item -> item % 2 == 0)
                .handle("scale", item -> item * 10)
                .executeResult();

        return switch (result) {
            case FlowResult.Completed<Integer>(Integer scaled) -> Optional.of(scaled);
            case FlowResult.Filtered<Integer> ignored -> Optional.empty();
        };
    }

    /**
     * 3. adapt is the only step that re-types the pipeline (Integer -> String).
     * handleSync runs inline on the event loop — for pure-CPU, sub-microsecond
     * work only. The segment is embedded as if it had been written here.
     */
    public String report(int value) {
        return reportFlow.just(value)
                .handleSync("double", item -> item * 2)
                .adapt(item -> "  Report #" + item + "  ")   // Integer -> String
                .use(NORMALIZE)                              // reusable piece, zero runtime cost
                .execute();
    }

    /**
     * 3b. A flow whose input and output are different types. The bean is a
     * NioFlow&lt;Integer, String&gt;: just() takes cents and the pipeline must reach a
     * String — the generics say exactly what goes in and what comes out, and the
     * compiler checks every step of the way there.
     */
    public String invoice(int cents) {
        return invoiceFlow.just(cents)                              // Integer in
                .handle("apply-vat", amount -> amount * 121 / 100)  // still cents
                .adapt(amount -> "EUR " + (amount / 100) + "." + String.format("%02d", amount % 100))
                .handle("stamp", amount -> amount + " (VAT included)")   // a String from here on
                .execute();                                         // String out
    }

    /** 4. match is first-match-wins: a small order never evaluates the "bulk" case. */
    public String route(int amount) {
        return reportFlow.just(amount)
                .adapt(item -> "order:" + item)
                .match()
                .is(order -> parseAmount(order) > 1_000,
                        lane -> lane.handle("manual-review", order -> order + " -> manual review"))
                .is(order -> parseAmount(order) > 100,
                        lane -> lane.handle("auto-approve", order -> order + " -> auto approved"))
                .otherwise(lane -> lane.handle("fast-path", order -> order + " -> fast path"))
                .handle("stamp", order -> order + " [done]")   // main line: runs for every value
                .execute();
    }

    /**
     * 5. Resilience in layers: each attempt gets its own 300ms budget, the retry
     * re-runs failed attempts with backoff, and recover is the final net. This
     * gateway fails twice and succeeds on the third attempt.
     */
    public String resilientCall(String value) {
        AtomicInteger attempts = new AtomicInteger();   // per request: captured by the stage

        return gatewayFlow.just(value)
                .handle("charge", item -> {
                    int attempt = attempts.incrementAndGet();
                    if (attempt < 3) {
                        throw new IllegalStateException("gateway unavailable (attempt " + attempt + ")");
                    }
                    return item + " charged on attempt " + attempt;
                }, Duration.ofMillis(300), Retry.of(3, Duration.ofMillis(50)))
                .recover("fallback", error -> value + " deferred: " + error.getMessage())
                .execute();
    }

    /** 5b. Same layers, but the gateway never answers: the timeout fires and recover catches it. */
    public String brokenCall(String value) {
        return gatewayFlow.just(value)
                .handle("hung-gateway", item -> {
                    sleep(2_000);                       // never finishes within the budget
                    return item;
                }, Duration.ofMillis(100))
                .recover("fallback", error -> value + " deferred: " + error.getClass().getSimpleName())
                .execute();
    }

    /**
     * 6. fanOut calls the three services concurrently on virtual threads and joins
     * their results in declaration order. Sequentially this would take ~300ms.
     */
    public String enrich(String customer) {
        List<Function<String, String>> branches = List.of(
                item -> remoteCall("credit", item),
                item -> remoteCall("loyalty", item),
                item -> remoteCall("risk", item));

        return enrichFlow.just(customer)
                .fanOut("enrich", branches, results -> String.join(" | ", results))
                .execute();
    }

    /**
     * 7. The rate limit paces the stage without blocking the event loop (it parks a
     * virtual thread). The context carries scratch state between stages — the value
     * type never learns about it.
     */
    public String tracked(String value, String traceId) {
        return textFlow.just(value)
                .handleContextual("open-trace", (item, ctx) -> {
                    ctx.put(TRACE_ID, traceId);
                    return item;
                })
                .handle("call-provider", item -> remoteCall("provider", item), PROVIDER_LIMIT)
                .handleContextual("close-trace", (item, ctx) -> item + " [trace=" + ctx.get(TRACE_ID) + "]")
                .execute();
    }

    /**
     * 8. Concurrent callers park at the batch link until 8 pile up (or 50ms pass),
     * then ONE bulk call serves them all — and each caller still gets its own
     * result. The batch is invisible from here.
     */
    public CompletableFuture<String> store(String value) {
        return bulkFlow.just(value).executeAsync();
    }

    /**
     * 9. Executions sharing a key run one at a time, in submission order — even
     * though they arrive concurrently. Different keys keep full parallelism.
     */
    public CompletableFuture<Integer> ordered(String key, int value) {
        return orderedFlow.just(value)
                .key(key)
                .handle("process", item -> {
                    sleep(20);                          // slow enough for the next one to overtake
                    processedInOrder.add(item);
                    return item;
                })
                .executeAsync();
    }

    /**
     * 10. The future goes straight to the controller: no servlet thread waits for
     * the pipeline. onComplete/onError observe this execution only.
     */
    public CompletableFuture<String> greetingAsync(String value) {
        return greetingFlow.just(value)
                .use(NORMALIZE)
                .handle("greet", item -> "hello " + item)
                .onComplete(result -> log.info("completed: {}", result))
                .onError(error -> log.warn("failed: {}", error.getMessage()))
                .executeAsync();
    }

    public int bulkCalls() {
        return bulkCalls.get();
    }

    public List<Integer> processedInOrder() {
        return List.copyOf(processedInOrder);
    }

    public void resetProbes() {
        bulkCalls.set(0);
        processedInOrder.clear();
    }

    private static int parseAmount(String order) {
        return Integer.parseInt(order.substring("order:".length()));
    }

    /** Stand-in for a remote dependency: it costs 100ms and it blocks — on a virtual thread, cheaply. */
    private static String remoteCall(String service, String value) {
        sleep(100);
        return service + "(" + value + ")";
    }

    private static void sleep(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
