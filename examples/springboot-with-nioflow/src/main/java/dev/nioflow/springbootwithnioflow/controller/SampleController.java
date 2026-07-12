package dev.nioflow.springbootwithnioflow.controller;

import dev.nioflow.springbootwithnioflow.service.SampleService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

/**
 * One endpoint per rung of the ladder in {@link SampleService}. The last three
 * return a CompletableFuture: the servlet thread is released immediately and
 * the response is written when the pipeline finishes.
 */
@Slf4j
@RestController
@RequiredArgsConstructor
public class SampleController {

    private final SampleService sampleService;

    /** 1. handle + background. */
    @GetMapping("/greeting")
    public String greeting(@RequestParam(defaultValue = "Hello") String value) {
        return this.sampleService.greeting(value);
    }

    /** 2. filter: odd numbers are cut, and a cut is not an error. */
    @GetMapping("/even/{value}")
    public String evenOnly(@PathVariable int value) {
        return this.sampleService.evenOnly(value)
                .map(String::valueOf)
                .orElse("filtered out");
    }

    /** 3. adapt + handleSync + a reusable segment. */
    @GetMapping("/report/{value}")
    public String report(@PathVariable int value) {
        return this.sampleService.report(value);
    }

    /** 3b. The contract: Integer in, String out — the pipeline has to get there. */
    @GetMapping("/credit/{cents}")
    public String credit(@PathVariable int cents) {
        return this.sampleService.credit(cents);
    }

    /** 3b. Different input and output types: cents in, formatted invoice out. */
    @GetMapping("/invoice/{cents}")
    public String invoice(@PathVariable int cents) {
        return this.sampleService.invoice(cents);
    }

    /** 4. match: the first case that hits wins. */
    @GetMapping("/route/{amount}")
    public String route(@PathVariable int amount) {
        return this.sampleService.route(amount);
    }

    /** 5. timeout + retry + recover: this gateway recovers on the third attempt. */
    @GetMapping("/resilient")
    public String resilient(@RequestParam(defaultValue = "order-1") String value) {
        return this.sampleService.resilientCall(value);
    }

    /** 5b. The gateway hangs: the budget cuts it and recover answers. */
    @GetMapping("/broken")
    public String broken(@RequestParam(defaultValue = "order-2") String value) {
        return this.sampleService.brokenCall(value);
    }

    /** 6. fanOut: three 100ms calls in parallel, joined in declaration order. */
    @GetMapping("/enrich")
    public String enrich(@RequestParam(defaultValue = "customer-7") String customer) {
        return this.sampleService.enrich(customer);
    }

    /** 7. Rate-limited stage plus per-execution context. */
    @GetMapping("/tracked")
    public String tracked(@RequestParam(defaultValue = "payload") String value,
                          @RequestParam(defaultValue = "trace-1") String traceId) {
        return this.sampleService.tracked(value, traceId);
    }

    /** 8. Concurrent callers coalesce into one bulk call — each still gets its own result. */
    @GetMapping("/store")
    public CompletableFuture<String> store(@RequestParam(defaultValue = "item") String value) {
        return this.sampleService.store(value);
    }

    /** 9. Same key, same order — even under concurrent requests. */
    @GetMapping("/ordered/{key}/{value}")
    public CompletableFuture<Integer> ordered(@PathVariable String key, @PathVariable int value) {
        return this.sampleService.ordered(key, value);
    }

    /** 10. Non-blocking endpoint: the future IS the response. */
    @GetMapping("/greeting-async")
    public CompletableFuture<String> greetingAsync(@RequestParam(defaultValue = "  WORLD  ") String value) {
        return this.sampleService.greetingAsync(value);
    }
}
