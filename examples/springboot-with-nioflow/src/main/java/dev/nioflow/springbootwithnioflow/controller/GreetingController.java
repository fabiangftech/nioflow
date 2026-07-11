package dev.nioflow.springbootwithnioflow.controller;

import dev.nioflow.core.facade.NioFlow;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

@Slf4j
@RestController
public class GreetingController {

    private final NioFlow<String, String> nioFlow;

    public GreetingController(NioFlow<String, String> nioFlow) {
        this.nioFlow = nioFlow;
    }

    /**
     * Non-blocking endpoint: the flow returns its ticket (CompletableFuture)
     * and Spring completes the response when it resolves — the request thread
     * is released immediately instead of waiting on execute().
     */
    @GetMapping("/greeting")
    public CompletableFuture<Integer> greeting() {
        return this.nioFlow
                .just("Hello World! - " + System.currentTimeMillis())
                .handle(String::toUpperCase)
                .background(value -> {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    log.info("Background task executed");
                })
                .adapt(String::length)
                .handle(value -> value + 1)
                .background(value -> {
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    log.info("Background task executed 2");
                })
                .executeAsync();
    }
}
