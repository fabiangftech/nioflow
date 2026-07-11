package dev.nioflow.springbootwithnioflow.controller;

import dev.nioflow.core.facade.NioFlow;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class GreetingController {

    private final NioFlow<String> nioFlow;

    public GreetingController(NioFlow<String> nioFlow) {
        this.nioFlow = nioFlow;
    }

    @GetMapping("/greeting")
    public int greeting() {
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
                .execute();
    }
}
