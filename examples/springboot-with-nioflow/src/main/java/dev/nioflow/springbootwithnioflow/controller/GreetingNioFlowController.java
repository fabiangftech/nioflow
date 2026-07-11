package dev.nioflow.springbootwithnioflow.controller;

import dev.nioflow.core.facade.NioFlow;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class GreetingNioFlowController {

    private final NioFlow<String> defaultNioFlow;

    public GreetingNioFlowController(NioFlow<String> nioFlow) {
        this.defaultNioFlow = nioFlow;
    }

    @GetMapping(path = "/greeting")
    public ResponseEntity<?> greeting() {
        return ResponseEntity.ok(this.defaultNioFlow
                .just("Hello")
                .handle(s -> s + ", World!")
                .submit(s -> {
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    System.out.println("Greeting: " + s);
                    return s;
                }).join());
    }
}
