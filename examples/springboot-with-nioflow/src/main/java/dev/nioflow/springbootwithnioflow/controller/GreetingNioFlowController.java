package dev.nioflow.springbootwithnioflow.controller;

import dev.nioflow.core.facade.NioFlow;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class GreetingNioFlowController {

    private final NioFlow<String> nioFlow;

    public GreetingNioFlowController(NioFlow<String> nioFlow) {
        this.nioFlow = nioFlow;
    }

    @GetMapping(path = "/greeting")
    public ResponseEntity<?> greeting() {
        return ResponseEntity.ok(this.nioFlow.just("Hello")
                .handle("greeting", s -> s + ", World!")
                .join());
    }

    @GetMapping(path = "/greeting-2")
    public ResponseEntity<?> greetingTwo() {
        return ResponseEntity.ok(this.nioFlow.just("Hola")
                .handle("greeting", s -> s + ", Mundo!")
                .seal()
                .join());
    }
}
