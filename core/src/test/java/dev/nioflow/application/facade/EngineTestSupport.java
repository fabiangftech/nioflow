package dev.nioflow.application.facade;

import dev.nioflow.core.model.Stage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

abstract class EngineTestSupport {

    DefaultNioEngine engine;

    @BeforeEach
    void setUpEngine() {
        engine = new DefaultNioEngine();
    }

    @AfterEach
    void tearDownEngine() {
        engine.shutdown(Duration.ofMillis(100));
    }

    static Stage stage(String name, Function<Object, Object> function) {
        return new Stage(name, function, false, null, List.of());
    }
}
