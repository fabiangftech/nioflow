package dev.nioflow.springbootwithnioflow;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class SpringbootWithNioflowApplicationTests {

    @Test
    void contextLoads() {
        // Wiring only: if the flow bean cannot be built (a bad type token, a
        // chain that fails validation at seal), the context fails to start and
        // this test is what says so. The behaviour lives in the other classes.
    }

}
