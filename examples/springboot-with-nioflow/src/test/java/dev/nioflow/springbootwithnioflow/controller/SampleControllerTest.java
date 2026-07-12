package dev.nioflow.springbootwithnioflow.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Every endpoint of the ladder, end to end through the real pipelines. */
@SpringBootTest
@AutoConfigureMockMvc
class SampleControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void greetingTransformsTheValue() throws Exception {
        mockMvc.perform(get("/greeting").param("value", "Hello"))
                .andExpect(status().isOk())
                .andExpect(content().string("Hello World!"));
    }

    @Test
    void evenValuesPassAndOddOnesAreFilteredOut() throws Exception {
        mockMvc.perform(get("/even/4"))
                .andExpect(status().isOk())
                .andExpect(content().string("40"));

        mockMvc.perform(get("/even/7"))
                .andExpect(status().isOk())
                .andExpect(content().string("filtered out"));
    }

    @Test
    void reportRetypesAndNormalizes() throws Exception {
        mockMvc.perform(get("/report/21"))
                .andExpect(status().isOk())
                .andExpect(content().string("report #42"));
    }

    @Test
    void routeTakesTheFirstMatchingCase() throws Exception {
        mockMvc.perform(get("/route/5000"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("manual review")));

        mockMvc.perform(get("/route/50"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("fast path")));
    }

    @Test
    void resilientEndpointRecoversOnTheThirdAttempt() throws Exception {
        mockMvc.perform(get("/resilient").param("value", "order-1"))
                .andExpect(status().isOk())
                .andExpect(content().string("order-1 charged on attempt 3"));
    }

    @Test
    void brokenGatewayIsCutByTheBudgetAndAnswered() throws Exception {
        mockMvc.perform(get("/broken").param("value", "order-2"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("TimeoutException")));
    }

    @Test
    void enrichJoinsTheThreeBranches() throws Exception {
        mockMvc.perform(get("/enrich").param("customer", "c-1"))
                .andExpect(status().isOk())
                .andExpect(content().string("credit(c-1) | loyalty(c-1) | risk(c-1)"));
    }

    @Test
    void trackedCarriesTheTraceThroughTheContext() throws Exception {
        mockMvc.perform(get("/tracked").param("value", "payload").param("traceId", "t-42"))
                .andExpect(status().isOk())
                .andExpect(content().string("provider(payload) [trace=t-42]"));
    }

    @Test
    void asyncEndpointsAnswerThroughTheirFuture() throws Exception {
        // The controller hands back a CompletableFuture: the servlet container
        // resumes the request once the pipeline completes.
        MvcResult pending = mockMvc.perform(get("/greeting-async").param("value", "  WORLD  "))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(pending))
                .andExpect(status().isOk())
                .andExpect(content().string("hello world"));
    }

    @Test
    void storeEndpointServesEachCallerItsOwnResult() throws Exception {
        MvcResult pending = mockMvc.perform(get("/store").param("value", "item-a"))
                .andExpect(request().asyncStarted())
                .andReturn();

        // Only one caller here: the 50ms window flushes the partial batch.
        mockMvc.perform(asyncDispatch(pending))
                .andExpect(status().isOk())
                .andExpect(content().string("ITEM-A"));
    }
}
