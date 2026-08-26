package com.example.springkafka.controller;

import com.example.springkafka.service.KafkaConsumerService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class KafkaAuditControllerTest {

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        KafkaConsumerService consumerService = new KafkaConsumerService();
        KafkaAuditController controller = new KafkaAuditController(consumerService);
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    void testAuditSummaryEndpoint() throws Exception {
        mockMvc.perform(get("/api/kafka/audit/summary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalReceived").value(0))
                .andExpect(jsonPath("$.totalDeadLetter").value(0));
    }
}
