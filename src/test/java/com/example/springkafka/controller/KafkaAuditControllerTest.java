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
        com.example.springkafka.service.OrderStreamsService streamsService =
                new com.example.springkafka.service.OrderStreamsService(new com.fasterxml.jackson.databind.ObjectMapper()) {
                    @Override
                    public void startTopology() {
                        // no-op in unit test
                    }
                    @Override
                    public String getStreamsState() {
                        return "RUNNING";
                    }
                };
        com.example.springkafka.avro.SchemaRegistryService schemaRegistryService =
                new com.example.springkafka.avro.SchemaRegistryService();
        com.example.springkafka.service.OutboxCdcService outboxService =
                new com.example.springkafka.service.OutboxCdcService(null, new com.fasterxml.jackson.databind.ObjectMapper());
        com.example.springkafka.service.EventSourcingService eventSourcingService =
                new com.example.springkafka.service.EventSourcingService(null, new com.fasterxml.jackson.databind.ObjectMapper());
        com.example.springkafka.service.SagaOrchestratorService sagaService =
                new com.example.springkafka.service.SagaOrchestratorService(null);
        com.example.springkafka.service.IdempotentDeduplicationService dedupService =
                new com.example.springkafka.service.IdempotentDeduplicationService();
        com.example.springkafka.service.DeadLetterRedriveService redriveService =
                new com.example.springkafka.service.DeadLetterRedriveService(null, consumerService);
        com.example.springkafka.service.DistributedTracingService tracingService =
                new com.example.springkafka.service.DistributedTracingService();
        com.example.springkafka.service.KafkaConnectSimulatorService connectSimulator =
                new com.example.springkafka.service.KafkaConnectSimulatorService(null, new com.fasterxml.jackson.databind.ObjectMapper());

        KafkaAuditController controller = new KafkaAuditController(
                consumerService,
                streamsService,
                schemaRegistryService,
                outboxService,
                eventSourcingService,
                sagaService,
                dedupService,
                redriveService,
                tracingService,
                connectSimulator
        );
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
