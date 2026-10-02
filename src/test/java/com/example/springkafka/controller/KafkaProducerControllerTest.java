package com.example.springkafka.controller;

import com.example.springkafka.dto.OrderEvent;
import com.example.springkafka.dto.PublishResponse;
import com.example.springkafka.service.KafkaProducerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.concurrent.CompletableFuture;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class KafkaProducerControllerTest {

    private MockMvc mockMvc;
    private final ObjectMapper objectMapper = new ObjectMapper();

    static class StubProducerService extends KafkaProducerService {
        public StubProducerService() {
            super(null, null);
        }

        @Override
        public CompletableFuture<PublishResponse> sendAsync(String topic, String key, OrderEvent event) {
            return CompletableFuture.completedFuture(
                    new PublishResponse(topic, 0, 10L, key, "SUCCESS", "2026-08-16T12:00:00Z")
            );
        }

        @Override
        public CompletableFuture<PublishResponse> sendWithHeaders(String topic, Integer partition, String key, OrderEvent event) {
            return CompletableFuture.completedFuture(
                    new PublishResponse(topic, partition, 11L, key, "SUCCESS", "2026-08-16T12:00:00Z")
            );
        }

        @Override
        public boolean sendInTransaction(String topic, String key, OrderEvent... events) {
            return true;
        }
    }

    @BeforeEach
    void setUp() {
        KafkaProducerController controller = new KafkaProducerController(
                new StubProducerService(),
                new com.example.springkafka.avro.AvroSerializerService()
        );
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    void testPublishSimpleEndpoint() throws Exception {
        OrderEvent event = new OrderEvent("ORD-100", "CUST-1", "IPHONE", 1, 999.0, "NORMAL", false, null);

        mockMvc.perform(post("/api/kafka/publish/simple")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(event)))
                .andExpect(status().isOk());
    }

    @Test
    void testPublishPartitionedEndpoint() throws Exception {
        OrderEvent event = new OrderEvent("ORD-101", "CUST-2", "MACBOOK", 1, 1999.0, "HIGH", false, null);

        mockMvc.perform(post("/api/kafka/publish/partitioned")
                        .param("partition", "1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(event)))
                .andExpect(status().isOk());
    }
}
