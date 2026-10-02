package com.example.springkafka.service;

import com.example.springkafka.dto.DomainEvent;
import com.example.springkafka.dto.OrderEvent;
import com.example.springkafka.dto.PublishResponse;
import com.example.springkafka.dto.SagaInstance;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class EnterpriseKafkaFeaturesTest {

    private KafkaProducerService producerService;
    private KafkaConsumerService consumerService;
    private EventSourcingService eventSourcingService;
    private SagaOrchestratorService sagaService;
    private IdempotentDeduplicationService dedupService;
    private DeadLetterRedriveService redriveService;

    @BeforeEach
    void setUp() {
        producerService = new KafkaProducerService(null, null) {
            @Override
            public CompletableFuture<PublishResponse> sendAsync(String topic, String key, OrderEvent event) {
                return CompletableFuture.completedFuture(new PublishResponse(topic, 0, 0, key, "SUCCESS", "now"));
            }

            @Override
            public CompletableFuture<PublishResponse> sendAsyncRaw(String topic, String key, Object payload) {
                return CompletableFuture.completedFuture(new PublishResponse(topic, 0, 0, key, "SUCCESS_RAW", "now"));
            }
        };

        consumerService = new KafkaConsumerService();

        eventSourcingService = new EventSourcingService(producerService, new ObjectMapper());
        sagaService = new SagaOrchestratorService(producerService);
        dedupService = new IdempotentDeduplicationService();
        redriveService = new DeadLetterRedriveService(producerService, consumerService);
    }

    @Test
    @DisplayName("Event Sourcing: Append events, stream to topic, and rehydrate aggregate")
    void testEventSourcingAndRehydration() {
        String aggregateId = "ORD-TEST-001";
        OrderEvent order = new OrderEvent(aggregateId, "CUST-10", "SKU-PROD-A", 2, 99.99, "HIGH", false, null);

        DomainEvent event1 = eventSourcingService.appendEvent("ORDER", aggregateId, "ORDER_CREATED", order);
        DomainEvent event2 = eventSourcingService.appendEvent("ORDER", aggregateId, "PAYMENT_RESERVED", order);
        DomainEvent event3 = eventSourcingService.appendEvent("ORDER", aggregateId, "ORDER_SHIPPED", order);

        assertThat(event1.version()).isEqualTo(1);
        assertThat(event2.version()).isEqualTo(2);
        assertThat(event3.version()).isEqualTo(3);

        List<DomainEvent> history = eventSourcingService.getEventStream(aggregateId);
        assertThat(history).hasSize(3);

        Map<String, Object> rehydrated = eventSourcingService.rehydrateAggregate(aggregateId);
        assertThat(rehydrated.get("aggregateId")).isEqualTo(aggregateId);
        assertThat(rehydrated.get("replayedEventCount")).isEqualTo(3);
        assertThat(rehydrated.get("lastEventType")).isEqualTo("ORDER_SHIPPED");

        Map<String, Map<String, Object>> projections = eventSourcingService.getAllProjections();
        assertThat(projections).containsKey(aggregateId);
        assertThat(projections.get(aggregateId).get("status")).isEqualTo("ORDER_SHIPPED");
    }

    @Test
    @DisplayName("Saga Orchestration: Normal checkout completion")
    void testSagaOrchestrationSuccess() {
        OrderEvent order = new OrderEvent("ORD-SAGA-1", "CUST-123", "SKU-LAPTOP", 1, 1200.0, "HIGH", false, null);

        SagaInstance instance = sagaService.startOrderSaga(order, false);

        assertThat(instance).isNotNull();
        assertThat(instance.orderId()).isEqualTo("ORD-SAGA-1");
        assertThat(instance.status()).isEqualTo("COMPLETED");
        assertThat(instance.stepHistory()).isNotEmpty();

        Optional<SagaInstance> fetched = sagaService.getSaga(instance.sagaId());
        assertThat(fetched).isPresent();
        assertThat(fetched.get().status()).isEqualTo("COMPLETED");
    }

    @Test
    @DisplayName("Saga Orchestration: Payment failure triggers compensating rollback")
    void testSagaOrchestrationCompensatingFailure() {
        OrderEvent order = new OrderEvent("ORD-SAGA-FAIL", "CUST-456", "SKU-PHONE", 1, 800.0, "NORMAL", false, null);

        SagaInstance instance = sagaService.startOrderSaga(order, true);

        assertThat(instance).isNotNull();
        assertThat(instance.status()).isEqualTo("COMPENSATED");
        assertThat(instance.stepHistory().toString()).contains("COMPENSATION");
    }

    @Test
    @DisplayName("Idempotent Consumer: Deduplication sliding window intercepts duplicates")
    void testIdempotentDeduplication() {
        String messageKey = "ORD-DEDUP-001";

        boolean firstAttempt = dedupService.checkAndSet(messageKey, "hash-1");
        assertThat(firstAttempt).isTrue();

        boolean duplicateAttempt = dedupService.checkAndSet(messageKey, "hash-2");
        assertThat(duplicateAttempt).isFalse();

        assertThat(dedupService.isProcessed(messageKey)).isTrue();
        assertThat(dedupService.getDuplicateAttempts()).hasSize(1);
        assertThat(dedupService.getAllProcessedKeys()).hasSize(1);
    }

    @Test
    @DisplayName("Dead Letter Redrive: Re-dispatches dead letter messages")
    void testDeadLetterRedrive() {
        OrderEvent poisonOrder = new OrderEvent(
                "ORD-FAIL-1", "CUST-1", "SKU-ITEM", 1, 100.0,
                "HIGH", true, null
        );
        consumerService.consumeDeadLetterTopic(poisonOrder, 0, 10L, "orders.retryable.DLT", "CUST-1");

        Optional<Map<String, Object>> result = redriveService.redriveDeadLetter("ORD-FAIL-1", "orders.standard", true);
        assertThat(result).isPresent();
        assertThat(result.get().get("orderId")).isEqualTo("ORD-FAIL-1");
        assertThat(result.get().get("clearedFailureFlag")).isEqualTo(true);

        assertThat(redriveService.getRedriveHistory()).hasSize(1);
    }
}
