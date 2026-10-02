package com.example.springkafka.service;

import com.example.springkafka.dto.*;
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
    private OrderStreamsService streamsService;
    private DistributedTracingService tracingService;
    private KafkaConnectSimulatorService connectSimulator;

    @BeforeEach
    void setUp() {
        tracingService = new DistributedTracingService();

        producerService = new KafkaProducerService(null, null, tracingService) {
            @Override
            public CompletableFuture<PublishResponse> sendAsync(String topic, String key, OrderEvent event) {
                return CompletableFuture.completedFuture(new PublishResponse(topic, 0, 0, key, "SUCCESS", "now"));
            }

            @Override
            public CompletableFuture<PublishResponse> sendAsyncRaw(String topic, String key, Object payload) {
                return CompletableFuture.completedFuture(new PublishResponse(topic, 0, 0, key, "SUCCESS_RAW", "now"));
            }
        };

        streamsService = new OrderStreamsService(new ObjectMapper()) {
            @Override
            public void startTopology() {}
        };

        consumerService = new KafkaConsumerService(null, null, dedupService, tracingService, streamsService);

        eventSourcingService = new EventSourcingService(producerService, new ObjectMapper());
        sagaService = new SagaOrchestratorService(producerService);
        dedupService = new IdempotentDeduplicationService();
        redriveService = new DeadLetterRedriveService(producerService, consumerService);
        connectSimulator = new KafkaConnectSimulatorService(producerService, new ObjectMapper());
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

    @Test
    @DisplayName("KStream-KStream Sliding Window Join: Correlates Orders and Shipments")
    void testKStreamKStreamWindowJoin() {
        OrderEvent order = new OrderEvent("ORD-JOIN-1", "CUST-JOIN", "SKU-DESK", 1, 350.0, "HIGH", false, null);
        streamsService.registerOrderForWindowJoin(order);

        ShipmentEvent shipment = ShipmentEvent.of("SHIP-1", "ORD-JOIN-1", "TRK-9900", "FEDEX");
        Optional<OrderShipmentEnrichedEvent> joined = streamsService.correlateShipment(shipment);

        assertThat(joined).isPresent();
        assertThat(joined.get().orderId()).isEqualTo("ORD-JOIN-1");
        assertThat(joined.get().trackingNumber()).isEqualTo("TRK-9900");
        assertThat(joined.get().carrier()).isEqualTo("FEDEX");
        assertThat(joined.get().totalAmount()).isEqualTo(350.0);

        assertThat(streamsService.getAllJoinedShipments()).hasSize(1);
    }

    @Test
    @DisplayName("Session Windows: Aggregates user interactions based on inactivity gap")
    void testSessionWindowsAggregation() {
        UserSessionEvent evt1 = UserSessionEvent.of("SESS-1", "USER-42", "VIEW_PAGE", "/home");
        UserSessionEvent evt2 = UserSessionEvent.of("SESS-1", "USER-42", "ADD_TO_CART", "/item/123");
        UserSessionEvent evt3 = UserSessionEvent.of("SESS-1", "USER-42", "CHECKOUT_INIT", "/cart");

        streamsService.aggregateUserSession(evt1);
        streamsService.aggregateUserSession(evt2);
        UserSessionSummary summary = streamsService.aggregateUserSession(evt3);

        assertThat(summary.userId()).isEqualTo("USER-42");
        assertThat(summary.actionCount()).isEqualTo(3);
        assertThat(summary.actions()).contains("VIEW_PAGE", "ADD_TO_CART", "CHECKOUT_INIT");

        Optional<UserSessionSummary> fetched = streamsService.getUserSession("USER-42");
        assertThat(fetched).isPresent();
        assertThat(fetched.get().actionCount()).isEqualTo(3);
    }

    @Test
    @DisplayName("W3C Distributed Tracing: Injects traceparent and tracks multi-hop lineage graph")
    void testW3cDistributedTracingLineage() {
        W3cTraceContext root = tracingService.startTrace("web-gateway");
        assertThat(root.traceId()).hasSize(32);
        assertThat(root.spanId()).hasSize(16);

        String header = root.toTraceParentHeader();
        assertThat(header).startsWith("00-" + root.traceId());

        W3cTraceContext hop1 = tracingService.recordHop(header, "order-producer");
        assertThat(hop1.traceId()).isEqualTo(root.traceId());
        assertThat(hop1.parentSpanId()).isEqualTo(root.spanId());

        W3cTraceContext hop2 = tracingService.recordHop(hop1.toTraceParentHeader(), "inventory-consumer");
        assertThat(hop2.traceId()).isEqualTo(root.traceId());

        Optional<List<W3cTraceContext>> lineage = tracingService.getTraceLineage(root.traceId());
        assertThat(lineage).isPresent();
        assertThat(lineage.get()).hasSize(3);
        assertThat(lineage.get().get(0).serviceName()).isEqualTo("web-gateway");
        assertThat(lineage.get().get(1).serviceName()).isEqualTo("order-producer");
        assertThat(lineage.get().get(2).serviceName()).isEqualTo("inventory-consumer");
    }

    @Test
    @DisplayName("Kafka Connect & CDC: Simulates Debezium WAL capture, SMT PII masking, and Lake Sink")
    void testKafkaConnectCdcAndLakeSink() {
        Map<String, Object> orderState = Map.of(
                "orderId", "ORD-CDC-100",
                "customerId", "CUST-500",
                "creditCardNumber", "4111222233334444",
                "email", "alice@example.com",
                "total", 299.99
        );

        KafkaConnectSimulatorService.TransformedCdcMessage transformed =
                connectSimulator.ingestTableChange("orders", "INSERT", Map.of(), orderState);

        assertThat(transformed.topic()).isEqualTo("orders.cdc.raw");
        assertThat(transformed.appliedSmts()).contains("MaskFieldSMT[creditCardNumber]", "MaskFieldSMT[email]");
        assertThat(transformed.payloadJson()).contains("****-****-****-4444");
        assertThat(transformed.payloadJson()).doesNotContain("4111222233334444");

        assertThat(connectSimulator.getRawCdcEvents()).hasSize(1);
        assertThat(connectSimulator.getTransformedMessages()).hasSize(1);
        assertThat(connectSimulator.getLakeBatches()).hasSize(1);
        assertThat(connectSimulator.getLakeBatches().get(0).fileFormat()).isEqualTo("PARQUET_SNAPPY");
    }
}
