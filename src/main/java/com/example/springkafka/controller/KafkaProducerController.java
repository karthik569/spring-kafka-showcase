package com.example.springkafka.controller;

import com.example.springkafka.dto.OrderEvent;
import com.example.springkafka.dto.PublishResponse;
import com.example.springkafka.service.KafkaProducerService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Tag(name = "Kafka Producer API", description = "Endpoints for publishing messages with key hashing, custom partitioning, filtering, retries, batching, and transactions")
@RestController
@RequestMapping("/api/kafka/publish")
public class KafkaProducerController {

    private final KafkaProducerService producerService;
    private final com.example.springkafka.avro.AvroSerializerService avroSerializer;
    private final com.example.springkafka.service.OutboxCdcService outboxService;
    private final com.example.springkafka.security.FieldEncryptionService encryptionService;
    private final com.example.springkafka.service.OrderStreamsService streamsService;
    private final com.example.springkafka.service.EventSourcingService eventSourcingService;
    private final com.example.springkafka.service.SagaOrchestratorService sagaService;
    private final com.example.springkafka.service.KafkaConnectSimulatorService connectSimulator;

    public KafkaProducerController(
            KafkaProducerService producerService,
            com.example.springkafka.avro.AvroSerializerService avroSerializer,
            com.example.springkafka.service.OutboxCdcService outboxService,
            com.example.springkafka.security.FieldEncryptionService encryptionService,
            com.example.springkafka.service.OrderStreamsService streamsService,
            com.example.springkafka.service.EventSourcingService eventSourcingService,
            com.example.springkafka.service.SagaOrchestratorService sagaService,
            com.example.springkafka.service.KafkaConnectSimulatorService connectSimulator) {
        this.producerService = producerService;
        this.avroSerializer = avroSerializer;
        this.outboxService = outboxService;
        this.encryptionService = encryptionService;
        this.streamsService = streamsService;
        this.eventSourcingService = eventSourcingService;
        this.sagaService = sagaService;
        this.connectSimulator = connectSimulator;
    }

    // 1. Simple Publish to Standard Topic
    @Operation(
            summary = "Publish standard order",
            description = "Publishes an order event asynchronously to 'orders.standard'. The partition is automatically chosen by hashing customerId or a random UUID."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Order published successfully",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = PublishResponse.class)))
    })
    @PostMapping("/simple")
    public CompletableFuture<PublishResponse> publishSimple(
            @RequestBody @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "Order event payload to publish", required = true) OrderEvent event) {
        String key = event.customerId() != null ? event.customerId() : UUID.randomUUID().toString();
        return producerService.sendAsync("orders.standard", key, event);
    }

    // 2. Publish to Specific Partition with Tracing Headers
    @Operation(
            summary = "Publish order to explicit partition",
            description = "Publishes an order event directly to a specified partition on 'orders.standard' with custom tracing headers ('X-Trace-ID', 'X-Sent-At')."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Order published to specified partition",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = PublishResponse.class)))
    })
    @PostMapping("/partitioned")
    public CompletableFuture<PublishResponse> publishPartitioned(
            @Parameter(description = "Zero-indexed partition number (0, 1, or 2 for orders.standard)", example = "1")
            @RequestParam(defaultValue = "0") int partition,
            @RequestBody @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "Order event payload", required = true) OrderEvent event) {
        String key = event.customerId() != null ? event.customerId() : "key-" + partition;
        return producerService.sendWithHeaders("orders.standard", partition, key, event);
    }

    // 3. Publish to Filtered High-Priority Topic
    @Operation(
            summary = "Publish VIP priority order",
            description = "Publishes to 'orders.high-priority'. The consumer container uses RecordFilterStrategy to discard any non-HIGH priority records."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Priority order published",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = PublishResponse.class)))
    })
    @PostMapping("/priority")
    public CompletableFuture<PublishResponse> publishPriority(
            @RequestBody @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "High-priority order event", required = true) OrderEvent event) {
        String key = event.orderId();
        return producerService.sendAsync("orders.high-priority", key, event);
    }

    // 4. Publish to Retryable Topic (Simulates 2 retries -> Dead Letter Topic)
    @Operation(
            summary = "Publish retryable order (DLT diversion test)",
            description = "Publishes to 'orders.retryable'. If fail=true, the consumer throws an exception, triggers 2 exponential backoff retries, and forwards the poisoned message to 'orders.retryable.DLT'."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Retryable order accepted and dispatched",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = PublishResponse.class)))
    })
    @PostMapping("/retry-dlt")
    public CompletableFuture<PublishResponse> publishRetryDlt(
            @Parameter(description = "Set to true to force simulated consumer failure and test DLT routing", example = "true")
            @RequestParam(defaultValue = "true") boolean fail,
            @RequestBody @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "Order event payload", required = true) OrderEvent event) {
        OrderEvent testEvent = new OrderEvent(
                event.orderId(),
                event.customerId(),
                event.skuCode(),
                event.quantity(),
                event.price(),
                event.priority(),
                fail,
                null
        );
        return producerService.sendAsync("orders.retryable", event.orderId(), testEvent);
    }

    // 5. Batch Publish
    @Operation(
            summary = "Publish batch of orders",
            description = "Dispatches multiple orders concurrently to 'orders.batch', which are ingested in bulk by a batch-enabled Kafka listener container."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Batch dispatched to orders.batch topic",
                    content = @Content(mediaType = "text/plain", schema = @Schema(example = "Dispatched 5 events to orders.batch topic")))
    })
    @PostMapping("/batch")
    public ResponseEntity<String> publishBatch(
            @RequestBody @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "List of order events to publish in batch", required = true) List<OrderEvent> events) {
        for (OrderEvent event : events) {
            producerService.sendAsync("orders.batch", event.orderId(), event);
        }
        return ResponseEntity.ok("Dispatched " + events.size() + " events to orders.batch topic");
    }

    // 6. Transactional Multi-Message Publish
    @Operation(
            summary = "Publish atomic transactional batch",
            description = "Publishes multiple order events inside a single Kafka producer transaction (`kafkaTemplate.executeInTransaction`), guaranteeing atomic all-or-nothing delivery semantics."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Transaction successfully committed",
                    content = @Content(mediaType = "text/plain", schema = @Schema(example = "Successfully committed transaction with 3 events"))),
            @ApiResponse(responseCode = "500", description = "Transaction aborted/rolled back",
                    content = @Content(mediaType = "text/plain", schema = @Schema(example = "Transaction rollback occurred")))
    })
    @PostMapping("/transaction")
    public ResponseEntity<String> publishTransaction(
            @RequestBody @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "List of events to commit atomically within a single transaction", required = true) List<OrderEvent> events) {
        boolean committed = producerService.sendInTransaction(
                "events.transactional",
                "tx-batch",
                events.toArray(new OrderEvent[0])
        );
        return committed
                ? ResponseEntity.ok("Successfully committed transaction with " + events.size() + " events")
                : ResponseEntity.internalServerError().body("Transaction rollback occurred");
    }

    // 7. Non-Blocking Retry Publishing (@RetryableTopic demonstration)
    @Operation(
            summary = "Publish to non-blocking retry topic",
            description = "Publishes to 'orders.nonblocking'. If fail=true, triggers @RetryableTopic exponential backoff across separate retry topics without blocking other messages in the partition."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Nonblocking retry order accepted",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = PublishResponse.class)))
    })
    @PostMapping("/nonblocking")
    public CompletableFuture<PublishResponse> publishNonblocking(
            @Parameter(description = "Set to true to force simulated consumer failure and test non-blocking backoff", example = "true")
            @RequestParam(defaultValue = "true") boolean fail,
            @RequestBody @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "Order event payload", required = true) OrderEvent event) {
        OrderEvent testEvent = new OrderEvent(
                event.orderId(),
                event.customerId(),
                event.skuCode(),
                event.quantity(),
                event.price(),
                event.priority(),
                fail,
                null
        );
        return producerService.sendAsync("orders.nonblocking", event.orderId(), testEvent);
    }

    // 8. Manual Acknowledgment Publishing
    @Operation(
            summary = "Publish to manual-ack topic",
            description = "Publishes to 'orders.manual-ack'. The consumer uses AckMode.MANUAL_IMMEDIATE to acknowledge offsets or issue backpressure nacks."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Manual ack order dispatched",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = PublishResponse.class)))
    })
    @PostMapping("/manual-ack")
    public CompletableFuture<PublishResponse> publishManualAck(
            @Parameter(description = "Set to true to simulate failure triggering negative acknowledgment (nack)", example = "false")
            @RequestParam(defaultValue = "false") boolean fail,
            @RequestBody @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "Order event payload", required = true) OrderEvent event) {
        OrderEvent testEvent = new OrderEvent(
                event.orderId(),
                event.customerId(),
                event.skuCode(),
                event.quantity(),
                event.price(),
                event.priority(),
                fail,
                null
        );
        return producerService.sendAsync("orders.manual-ack", event.orderId(), testEvent);
    }

    // 9. Compacted Topic Inventory Update
    @Operation(
            summary = "Upsert inventory record (Log Compaction)",
            description = "Publishes an updated state for a SKU key to 'inventory.compacted'. Kafka log cleaner retains the latest value per key."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Inventory record updated",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = PublishResponse.class)))
    })
    @PostMapping("/inventory")
    public CompletableFuture<PublishResponse> publishInventory(
            @RequestBody @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "Inventory item to upsert", required = true) com.example.springkafka.dto.InventoryItem item) {
        return producerService.sendInventory("inventory.compacted", item.skuCode(), item);
    }

    // 10. Compacted Topic Tombstone Record (Deletion)
    @Operation(
            summary = "Dispatch tombstone deletion record (Log Compaction)",
            description = "Sends a null payload record for the specified SKU key to 'inventory.compacted', triggering deletion of the key during log compaction."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Tombstone record dispatched",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = PublishResponse.class)))
    })
    @DeleteMapping("/inventory/tombstone/{skuCode}")
    public CompletableFuture<PublishResponse> publishTombstone(
            @Parameter(description = "SKU Code key to delete via tombstone", example = "SKU-LAPTOP-X1")
            @PathVariable String skuCode) {
        return producerService.sendInventory("inventory.compacted", skuCode, null);
    }

    // 11. Kafka Streams Input Event
    @Operation(
            summary = "Publish order to Kafka Streams input topic",
            description = "Publishes an order event to 'streams.orders.input'. Kafka Streams topology dynamically aggregates cumulative customer order count and spend."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Order published to streams topology",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = PublishResponse.class)))
    })
    @PostMapping("/streams-order")
    public CompletableFuture<PublishResponse> publishStreamsOrder(
            @RequestBody @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "Order event payload for stream aggregation", required = true) OrderEvent event) {
        String key = (event.customerId() != null) ? event.customerId() : event.orderId();
        return producerService.sendAsync("streams.orders.input", key, event);
    }

    // 12. Apache Avro Binary Serialization Publish
    @Operation(
            summary = "Publish order with Apache Avro binary serialization",
            description = "Encodes domain OrderEvent into compact Apache Avro binary bytes conforming to formal Avro record schema and publishes to 'orders.avro'."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Avro binary payload successfully published",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = PublishResponse.class)))
    })
    @PostMapping("/avro")
    public CompletableFuture<PublishResponse> publishAvro(
            @RequestBody @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "Order event to serialize into compact Avro bytes", required = true) OrderEvent event) {
        OrderEvent enriched = event.withTimestamp();
        byte[] avroBytes = avroSerializer.serializeToAvro(enriched);
        String key = (enriched.customerId() != null) ? enriched.customerId() : enriched.orderId();
        return producerService.sendAvroOrder("orders.avro", key, avroBytes);
    }

    // 13. Transactional Outbox Pattern & CDC Publish
    @Operation(
            summary = "Save order to Outbox table & dispatch via CDC",
            description = "Commits an order state transition into a local ACID Outbox table and invokes the CDC poller to stream it to 'orders.outbox.cdc', ensuring zero dual-write loss."
    )
    @PostMapping("/outbox-cdc")
    public ResponseEntity<com.example.springkafka.dto.OutboxRecord> publishOutboxCdc(
            @RequestBody OrderEvent event) {
        com.example.springkafka.dto.OutboxRecord record = outboxService.saveToOutbox(
                "ORDER",
                event.orderId(),
                "ORDER_CREATED",
                "orders.outbox.cdc",
                event.withTimestamp()
        );
        outboxService.dispatchPendingOutboxRecords();
        return ResponseEntity.ok(record);
    }

    // 14. Client-Side Field-Level Encryption (PII/GDPR)
    @Operation(
            summary = "Publish order with AES encrypted customer PII",
            description = "Applies client-side envelope encryption to sensitive fields (customerId) using AES-128 before publishing to 'orders.encrypted'. Kafka broker receives zero-knowledge ciphertext."
    )
    @PostMapping("/encrypted")
    public CompletableFuture<PublishResponse> publishEncrypted(
            @RequestBody OrderEvent event) {
        String encryptedCustomerId = encryptionService.encryptField(event.customerId());
        OrderEvent secureEvent = new OrderEvent(
                event.orderId(),
                encryptedCustomerId,
                event.skuCode(),
                event.quantity(),
                event.price(),
                event.priority(),
                event.simulateFailure(),
                java.time.Instant.now().toString()
        );
        return producerService.sendAsync("orders.encrypted", event.orderId(), secureEvent);
    }

    // 15. Real-Time Stream Join Simulation (KStream - KTable Enrichment)
    @Operation(
            summary = "Publish joined order enrichment event",
            description = "Correlates order event with inventory item metadata and publishes enriched event directly to 'orders.joined.output'."
    )
    @PostMapping("/joined-enrichment")
    public CompletableFuture<PublishResponse> publishJoinedEnrichment(
            @RequestBody OrderEvent event,
            @RequestParam(defaultValue = "Ultrabook Pro 16") String itemName,
            @RequestParam(defaultValue = "WH-US-EAST-1") String warehouseCode) {
        com.example.springkafka.dto.InventoryItem item = new com.example.springkafka.dto.InventoryItem(
                event.skuCode(), itemName, 100, warehouseCode, "ACTIVE", java.time.Instant.now().toString()
        );
        com.example.springkafka.dto.EnrichedOrderEvent enriched = com.example.springkafka.dto.EnrichedOrderEvent.of(event.withTimestamp(), item);
        streamsService.recordJoinedEnrichment(enriched);
        return producerService.sendAsyncRaw("orders.joined.output", event.orderId(), enriched);
    }

    // 16. Long-Term Cold / Tiered Retention Topic Publish
    @Operation(
            summary = "Publish order to long-term tiered retention topic",
            description = "Publishes orders to 'orders.tiered.retention' configured with extended retention and customized segment size for tiered offloading."
    )
    @PostMapping("/tiered-retention")
    public CompletableFuture<PublishResponse> publishTieredRetention(
            @RequestBody OrderEvent event) {
        return producerService.sendAsync("orders.tiered.retention", event.orderId(), event.withTimestamp());
    }

    // 17. Event Sourcing Command Append
    @Operation(
            summary = "Append domain event to Event Store (Event Sourcing)",
            description = "Appends an immutable domain event (e.g. ORDER_CREATED, PAYMENT_RESERVED) to aggregate stream and broadcasts to 'events.sourced'."
    )
    @PostMapping("/event-sourced/{aggregateId}")
    public ResponseEntity<com.example.springkafka.dto.DomainEvent> publishEventSourced(
            @PathVariable String aggregateId,
            @RequestParam(defaultValue = "ORDER_CREATED") String eventType,
            @RequestBody OrderEvent payload) {
        com.example.springkafka.dto.DomainEvent event = eventSourcingService.appendEvent("ORDER", aggregateId, eventType, payload);
        return ResponseEntity.ok(event);
    }

    // 18. Distributed Saga Checkout Orchestration
    @Operation(
            summary = "Execute distributed Saga checkout transaction",
            description = "Orchestrates multi-service transaction across Order, Payment, and Inventory topics. Set forcePaymentFail=true to trigger automatic compensating rollback."
    )
    @PostMapping("/saga/checkout")
    public ResponseEntity<com.example.springkafka.dto.SagaInstance> executeSagaCheckout(
            @RequestBody OrderEvent event,
            @RequestParam(defaultValue = "false") boolean forcePaymentFail) {
        com.example.springkafka.dto.SagaInstance saga = sagaService.startOrderSaga(event.withTimestamp(), forcePaymentFail);
        return ResponseEntity.ok(saga);
    }

    // 19. Publish Shipment for KStream-KStream Sliding-Window Join
    @Operation(
            summary = "Publish shipment event (KStream-KStream sliding-window correlation)",
            description = "Publishes a warehouse shipment event to 'streams.shipments.input' which is correlated with matching orders within a sliding window."
    )
    @PostMapping("/shipment")
    public CompletableFuture<PublishResponse> publishShipment(
            @RequestBody com.example.springkafka.dto.ShipmentEvent event) {
        return producerService.sendShipmentAsync("streams.shipments.input", event.orderId(), event);
    }

    // 20. Publish User Session Interaction for Session Windows
    @Operation(
            summary = "Publish user session event (Kafka Streams Session Windows)",
            description = "Publishes a user interaction event to 'user.sessions.input' aggregated based on an inactivity gap."
    )
    @PostMapping("/user-session")
    public CompletableFuture<PublishResponse> publishUserSession(
            @RequestBody com.example.springkafka.dto.UserSessionEvent event) {
        return producerService.sendUserSessionAsync("user.sessions.input", event.userId(), event);
    }

    // 21. Debezium CDC + SMT + Lake Sink Simulator
    @Operation(
            summary = "Simulate Debezium CDC table change with SMT masking and Lake Sink",
            description = "Simulates capturing a database table mutation, applies inline PII masking SMTs, streams to 'orders.cdc.raw', and batches into a simulated S3 Parquet data lake."
    )
    @PostMapping("/connect/cdc-table")
    public ResponseEntity<com.example.springkafka.service.KafkaConnectSimulatorService.TransformedCdcMessage> publishCdcChange(
            @RequestParam(defaultValue = "orders") String table,
            @RequestParam(defaultValue = "INSERT") String op,
            @RequestBody Map<String, Object> afterState) {
        var result = connectSimulator.ingestTableChange(table, op, Map.of(), afterState);
        return ResponseEntity.ok(result);
    }
}
