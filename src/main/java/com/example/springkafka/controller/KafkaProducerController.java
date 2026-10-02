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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Tag(name = "Kafka Producer API", description = "Endpoints for publishing messages with key hashing, custom partitioning, filtering, retries, batching, and transactions")
@RestController
@RequestMapping("/api/kafka/publish")
public class KafkaProducerController {

    private final KafkaProducerService producerService;
    private final com.example.springkafka.avro.AvroSerializerService avroSerializer;

    public KafkaProducerController(
            KafkaProducerService producerService,
            com.example.springkafka.avro.AvroSerializerService avroSerializer) {
        this.producerService = producerService;
        this.avroSerializer = avroSerializer;
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
}
