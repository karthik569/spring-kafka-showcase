package com.example.springkafka.service;

import com.example.springkafka.dto.OrderEvent;
import com.example.springkafka.dto.PublishResponse;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Service responsible for publishing events to Apache Kafka topics.
 * <p>
 * Demonstrates multiple publishing paradigms:
 * <ul>
 *   <li>Non-blocking asynchronous message dispatch with {@link CompletableFuture} callbacks</li>
 *   <li>Explicit partition targeting with distributed tracing record headers (e.g. {@code X-Correlation-ID})</li>
 *   <li>Transactional batch publishing via {@link KafkaTemplate#executeInTransaction}</li>
 * </ul>
 *
 * @author Spring Showcase Team
 * @version 1.0
 */
@Service
public class KafkaProducerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaTemplate<String, Object> genericKafkaTemplate;
    private final KafkaTemplate<String, byte[]> byteArrayKafkaTemplate;
    private final DistributedTracingService tracingService;

    @org.springframework.beans.factory.annotation.Autowired
    public KafkaProducerService(
            KafkaTemplate<String, Object> genericKafkaTemplate,
            KafkaTemplate<String, byte[]> byteArrayKafkaTemplate,
            DistributedTracingService tracingService) {
        this.genericKafkaTemplate = genericKafkaTemplate;
        this.byteArrayKafkaTemplate = byteArrayKafkaTemplate;
        this.tracingService = (tracingService != null) ? tracingService : new DistributedTracingService();
    }

    public KafkaProducerService(
            KafkaTemplate<String, Object> genericKafkaTemplate,
            KafkaTemplate<String, byte[]> byteArrayKafkaTemplate) {
        this(genericKafkaTemplate, byteArrayKafkaTemplate, new DistributedTracingService());
    }

    /**
     * Publishes an {@link OrderEvent} asynchronously to a designated Kafka topic using default partition hash routing.
     * Injects W3C distributed tracing context (traceparent) into Kafka record headers.
     *
     * @param topic the target Kafka topic name
     * @param key   the message key used by Kafka's DefaultPartitioner to calculate partition hashing
     * @param event the domain payload representing the order
     * @return a {@link CompletableFuture} yielding {@link PublishResponse} upon broker acknowledgment
     */
    public CompletableFuture<PublishResponse> sendAsync(String topic, String key, OrderEvent event) {
        OrderEvent enriched = event.withTimestamp();
        com.example.springkafka.dto.W3cTraceContext trace = tracingService.startTrace("producer-order-service");
        log.info("[KAFKA-PRODUCER] Sending async event to topic={} key={} traceId={}", topic, key, trace.traceId());

        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, enriched);
        record.headers().add("traceparent", trace.toTraceParentHeader().getBytes(StandardCharsets.UTF_8));
        record.headers().add("X-Correlation-ID", trace.traceId().getBytes(StandardCharsets.UTF_8));

        return genericKafkaTemplate.send(record)
                .thenApply(sendResult -> mapToSendResponse(sendResult, "SUCCESS"))
                .exceptionally(ex -> {
                    log.error("[KAFKA-PRODUCER] Failed to send message to topic={}: {}", topic, ex.getMessage());
                    return new PublishResponse(topic, -1, -1, key, "FAILED: " + ex.getMessage(), Instant.now().toString());
                });
    }

    public CompletableFuture<PublishResponse> sendShipmentAsync(String topic, String key, com.example.springkafka.dto.ShipmentEvent event) {
        log.info("[KAFKA-PRODUCER] Sending async shipment event to topic={} key={}", topic, key);
        return genericKafkaTemplate.send(topic, key, event)
                .thenApply(sendResult -> mapToSendResponse(sendResult, "SUCCESS"))
                .exceptionally(ex -> new PublishResponse(topic, -1, -1, key, "FAILED: " + ex.getMessage(), Instant.now().toString()));
    }

    public CompletableFuture<PublishResponse> sendUserSessionAsync(String topic, String key, com.example.springkafka.dto.UserSessionEvent event) {
        log.info("[KAFKA-PRODUCER] Sending async user session to topic={} key={}", topic, key);
        return genericKafkaTemplate.send(topic, key, event)
                .thenApply(sendResult -> mapToSendResponse(sendResult, "SUCCESS"))
                .exceptionally(ex -> new PublishResponse(topic, -1, -1, key, "FAILED: " + ex.getMessage(), Instant.now().toString()));
    }

    /**
     * Publishes a raw String or Object payload asynchronously.
     */
    public CompletableFuture<PublishResponse> sendAsyncRaw(String topic, String key, Object payload) {
        log.info("[KAFKA-PRODUCER] Sending async raw payload to topic={} key={}", topic, key);

        return genericKafkaTemplate.send(topic, key, payload)
                .thenApply(sendResult -> mapToSendResponse(sendResult, "SUCCESS_RAW"))
                .exceptionally(ex -> {
                    log.error("[KAFKA-PRODUCER] Failed to send raw message to topic={}: {}", topic, ex.getMessage());
                    return new PublishResponse(topic, -1, -1, key, "FAILED: " + ex.getMessage(), Instant.now().toString());
                });
    }

    /**
     * Publishes an {@link OrderEvent} to an exact target partition with custom distributed tracing headers.
     * <p>
     * Injects {@code X-Correlation-ID} and {@code X-Source-Service} headers into the {@link ProducerRecord}
     * for end-to-end distributed tracing across microservice boundaries.
     *
     * @param topic     the target Kafka topic name
     * @param partition the zero-based partition index where the record must be placed
     * @param key       the partition/message key
     * @param event     the domain payload representing the order
     * @return a {@link CompletableFuture} yielding {@link PublishResponse} upon broker acknowledgment
     */
    public CompletableFuture<PublishResponse> sendWithHeaders(String topic, Integer partition, String key, OrderEvent event) {
        OrderEvent enriched = event.withTimestamp();
        String correlationId = UUID.randomUUID().toString();

        ProducerRecord<String, Object> record = new ProducerRecord<>(
                topic,
                partition,
                key,
                enriched
        );
        record.headers().add(new RecordHeader("X-Correlation-ID", correlationId.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("X-Source-Service", "spring-kafka-showcase".getBytes(StandardCharsets.UTF_8)));

        log.info("[KAFKA-PRODUCER] Sending with headers correlationId={} to partition={}", correlationId, partition);

        return genericKafkaTemplate.send(record)
                .thenApply(sendResult -> mapToSendResponse(sendResult, "SUCCESS (Correlation: " + correlationId + ")"));
    }

    /**
     * Publishes multiple {@link OrderEvent} messages atomically within an isolated Kafka transaction.
     * <p>
     * If any message fails to publish or throws an exception, the entire transaction is aborted and rolled back,
     * ensuring read-committed consumers never observe partial batches.
     *
     * @param topic  the target Kafka topic
     * @param key    the message key for all events in this transaction
     * @param events varargs list of {@link OrderEvent} instances to publish atomically
     * @return {@code true} if the transaction committed successfully, {@code false} otherwise
     */
    public boolean sendInTransaction(String topic, String key, OrderEvent... events) {
        log.info("[KAFKA-PRODUCER] Executing Kafka transaction for {} events...", events.length);
        return Boolean.TRUE.equals(genericKafkaTemplate.executeInTransaction(operations -> {
            for (OrderEvent event : events) {
                operations.send(topic, key, event.withTimestamp());
            }
            log.info("[KAFKA-PRODUCER] Transaction committed successfully for topic={}", topic);
            return true;
        }));
    }

    /**
     * Publishes an inventory record to the compacted topic. If item is null, dispatches a tombstone record
     * (null payload) to trigger Kafka log compaction deletion.
     *
     * @param topic   target compacted topic
     * @param skuCode key for the inventory item
     * @param item    inventory item, or null to dispatch tombstone deletion record
     * @return CompletableFuture of PublishResponse
     */
    public CompletableFuture<PublishResponse> sendInventory(String topic, String skuCode, com.example.springkafka.dto.InventoryItem item) {
        Object payload = (item != null) ? item.withTimestamp() : null;
        log.info("[KAFKA-PRODUCER] Publishing inventory to topic={} skuCode={} isTombstone={}",
                topic, skuCode, (payload == null));

        return genericKafkaTemplate.send(topic, skuCode, payload)
                .thenApply(sendResult -> new PublishResponse(
                        sendResult.getRecordMetadata().topic(),
                        sendResult.getRecordMetadata().partition(),
                        sendResult.getRecordMetadata().offset(),
                        skuCode,
                        (payload == null) ? "TOMBSTONE_DISPATCHED" : "INVENTORY_UPDATED",
                        Instant.now().toString()
                ))
                .exceptionally(ex -> {
                    log.error("[KAFKA-PRODUCER] Failed to send inventory to topic={}: {}", topic, ex.getMessage());
                    return new PublishResponse(topic, -1, -1, skuCode, "FAILED: " + ex.getMessage(), Instant.now().toString());
                });
    }

    /**
     * Publishes a binary Apache Avro serialized payload.
     *
     * @param topic     target topic (e.g. orders.avro)
     * @param key       partition/order key
     * @param avroBytes compact binary Avro bytes
     * @return CompletableFuture yielding PublishResponse
     */
    public CompletableFuture<PublishResponse> sendAvroOrder(String topic, String key, byte[] avroBytes) {
        log.info("[KAFKA-PRODUCER] Publishing binary Avro event (size: {} bytes) to topic={} key={}",
                avroBytes.length, topic, key);

        return byteArrayKafkaTemplate.send(topic, key, avroBytes)
                .thenApply(sendResult -> new PublishResponse(
                        sendResult.getRecordMetadata().topic(),
                        sendResult.getRecordMetadata().partition(),
                        sendResult.getRecordMetadata().offset(),
                        key,
                        "AVRO_BINARY_PUBLISHED (bytes: " + avroBytes.length + ")",
                        Instant.now().toString()
                ))
                .exceptionally(ex -> {
                    log.error("[KAFKA-PRODUCER] Failed to send Avro to topic={}: {}", topic, ex.getMessage());
                    return new PublishResponse(topic, -1, -1, key, "FAILED: " + ex.getMessage(), Instant.now().toString());
                });
    }

    /**
     * Helper mapping method converting a Kafka {@link SendResult} into an application {@link PublishResponse}.
     *
     * @param result the metadata result returned by the Kafka broker
     * @param status the operational status description
     * @return structured {@link PublishResponse} with topic, partition, offset, and timestamp
     */
    private PublishResponse mapToSendResponse(SendResult<String, Object> result, String status) {
        return new PublishResponse(
                result.getRecordMetadata().topic(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset(),
                result.getProducerRecord().key(),
                status,
                Instant.now().toString()
        );
    }
}
