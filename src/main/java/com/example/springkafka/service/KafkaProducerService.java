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

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    /**
     * Constructs a new {@code KafkaProducerService} with the configured {@link KafkaTemplate}.
     *
     * @param kafkaTemplate the high-level Kafka producer template for message serialization and delivery
     */
    public KafkaProducerService(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publishes an {@link OrderEvent} asynchronously to a designated Kafka topic using default partition hash routing.
     * <p>
     * Non-blocking: attaches asynchronous handlers ({@code thenApply} and {@code exceptionally}) to construct
     * a {@link PublishResponse} containing the published topic, partition, offset, and delivery status.
     *
     * @param topic the target Kafka topic name
     * @param key   the message key used by Kafka's DefaultPartitioner to calculate partition hashing
     * @param event the domain payload representing the order
     * @return a {@link CompletableFuture} yielding {@link PublishResponse} upon broker acknowledgment
     */
    public CompletableFuture<PublishResponse> sendAsync(String topic, String key, OrderEvent event) {
        OrderEvent enriched = event.withTimestamp();
        log.info("[KAFKA-PRODUCER] Sending async event to topic={} key={}", topic, key);

        return kafkaTemplate.send(topic, key, enriched)
                .thenApply(sendResult -> mapToSendResponse(sendResult, "SUCCESS"))
                .exceptionally(ex -> {
                    log.error("[KAFKA-PRODUCER] Failed to send message to topic={}: {}", topic, ex.getMessage());
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

        ProducerRecord<String, OrderEvent> record = new ProducerRecord<>(
                topic,
                partition,
                key,
                enriched
        );
        record.headers().add(new RecordHeader("X-Correlation-ID", correlationId.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("X-Source-Service", "spring-kafka-showcase".getBytes(StandardCharsets.UTF_8)));

        log.info("[KAFKA-PRODUCER] Sending with headers correlationId={} to partition={}", correlationId, partition);

        return kafkaTemplate.send(record)
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
        return Boolean.TRUE.equals(kafkaTemplate.executeInTransaction(operations -> {
            for (OrderEvent event : events) {
                operations.send(topic, key, event.withTimestamp());
            }
            log.info("[KAFKA-PRODUCER] Transaction committed successfully for topic={}", topic);
            return true;
        }));
    }

    /**
     * Helper mapping method converting a Kafka {@link SendResult} into an application {@link PublishResponse}.
     *
     * @param result the metadata result returned by the Kafka broker
     * @param status the operational status description
     * @return structured {@link PublishResponse} with topic, partition, offset, and timestamp
     */
    private PublishResponse mapToSendResponse(SendResult<String, OrderEvent> result, String status) {
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
