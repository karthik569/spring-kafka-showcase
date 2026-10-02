package com.example.springkafka.service;

import com.example.springkafka.dto.AuditRecord;
import com.example.springkafka.dto.InventoryItem;
import com.example.springkafka.dto.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Service encapsulating multiple Apache Kafka listener patterns.
 * <p>
 * Demonstrates:
 * <ul>
 *   <li>Standard single-record consumer with partition/offset metadata injection</li>
 *   <li>Filter-backed consumer discarding non-matching messages upstream</li>
 *   <li>High-throughput batch consumer processing lists of {@link ConsumerRecord}</li>
 *   <li>Resilient consumer triggering automatic error handling and Dead Letter Topic (DLT) publishing</li>
 *   <li>DLT audit monitor consumer</li>
 * </ul>
 *
 * @author Spring Showcase Team
 * @version 1.0
 */
@Service
public class KafkaConsumerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);

    private final List<AuditRecord> receivedRecords = new CopyOnWriteArrayList<>();
    private final List<AuditRecord> dltRecords = new CopyOnWriteArrayList<>();
    private final Map<String, InventoryItem> inventoryState = new ConcurrentHashMap<>();
    private final List<AuditRecord> nonblockingRecords = new CopyOnWriteArrayList<>();
    private final List<AuditRecord> manualAckRecords = new CopyOnWriteArrayList<>();
    private final List<AuditRecord> avroRecords = new CopyOnWriteArrayList<>();
    private final List<AuditRecord> outboxRecords = new CopyOnWriteArrayList<>();
    private final List<com.example.springkafka.dto.EnrichedOrderEvent> joinedRecords = new CopyOnWriteArrayList<>();
    private final List<AuditRecord> encryptedRecords = new CopyOnWriteArrayList<>();
    private final List<AuditRecord> tieredRecords = new CopyOnWriteArrayList<>();

    private final com.example.springkafka.avro.AvroSerializerService avroSerializer;
    private final com.example.springkafka.security.FieldEncryptionService encryptionService;

    public KafkaConsumerService(
            com.example.springkafka.avro.AvroSerializerService avroSerializer,
            com.example.springkafka.security.FieldEncryptionService encryptionService) {
        this.avroSerializer = avroSerializer;
        this.encryptionService = encryptionService;
    }

    public KafkaConsumerService() {
        this.avroSerializer = new com.example.springkafka.avro.AvroSerializerService();
        this.encryptionService = new com.example.springkafka.security.FieldEncryptionService();
    }

    /**
     * Standard point-to-point Kafka listener ingesting single order events.
     * <p>
     * Demonstrates header extraction for partition ID, topic offset, and partition key,
     * and records successfully processed events into the thread-safe audit store.
     *
     * @param event     the deserialized {@link OrderEvent} payload
     * @param partition the zero-based partition number assigned by Kafka
     * @param offset    the sequential offset index within the partition
     * @param topic     the name of the topic from which the record was read
     * @param key       the partition/message routing key (optional)
     */
    @KafkaListener(topics = "orders.standard", groupId = "standard-consumers-group")
    public void consumeStandard(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key) {

        log.info("[KAFKA-CONSUMER-STANDARD] Consumed orderId={} from topic={} partition={} offset={}",
                event.orderId(), topic, partition, offset);

        receivedRecords.add(AuditRecord.of(topic, partition, offset, key, event, "standard-consumers-group", "PROCESSED"));
    }

    /**
     * Filtered VIP Kafka consumer utilizing a custom container factory.
     * <p>
     * Uses {@code filterContainerFactory} configured with {@code RecordFilterStrategy}
     * so that any order without {@code priority == "HIGH"} is silently dropped before method invocation.
     *
     * @param event     the high-priority {@link OrderEvent}
     * @param partition the source partition
     * @param offset    the message offset
     * @param topic     the source topic
     * @param key       the routing key
     */
    @KafkaListener(topics = "orders.high-priority", groupId = "vip-orders-group", containerFactory = "filterContainerFactory")
    public void consumeHighPriority(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key) {

        log.info("[KAFKA-CONSUMER-FILTERED] Processing HIGH priority orderId={} from partition={}", event.orderId(), partition);
        receivedRecords.add(AuditRecord.of(topic, partition, offset, key, event, "vip-orders-group", "HIGH_PRIORITY_PROCESSED"));
    }

    /**
     * High-throughput batch consumer processing records in bulk.
     * <p>
     * Reduces thread context switching and I/O polling overhead by receiving a complete
     * {@link List} of {@link ConsumerRecord} instances per poll interval.
     *
     * @param records the collection of raw Kafka consumer records received in the current poll
     */
    @KafkaListener(topics = "orders.batch", groupId = "batch-consumers-group", containerFactory = "batchContainerFactory")
    public void consumeBatch(List<ConsumerRecord<String, OrderEvent>> records) {
        log.info("[KAFKA-CONSUMER-BATCH] Received batch of {} records", records.size());

        for (ConsumerRecord<String, OrderEvent> record : records) {
            log.info("  -> Batch item key={} orderId={} partition={} offset={}",
                    record.key(), record.value().orderId(), record.partition(), record.offset());
            receivedRecords.add(AuditRecord.of(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key(),
                    record.value(),
                    "batch-consumers-group",
                    "BATCH_PROCESSED"
            ));
        }
    }

    /**
     * Resilient Kafka consumer with simulated error triggering.
     * <p>
     * When {@code event.simulateFailure()} is true, deliberately throws a {@link RuntimeException}.
     * This activates Spring Kafka's {@code DefaultErrorHandler}, executing retry attempts with backoff
     * before delegating to {@code DeadLetterPublishingRecoverer} to forward the poisoned record to {@code orders.retryable.DLT}.
     *
     * @param event     the order event payload
     * @param partition the source partition
     * @param offset    the message offset
     * @param topic     the source topic
     * @param key       the partition key
     * @throws RuntimeException when synthetic failure simulation is requested
     */
    @KafkaListener(topics = "orders.retryable", groupId = "retryable-orders-group")
    public void consumeRetryable(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key) {

        log.info("[KAFKA-CONSUMER-RETRYABLE] Attempting to process orderId={} (simulateFailure={})",
                event.orderId(), event.simulateFailure());

        if (event.simulateFailure()) {
            log.error("[KAFKA-CONSUMER-RETRYABLE] Simulation flag triggered! Throwing RuntimeException for orderId={}", event.orderId());
            throw new RuntimeException("Simulated Processing Error for orderId=" + event.orderId());
        }

        receivedRecords.add(AuditRecord.of(topic, partition, offset, key, event, "retryable-orders-group", "PROCESSED"));
    }

    /**
     * Dead Letter Topic (DLT) audit consumer.
     * <p>
     * Ingests poisoned records diverted to {@code orders.retryable.DLT} after retry exhaustion,
     * recording them for post-mortem diagnostics and alert generation.
     *
     * @param failedEvent the failed {@link OrderEvent} payload
     * @param partition   the DLT partition
     * @param offset      the DLT offset
     * @param topic       the DLT topic name
     * @param key         the original routing key
     */
    @KafkaListener(topics = "orders.retryable.DLT", groupId = "dlt-monitor-group")
    public void consumeDeadLetterTopic(
            @Payload OrderEvent failedEvent,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key) {

        log.warn("[KAFKA-DLT-CONSUMER] Dead Letter received! orderId={} routed to DLT topic={} offset={}",
                failedEvent.orderId(), topic, offset);

        dltRecords.add(AuditRecord.of(topic, partition, offset, key, failedEvent, "dlt-monitor-group", "ROUTED_TO_DLT"));
    }

    /**
     * Non-blocking retry consumer using {@link RetryableTopic}.
     * <p>
     * Employs separate retry topics with exponential backoff (1s, 2s) so that failed messages
     * do not block or stall the partition for subsequent messages.
     * After max attempts, forwards to DLT.
     */
    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(delay = 1000, multiplier = 2.0),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE
    )
    @KafkaListener(topics = "orders.nonblocking", groupId = "nonblocking-orders-group")
    public void consumeNonblocking(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key) {

        log.info("[KAFKA-NONBLOCKING] Processing orderId={} topic={} offset={} simulateFailure={}",
                event.orderId(), topic, offset, event.simulateFailure());

        if (event.simulateFailure()) {
            log.error("[KAFKA-NONBLOCKING] Simulated error triggered for orderId={}. Forwarding to next retry topic!", event.orderId());
            throw new RuntimeException("Simulated Nonblocking Processing Failure for " + event.orderId());
        }

        nonblockingRecords.add(AuditRecord.of(topic, partition, offset, key, event, "nonblocking-orders-group", "PROCESSED"));
    }

    /**
     * DLT handler for {@link #consumeNonblocking}.
     */
    @DltHandler
    public void handleNonblockingDlt(
            @Payload OrderEvent failedEvent,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key) {

        log.warn("[KAFKA-NONBLOCKING-DLT] Exhausted non-blocking retries for orderId={} in topic={} offset={}",
                failedEvent.orderId(), topic, offset);

        nonblockingRecords.add(AuditRecord.of(topic, partition, offset, key, failedEvent, "nonblocking-orders-group", "DLT_EXHAUSTED"));
    }

    /**
     * Manual acknowledgment consumer utilizing {@code manualAckContainerFactory} (AckMode.MANUAL_IMMEDIATE).
     * <p>
     * Grants fine-grained programmatic control over offset commits.
     */
    @KafkaListener(
            topics = "orders.manual-ack",
            groupId = "manual-ack-group",
            containerFactory = "manualAckContainerFactory"
    )
    public void consumeManualAck(
            ConsumerRecord<String, OrderEvent> record,
            Acknowledgment acknowledgment) {

        OrderEvent event = record.value();
        log.info("[KAFKA-MANUAL-ACK] Received record key={} orderId={} simulateFailure={}",
                record.key(), event != null ? event.orderId() : "null", event != null && event.simulateFailure());

        if (event != null && event.simulateFailure()) {
            log.warn("[KAFKA-MANUAL-ACK] Processing simulated failure for orderId={}. Negative acknowledgment (nack 1000ms delay)!",
                    event.orderId());
            acknowledgment.nack(java.time.Duration.ofMillis(1000));
            manualAckRecords.add(AuditRecord.of(
                    record.topic(), record.partition(), record.offset(), record.key(), event,
                    "manual-ack-group", "NACKED_1000MS"
            ));
            return;
        }

        acknowledgment.acknowledge();
        log.info("[KAFKA-MANUAL-ACK] Offset committed immediately for record key={}", record.key());
        manualAckRecords.add(AuditRecord.of(
                record.topic(), record.partition(), record.offset(), record.key(), event,
                "manual-ack-group", "ACKNOWLEDGED_IMMEDIATE"
        ));
    }

    /**
     * Compacted Topic Consumer for Warehouse Inventory.
     * <p>
     * Demonstrates log compaction semantics:
     * - Records with non-null values update/upsert the latest known state for the SKU key.
     * - Records with null values (tombstone records) delete the key from the compacted state store.
     */
    @KafkaListener(
            topics = "inventory.compacted",
            groupId = "inventory-compacted-group"
    )
    public void consumeCompactedInventory(ConsumerRecord<String, InventoryItem> record) {
        String sku = record.key();
        InventoryItem item = record.value();

        if (item == null) {
            log.info("[KAFKA-COMPACTED] Received TOMBSTONE record for key={}. Removing from inventory state.", sku);
            inventoryState.remove(sku);
        } else {
            log.info("[KAFKA-COMPACTED] Received inventory update for sku={} stock={} status={}",
                    sku, item.stockQuantity(), item.status());
            inventoryState.put(sku, item);
        }
    }

    /**
     * Returns the thread-safe in-memory list of all successfully processed records.
     *
     * @return unmodifiable/thread-safe copy of processed {@link AuditRecord} entries
     */
    public List<AuditRecord> getReceivedRecords() {
        return receivedRecords;
    }

    /**
     * Returns the thread-safe in-memory list of all records captured on the Dead Letter Topic.
     *
     * @return thread-safe copy of dead-letter {@link AuditRecord} entries
     */
    public List<AuditRecord> getDltRecords() {
        return dltRecords;
    }

    public List<AuditRecord> getNonblockingRecords() {
        return nonblockingRecords;
    }

    public List<AuditRecord> getManualAckRecords() {
        return manualAckRecords;
    }

    /**
     * Compact binary Apache Avro consumer listener.
     */
    @KafkaListener(
            topics = "orders.avro",
            groupId = "avro-orders-group",
            containerFactory = "byteArrayContainerFactory"
    )
    public void consumeAvro(
            ConsumerRecord<String, byte[]> record,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        byte[] avroBytes = record.value();
        OrderEvent deserialized = avroSerializer.deserializeFromAvro(avroBytes);

        log.info("[KAFKA-CONSUMER-AVRO] Consumed orderId={} from topic={} partition={} offset={} (Avro payload: {} bytes)",
                deserialized.orderId(), topic, partition, offset, avroBytes.length);

        avroRecords.add(AuditRecord.of(topic, partition, offset, record.key(), deserialized, "avro-orders-group", "AVRO_DESERIALIZED"));
    }

    public List<AuditRecord> getAvroRecords() {
        return avroRecords;
    }

    public Map<String, InventoryItem> getInventoryState() {
        return inventoryState;
    }

    /**
     * Consumes Change Data Capture (CDC) events streamed from Transactional Outbox.
     */
    @KafkaListener(topics = "orders.outbox.cdc", groupId = "cdc-outbox-group")
    public void consumeOutboxCdc(
            ConsumerRecord<String, String> record,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("[KAFKA-CONSUMER-CDC] Consumed CDC outbox event key={} partition={} offset={}",
                record.key(), partition, offset);

        outboxRecords.add(new AuditRecord(
                topic,
                partition,
                offset,
                record.key(),
                new OrderEvent(record.key(), "CDC-CUST", "CDC-ITEM", 1, 0.0, "NORMAL", false, java.time.Instant.now().toString()),
                "cdc-outbox-group",
                java.time.Instant.now().toString(),
                "CDC_CAPTURED: " + record.value()
        ));
    }

    /**
     * Consumes joined real-time stream enrichment events (Order + Inventory joined).
     */
    @KafkaListener(topics = "orders.joined.output", groupId = "joined-orders-group")
    public void consumeJoinedOrder(ConsumerRecord<String, com.example.springkafka.dto.EnrichedOrderEvent> record) {
        log.info("[KAFKA-CONSUMER-JOINED] Ingested real-time enriched event key={} sku={} totalCost={}",
                record.key(), record.value().skuCode(), record.value().totalCost());
        joinedRecords.add(record.value());
    }

    /**
     * Consumes client-side encrypted records and decrypts sensitive PII fields.
     */
    @KafkaListener(topics = "orders.encrypted", groupId = "encrypted-orders-group")
    public void consumeEncrypted(
            @Payload OrderEvent encryptedEvent,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key) {

        String decryptedCustomer = encryptionService.decryptField(encryptedEvent.customerId());
        log.info("[KAFKA-CONSUMER-ENCRYPTED] Decrypted customerId: '{}' -> '{}' for orderId={}",
                encryptedEvent.customerId(), decryptedCustomer, encryptedEvent.orderId());

        OrderEvent decrypted = new OrderEvent(
                encryptedEvent.orderId(),
                decryptedCustomer,
                encryptedEvent.skuCode(),
                encryptedEvent.quantity(),
                encryptedEvent.price(),
                encryptedEvent.priority(),
                encryptedEvent.simulateFailure(),
                encryptedEvent.timestamp()
        );

        encryptedRecords.add(AuditRecord.of(topic, partition, offset, key, decrypted, "encrypted-orders-group", "PII_DECRYPTED"));
    }

    /**
     * Consumes from long-term tiered retention topic.
     */
    @KafkaListener(topics = "orders.tiered.retention", groupId = "tiered-retention-group")
    public void consumeTieredRetention(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key) {

        log.info("[KAFKA-CONSUMER-TIERED] Ingested cold/tiered storage event orderId={} offset={}", event.orderId(), offset);
        tieredRecords.add(AuditRecord.of(topic, partition, offset, key, event, "tiered-retention-group", "TIERED_STORAGE_RETAINED"));
    }

    public List<AuditRecord> getOutboxRecords() {
        return outboxRecords;
    }

    public List<com.example.springkafka.dto.EnrichedOrderEvent> getJoinedRecords() {
        return joinedRecords;
    }

    public List<AuditRecord> getEncryptedRecords() {
        return encryptedRecords;
    }

    public List<AuditRecord> getTieredRecords() {
        return tieredRecords;
    }
}
