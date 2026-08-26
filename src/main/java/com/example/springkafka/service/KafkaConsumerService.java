package com.example.springkafka.service;

import com.example.springkafka.dto.AuditRecord;
import com.example.springkafka.dto.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.List;
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
}
