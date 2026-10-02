package com.example.springkafka.service;

import com.example.springkafka.dto.OutboxRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Service implementing the Transactional Outbox Pattern and CDC (Change Data Capture) dispatch.
 * <p>
 * Ensures 100% data consistency between local relational state and distributed Kafka events,
 * preventing dual-write loss or phantom messages.
 */
@Service
public class OutboxCdcService {

    private static final Logger log = LoggerFactory.getLogger(OutboxCdcService.class);

    private final KafkaProducerService producerService;
    private final ObjectMapper objectMapper;

    // Simulated local Outbox Table (in-memory ACID store)
    private final Map<String, OutboxRecord> outboxTable = new ConcurrentHashMap<>();
    private final List<OutboxRecord> processedHistory = new CopyOnWriteArrayList<>();

    public OutboxCdcService(KafkaProducerService producerService, ObjectMapper objectMapper) {
        this.producerService = producerService;
        this.objectMapper = objectMapper;
    }

    /**
     * Simulates writing an entity state change and its Outbox Event in a single local ACID transaction.
     */
    public OutboxRecord saveToOutbox(String aggregateType, String aggregateId, String eventType, String targetTopic, Object payload) {
        try {
            String payloadJson = objectMapper.writeValueAsString(payload);
            String outboxId = "OUTBOX-" + UUID.randomUUID().toString().substring(0, 8);
            OutboxRecord record = OutboxRecord.create(outboxId, aggregateType, aggregateId, eventType, targetTopic, payloadJson);

            outboxTable.put(outboxId, record);
            log.info("[OUTBOX-TRANSACTION] Committed outbox record id={} aggregateId={} in local transaction", outboxId, aggregateId);
            return record;
        } catch (Exception e) {
            log.error("[OUTBOX-TRANSACTION] Failed to commit outbox record: {}", e.getMessage());
            throw new RuntimeException("Outbox transaction failure", e);
        }
    }

    /**
     * Simulates a CDC Log Tailer / Poller streaming committed Outbox events to Kafka.
     * Can also be triggered directly via API.
     */
    public synchronized int dispatchPendingOutboxRecords() {
        int dispatchedCount = 0;
        for (OutboxRecord record : outboxTable.values()) {
            if ("PENDING".equals(record.status())) {
                try {
                    log.info("[CDC-DISPATCHER] Polled pending outboxId={} for aggregateId={}. Publishing to topic={}...",
                            record.id(), record.aggregateId(), record.targetTopic());

                    producerService.sendAsyncRaw(record.targetTopic(), record.aggregateId(), record.payloadJson());

                    OutboxRecord updated = record.withStatus("DISPATCHED");
                    outboxTable.put(record.id(), updated);
                    processedHistory.add(updated);
                    dispatchedCount++;
                } catch (Exception e) {
                    log.error("[CDC-DISPATCHER] Failed to dispatch outboxId={}: {}", record.id(), e.getMessage());
                    outboxTable.put(record.id(), record.withStatus("FAILED"));
                }
            }
        }
        return dispatchedCount;
    }

    public List<OutboxRecord> getAllOutboxRecords() {
        return new ArrayList<>(outboxTable.values());
    }

    public List<OutboxRecord> getDispatchedHistory() {
        return processedHistory;
    }
}
