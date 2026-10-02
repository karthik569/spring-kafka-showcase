package com.example.springkafka.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Service simulating Kafka Connect runtime architecture:
 * <ul>
 *   <li>Debezium Source Connector capturing relational table CDC write-ahead logs (WAL)</li>
 *   <li>Single Message Transforms (SMT) for inline PII masking and dynamic routing</li>
 *   <li>Cloud Data Lake / S3 Sink Connector streaming partitioned columnar files</li>
 * </ul>
 */
@Service
public class KafkaConnectSimulatorService {

    private static final Logger log = LoggerFactory.getLogger(KafkaConnectSimulatorService.class);

    public record CdcRecord(
            String sourceTable,
            String operation, // INSERT, UPDATE, DELETE
            Map<String, Object> beforeState,
            Map<String, Object> afterState,
            long lsnOffset,
            String timestamp
    ) {}

    public record TransformedCdcMessage(
            String topic,
            String partitionKey,
            String payloadJson,
            List<String> appliedSmts,
            String transformedAt
    ) {}

    public record LakeBatch(
            String batchId,
            String partitionPath,
            int recordCount,
            String fileFormat, // PARQUET_SNAPPY
            List<String> recordIds,
            String writtenAt
    ) {}

    private final KafkaProducerService producerService;
    private final ObjectMapper objectMapper;

    private final List<CdcRecord> rawCdcEvents = new CopyOnWriteArrayList<>();
    private final List<TransformedCdcMessage> transformedMessages = new CopyOnWriteArrayList<>();
    private final List<LakeBatch> lakeBatches = new CopyOnWriteArrayList<>();

    public KafkaConnectSimulatorService(KafkaProducerService producerService, ObjectMapper objectMapper) {
        this.producerService = producerService;
        this.objectMapper = objectMapper;
    }

    /**
     * Simulates Debezium capturing an INSERT/UPDATE in a PostgreSQL orders table,
     * applying SMTs, and publishing to Kafka.
     */
    public TransformedCdcMessage ingestTableChange(String table, String op, Map<String, Object> before, Map<String, Object> after) {
        long lsn = System.currentTimeMillis();
        CdcRecord cdc = new CdcRecord(table, op, before, after, lsn, Instant.now().toString());
        rawCdcEvents.add(cdc);

        // Single Message Transform (SMT) Pipeline:
        // 1. Mask PII fields (credit card, SSN)
        Map<String, Object> sanitizedAfter = new LinkedHashMap<>(after);
        List<String> appliedTransforms = new ArrayList<>();

        if (sanitizedAfter.containsKey("creditCardNumber")) {
            String cc = String.valueOf(sanitizedAfter.get("creditCardNumber"));
            sanitizedAfter.put("creditCardNumber", "****-****-****-" + (cc.length() >= 4 ? cc.substring(cc.length() - 4) : "0000"));
            appliedTransforms.add("MaskFieldSMT[creditCardNumber]");
        }
        if (sanitizedAfter.containsKey("email")) {
            sanitizedAfter.put("email", "anonymized@showcase.local");
            appliedTransforms.add("MaskFieldSMT[email]");
        }

        // 2. Dynamic Topic Routing SMT
        String targetTopic = "orders.cdc.raw";
        appliedTransforms.add("RegexRouterSMT[table." + table + " -> " + targetTopic + "]");

        String key = String.valueOf(after.getOrDefault("orderId", "KEY-" + lsn));

        try {
            Map<String, Object> envelope = new LinkedHashMap<>();
            envelope.put("schema", Map.of("type", "struct", "name", "cdc." + table + ".Envelope"));
            envelope.put("payload", Map.of("op", op, "ts_ms", lsn, "before", before, "after", sanitizedAfter));

            String payloadJson = objectMapper.writeValueAsString(envelope);
            TransformedCdcMessage msg = new TransformedCdcMessage(targetTopic, key, payloadJson, appliedTransforms, Instant.now().toString());
            transformedMessages.add(msg);

            // Stream to Kafka
            producerService.sendAsyncRaw(targetTopic, key, payloadJson);

            // Trigger Data Lake Sink batching
            triggerDataLakeSink(key, payloadJson);

            log.info("[KAFKA-CONNECT-CDC] Ingested table={} op={} with {} SMTs to topic={}",
                    table, op, appliedTransforms.size(), targetTopic);
            return msg;
        } catch (Exception e) {
            log.error("[KAFKA-CONNECT-CDC] Failed to transform CDC event: {}", e.getMessage());
            throw new RuntimeException("CDC transformation failure", e);
        }
    }

    private void triggerDataLakeSink(String recordId, String payloadJson) {
        LocalDate today = LocalDate.now();
        String partitionPath = String.format("/lake/year=%d/month=%02d/day=%02d",
                today.getYear(), today.getMonthValue(), today.getDayOfMonth());

        LakeBatch batch = new LakeBatch(
                "BATCH-" + UUID.randomUUID().toString().substring(0, 8),
                partitionPath,
                1,
                "PARQUET_SNAPPY",
                List.of(recordId),
                Instant.now().toString()
        );
        lakeBatches.add(batch);
        log.info("[KAFKA-CONNECT-SINK] Flushed batchId={} to lake partitionPath={}", batch.batchId(), partitionPath);
    }

    public List<CdcRecord> getRawCdcEvents() {
        return rawCdcEvents;
    }

    public List<TransformedCdcMessage> getTransformedMessages() {
        return transformedMessages;
    }

    public List<LakeBatch> getLakeBatches() {
        return lakeBatches;
    }
}
