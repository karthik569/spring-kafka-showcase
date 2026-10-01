package com.example.springkafka.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

@Schema(description = "In-memory audit log entry recorded upon consumption by a Kafka consumer listener")
public record AuditRecord(
        @Schema(description = "Originating Kafka topic name", example = "orders.standard")
        String topic,

        @Schema(description = "Partition number from which the record was consumed", example = "0")
        int partition,

        @Schema(description = "Offset of the consumed record in the partition log", example = "105")
        long offset,

        @Schema(description = "Message partition key", example = "ORD-98721")
        String key,

        @Schema(description = "Deserialized JSON payload")
        OrderEvent payload,

        @Schema(description = "Consumer group ID processing the message", example = "standard-consumers-group")
        String consumerGroup,

        @Schema(description = "Instant when consumer completed processing", example = "2026-10-01T17:30:01.050Z")
        String processedAt,

        @Schema(description = "Processing lifecycle status (PROCESSED, DEAD_LETTER)", example = "PROCESSED")
        String status
) {
    public static AuditRecord of(String topic, int partition, long offset, String key, OrderEvent payload, String consumerGroup, String status) {
        return new AuditRecord(topic, partition, offset, key, payload, consumerGroup, Instant.now().toString(), status);
    }
}
