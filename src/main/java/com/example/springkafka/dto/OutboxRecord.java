package com.example.springkafka.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

@Schema(description = "Represents an Outbox Table record captured from local ACID database transactions for reliable streaming (CDC)")
public record OutboxRecord(
        @Schema(description = "Outbox event unique ID", example = "OUTBOX-001")
        String id,

        @Schema(description = "Aggregate type (e.g. ORDER, CUSTOMER, PAYMENT)", example = "ORDER")
        String aggregateType,

        @Schema(description = "Aggregate business key", example = "ORD-98721")
        String aggregateId,

        @Schema(description = "Event type", example = "ORDER_CREATED")
        String eventType,

        @Schema(description = "Target destination Kafka topic", example = "orders.outbox.cdc")
        String targetTopic,

        @Schema(description = "Serialized JSON payload")
        String payloadJson,

        @Schema(description = "Outbox status: PENDING, DISPATCHED, FAILED", example = "DISPATCHED")
        String status,

        @Schema(description = "Timestamp when outbox record was created")
        String createdAt
) {
    public static OutboxRecord create(String id, String aggregateType, String aggregateId, String eventType, String targetTopic, String payloadJson) {
        return new OutboxRecord(id, aggregateType, aggregateId, eventType, targetTopic, payloadJson, "PENDING", Instant.now().toString());
    }

    public OutboxRecord withStatus(String newStatus) {
        return new OutboxRecord(id, aggregateType, aggregateId, eventType, targetTopic, payloadJson, newStatus, createdAt);
    }
}
