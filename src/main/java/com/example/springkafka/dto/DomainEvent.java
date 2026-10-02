package com.example.springkafka.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

@Schema(description = "Represents an immutable domain event in an Event-Sourced architecture")
public record DomainEvent(
        @Schema(description = "Unique event identifier", example = "EVT-8812")
        String eventId,

        @Schema(description = "Aggregate type (e.g. ORDER, PAYMENT, SHIPMENT)", example = "ORDER")
        String aggregateType,

        @Schema(description = "Aggregate business ID", example = "ORD-9901")
        String aggregateId,

        @Schema(description = "Event type name", example = "ORDER_CREATED", allowableValues = {"ORDER_CREATED", "PAYMENT_RESERVED", "ORDER_SHIPPED", "ORDER_CANCELLED"})
        String eventType,

        @Schema(description = "Sequential event version for aggregate rehydration", example = "1")
        int version,

        @Schema(description = "Event payload in JSON format")
        String payloadJson,

        @Schema(description = "ISO-8601 timestamp when the event occurred")
        String timestamp
) {
    public static DomainEvent of(String aggregateType, String aggregateId, String eventType, int version, String payloadJson) {
        return new DomainEvent(
                "EVT-" + java.util.UUID.randomUUID().toString().substring(0, 8),
                aggregateType,
                aggregateId,
                eventType,
                version,
                payloadJson,
                Instant.now().toString()
        );
    }
}
