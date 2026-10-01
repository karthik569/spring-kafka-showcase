package com.example.springkafka.dto;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "Kafka producer acknowledgment details confirming topic offset and partition assignment")
public record PublishResponse(
        @Schema(description = "Destination Kafka topic", example = "orders.standard")
        String topic,

        @Schema(description = "Assigned topic partition index", example = "1")
        int partition,

        @Schema(description = "Committed topic log offset", example = "42")
        long offset,

        @Schema(description = "Message routing key used for hashing/partitioning", example = "CUST-4501")
        String key,

        @Schema(description = "Publish status status indicator", example = "SUCCESS")
        String status,

        @Schema(description = "Timestamp when message was successfully committed by broker", example = "2026-10-01T17:30:00.123Z")
        String timestamp
) {}
