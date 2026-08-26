package com.example.springkafka.dto;

import java.time.Instant;

public record AuditRecord(
        String topic,
        int partition,
        long offset,
        String key,
        OrderEvent payload,
        String consumerGroup,
        String processedAt,
        String status
) {
    public static AuditRecord of(String topic, int partition, long offset, String key, OrderEvent payload, String consumerGroup, String status) {
        return new AuditRecord(topic, partition, offset, key, payload, consumerGroup, Instant.now().toString(), status);
    }
}
