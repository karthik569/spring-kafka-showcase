package com.example.springkafka.dto;

import java.time.Instant;

public record OrderEvent(
        String orderId,
        String customerId,
        String skuCode,
        int quantity,
        double price,
        String priority,     // HIGH, NORMAL, LOW
        boolean simulateFailure,
        String timestamp
) {
    public OrderEvent withTimestamp() {
        return new OrderEvent(
                orderId,
                customerId,
                skuCode,
                quantity,
                price,
                priority,
                simulateFailure,
                Instant.now().toString()
        );
    }
}
