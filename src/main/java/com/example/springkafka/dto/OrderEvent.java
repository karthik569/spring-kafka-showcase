package com.example.springkafka.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

@Schema(description = "Represents an e-commerce order event published through Apache Kafka")
public record OrderEvent(
        @Schema(description = "Unique business identifier for the order", example = "ORD-98721")
        String orderId,

        @Schema(description = "Identifier of the customer placing the order", example = "CUST-4501")
        String customerId,

        @Schema(description = "Stock Keeping Unit code", example = "SKU-LAPTOP-X1")
        String skuCode,

        @Schema(description = "Ordered item quantity", example = "2")
        int quantity,

        @Schema(description = "Unit price of the item", example = "1299.99")
        double price,

        @Schema(description = "Order priority classification (HIGH, NORMAL, LOW)", example = "HIGH", allowableValues = {"HIGH", "NORMAL", "LOW"})
        String priority,

        @Schema(description = "Synthetic test flag to simulate consumer processing failure and trigger DLT retry flow", example = "false")
        boolean simulateFailure,

        @Schema(description = "ISO-8601 creation timestamp", example = "2026-10-01T17:30:00Z")
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
