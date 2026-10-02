package com.example.springkafka.dto;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "Correlated event resulting from a real-time KStream-KStream sliding-window join between Orders and Shipments")
public record OrderShipmentEnrichedEvent(
        @Schema(description = "Correlated order business ID", example = "ORD-9901")
        String orderId,

        @Schema(description = "Customer ID", example = "CUST-4501")
        String customerId,

        @Schema(description = "SKU item code", example = "IPHONE-16")
        String skuCode,

        @Schema(description = "Total purchase value", example = "999.00")
        double totalAmount,

        @Schema(description = "Shipment tracking number", example = "TRK-FEDEX-998811")
        String trackingNumber,

        @Schema(description = "Logistics carrier", example = "FEDEX")
        String carrier,

        @Schema(description = "Order-to-shipment fulfillment duration in seconds", example = "142")
        long fulfillmentDurationSeconds,

        @Schema(description = "Join timestamp")
        String joinedAt
) {}
