package com.example.springkafka.dto;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "Aggregated customer order analytics computed by Kafka Streams topology")
public record StreamAnalytics(
        @Schema(description = "Customer ID partition key", example = "CUST-4501")
        String customerId,

        @Schema(description = "Total number of orders processed in real time", example = "3")
        long orderCount,

        @Schema(description = "Cumulative total spend across all orders", example = "3899.97")
        double totalSpend,

        @Schema(description = "Average order value", example = "1299.99")
        double averageSpend,

        @Schema(description = "Timestamp of the latest computed aggregation", example = "2026-10-02T01:35:00Z")
        String lastAggregatedAt
) {}
