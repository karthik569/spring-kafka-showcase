package com.example.springkafka.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.List;

@Schema(description = "Represents the state of a distributed multi-service checkout Saga")
public record SagaInstance(
        @Schema(description = "Unique Saga correlation transaction ID", example = "SAGA-TX-1001")
        String sagaId,

        @Schema(description = "Business Order ID", example = "ORD-9901")
        String orderId,

        @Schema(description = "Customer ID", example = "CUST-4501")
        String customerId,

        @Schema(description = "Order total amount", example = "1299.99")
        double amount,

        @Schema(description = "Current Saga lifecycle status", example = "COMPLETED", allowableValues = {"STARTED", "PAYMENT_PENDING", "INVENTORY_PENDING", "COMPLETED", "COMPENSATING", "COMPENSATED", "FAILED"})
        String status,

        @Schema(description = "Audit trail of executed steps and compensations")
        List<String> stepHistory,

        @Schema(description = "Latest status update timestamp")
        String lastUpdated
) {
    public static SagaInstance start(String sagaId, String orderId, String customerId, double amount) {
        return new SagaInstance(
                sagaId,
                orderId,
                customerId,
                amount,
                "STARTED",
                new java.util.concurrent.CopyOnWriteArrayList<>(List.of("Saga started for order " + orderId)),
                Instant.now().toString()
        );
    }

    public SagaInstance addStep(String step, String newStatus) {
        stepHistory.add(step + " at " + Instant.now().toString());
        return new SagaInstance(sagaId, orderId, customerId, amount, newStatus, stepHistory, Instant.now().toString());
    }
}
