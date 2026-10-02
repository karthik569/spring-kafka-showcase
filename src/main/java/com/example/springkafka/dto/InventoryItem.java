package com.example.springkafka.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

@Schema(description = "Represents inventory item state for log compaction testing")
public record InventoryItem(
        @Schema(description = "Stock Keeping Unit item key", example = "SKU-LAPTOP-X1")
        String skuCode,

        @Schema(description = "Item descriptive name", example = "Ultrabook Pro 16")
        String itemName,

        @Schema(description = "Current available warehouse stock quantity. Negative or 0 indicates depleted stock.", example = "150")
        int stockQuantity,

        @Schema(description = "Warehouse location code", example = "WH-US-EAST-1")
        String warehouseCode,

        @Schema(description = "Item status (ACTIVE, DISCONTINUED, DELETED)", example = "ACTIVE")
        String status,

        @Schema(description = "Last updated timestamp", example = "2026-10-02T01:30:00Z")
        String lastUpdated
) {
    public InventoryItem withTimestamp() {
        return new InventoryItem(
                skuCode,
                itemName,
                stockQuantity,
                warehouseCode,
                status,
                Instant.now().toString()
        );
    }
}
