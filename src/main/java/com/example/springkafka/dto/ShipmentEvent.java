package com.example.springkafka.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

@Schema(description = "Represents a warehouse shipping/fulfillment event to be joined with orders")
public record ShipmentEvent(
        @Schema(description = "Unique shipment tracking identifier", example = "SHIP-88219")
        String shipmentId,

        @Schema(description = "Associated order identifier", example = "ORD-9901")
        String orderId,

        @Schema(description = "Carrier tracking number", example = "TRK-FEDEX-998811")
        String trackingNumber,

        @Schema(description = "Logistics carrier", example = "FEDEX", allowableValues = {"FEDEX", "UPS", "DHL", "BLUE_DART"})
        String carrier,

        @Schema(description = "Estimated delivery date", example = "2026-10-05T18:00:00Z")
        String estimatedDelivery,

        @Schema(description = "Shipment status", example = "DISPATCHED")
        String status,

        @Schema(description = "Event creation timestamp")
        String timestamp
) {
    public static ShipmentEvent of(String shipmentId, String orderId, String trackingNumber, String carrier) {
        return new ShipmentEvent(
                shipmentId,
                orderId,
                trackingNumber,
                carrier,
                Instant.now().plusSeconds(86400 * 3).toString(),
                "DISPATCHED",
                Instant.now().toString()
        );
    }
}
