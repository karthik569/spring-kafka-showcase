package com.example.springkafka.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

@Schema(description = "User browsing activity event used for Kafka Streams Session Window aggregation")
public record UserSessionEvent(
        @Schema(description = "Session identifier or cookie token", example = "SESS-8812-XYZ")
        String sessionId,

        @Schema(description = "User or visitor ID", example = "USER-5501")
        String userId,

        @Schema(description = "Action performed", example = "VIEW_PRODUCT", allowableValues = {"VIEW_PRODUCT", "ADD_TO_CART", "CHECKOUT_INIT", "SEARCH"})
        String action,

        @Schema(description = "Page or endpoint URL", example = "/products/electronics/iphone-16")
        String pageUrl,

        @Schema(description = "Event occurrence timestamp")
        String timestamp
) {
    public static UserSessionEvent of(String sessionId, String userId, String action, String pageUrl) {
        return new UserSessionEvent(sessionId, userId, action, pageUrl, Instant.now().toString());
    }
}
