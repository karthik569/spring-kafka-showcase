package com.example.springkafka.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

@Schema(description = "Aggregated session state materialized by Kafka Streams Session Windows based on inactivity gap")
public record UserSessionSummary(
        @Schema(description = "User or visitor ID", example = "USER-5501")
        String userId,

        @Schema(description = "Total interactions within the active session window", example = "5")
        long actionCount,

        @Schema(description = "List of distinct actions performed", example = "[\"VIEW_PRODUCT\", \"ADD_TO_CART\"]")
        List<String> actions,

        @Schema(description = "Timestamp of first event in session")
        String sessionStart,

        @Schema(description = "Timestamp of latest event in session")
        String sessionEnd,

        @Schema(description = "Active session duration in seconds", example = "184")
        long durationSeconds
) {
    public static UserSessionSummary init(String userId) {
        return new UserSessionSummary(userId, 0, List.of(), "", "", 0);
    }
}
