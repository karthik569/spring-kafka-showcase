package com.example.springkafka.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.UUID;

@Schema(description = "Represents W3C Trace Context (traceparent / tracestate) for distributed tracing across Kafka microservices")
public record W3cTraceContext(
        @Schema(description = "W3C Trace version", example = "00")
        String version,

        @Schema(description = "Global 128-bit distributed Trace ID (32 hex characters)", example = "4bf92f3577b34da6a3ce929d0e0e4736")
        String traceId,

        @Schema(description = "Current 64-bit Span ID (16 hex characters)", example = "00f067aa0ba902b7")
        String spanId,

        @Schema(description = "Parent Span ID", example = "5a2b1c3d4e5f6a7b")
        String parentSpanId,

        @Schema(description = "Trace flags (01 = sampled)", example = "01")
        String traceFlags,

        @Schema(description = "Current service node or component name", example = "order-producer-service")
        String serviceName,

        @Schema(description = "Timestamp when the span was recorded")
        String timestamp
) {
    public static W3cTraceContext newRoot(String serviceName) {
        String traceId = UUID.randomUUID().toString().replace("-", "");
        String spanId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        return new W3cTraceContext("00", traceId, spanId, null, "01", serviceName, java.time.Instant.now().toString());
    }

    public W3cTraceContext createChildSpan(String childServiceName) {
        String newSpanId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        return new W3cTraceContext(this.version, this.traceId, newSpanId, this.spanId, this.traceFlags, childServiceName, java.time.Instant.now().toString());
    }

    /**
     * Serializes to official W3C traceparent header string:
     * version-traceid-parentid-traceflags
     */
    public String toTraceParentHeader() {
        return String.format("%s-%s-%s-%s", version, traceId, spanId, traceFlags);
    }

    public static W3cTraceContext fromTraceParentHeader(String header, String serviceName) {
        if (header == null || !header.matches("^[0-9a-fA-F]{2}-[0-9a-fA-F]{32}-[0-9a-fA-F]{16}-[0-9a-fA-F]{2}$")) {
            return newRoot(serviceName);
        }
        String[] parts = header.split("-");
        String version = parts[0];
        String traceId = parts[1];
        String parentSpanId = parts[2];
        String flags = parts[3];
        String newSpanId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        return new W3cTraceContext(version, traceId, newSpanId, parentSpanId, flags, serviceName, java.time.Instant.now().toString());
    }
}
