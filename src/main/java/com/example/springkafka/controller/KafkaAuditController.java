package com.example.springkafka.controller;

import com.example.springkafka.dto.AuditRecord;
import com.example.springkafka.service.KafkaConsumerService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@Tag(name = "Kafka Audit API", description = "Endpoints to inspect messages consumed by Kafka listeners and monitor Dead Letter Topics")
@RestController
@RequestMapping("/api/kafka/audit")
public class KafkaAuditController {

    private final KafkaConsumerService consumerService;

    public KafkaAuditController(KafkaConsumerService consumerService) {
        this.consumerService = consumerService;
    }

    @Operation(
            summary = "Get all processed messages",
            description = "Returns the historical audit trail of all order events ingested by standard and priority consumer listeners."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "List of processed audit records",
                    content = @Content(mediaType = "application/json", array = @ArraySchema(schema = @Schema(implementation = AuditRecord.class))))
    })
    @GetMapping("/received")
    public List<AuditRecord> getReceivedMessages() {
        return consumerService.getReceivedRecords();
    }

    @Operation(
            summary = "Get Dead-Letter-Topic (DLT) messages",
            description = "Returns all failed messages that exhausted retry attempts and were diverted to 'orders.retryable.DLT'."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "List of poisoned messages in DLT",
                    content = @Content(mediaType = "application/json", array = @ArraySchema(schema = @Schema(implementation = AuditRecord.class))))
    })
    @GetMapping("/dlt")
    public List<AuditRecord> getDeadLetterMessages() {
        return consumerService.getDltRecords();
    }

    @Operation(
            summary = "Get audit statistics summary",
            description = "Returns quantitative metrics showing total received and total dead-letter counts."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Summary statistics object",
                    content = @Content(mediaType = "application/json", schema = @Schema(example = "{\"totalReceived\": 12, \"totalDeadLetter\": 2}")))
    })
    @GetMapping("/summary")
    public Map<String, Object> getSummary() {
        return Map.of(
                "totalReceived", consumerService.getReceivedRecords().size(),
                "totalDeadLetter", consumerService.getDltRecords().size()
        );
    }
}
