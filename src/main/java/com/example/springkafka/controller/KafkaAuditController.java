package com.example.springkafka.controller;

import com.example.springkafka.dto.AuditRecord;
import com.example.springkafka.dto.InventoryItem;
import com.example.springkafka.dto.StreamAnalytics;
import com.example.springkafka.service.KafkaConsumerService;
import com.example.springkafka.service.OrderStreamsService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@Tag(name = "Kafka Audit API", description = "Endpoints to inspect messages consumed by Kafka listeners and monitor Dead Letter Topics")
@RestController
@RequestMapping("/api/kafka/audit")
public class KafkaAuditController {

    private final KafkaConsumerService consumerService;
    private final OrderStreamsService streamsService;
    private final com.example.springkafka.avro.SchemaRegistryService schemaRegistryService;
    private final com.example.springkafka.service.OutboxCdcService outboxService;

    public KafkaAuditController(
            KafkaConsumerService consumerService,
            OrderStreamsService streamsService,
            com.example.springkafka.avro.SchemaRegistryService schemaRegistryService,
            com.example.springkafka.service.OutboxCdcService outboxService) {
        this.consumerService = consumerService;
        this.streamsService = streamsService;
        this.schemaRegistryService = schemaRegistryService;
        this.outboxService = outboxService;
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
            description = "Returns quantitative metrics showing total received and total dead-letter counts across all topics."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Summary statistics object",
                    content = @Content(mediaType = "application/json", schema = @Schema(example = "{\"totalReceived\": 12, \"totalDeadLetter\": 2}")))
    })
    @GetMapping("/summary")
    public Map<String, Object> getSummary() {
        return Map.of(
                "totalReceived", consumerService.getReceivedRecords().size(),
                "totalDeadLetter", consumerService.getDltRecords().size(),
                "totalNonblocking", consumerService.getNonblockingRecords().size(),
                "totalManualAck", consumerService.getManualAckRecords().size(),
                "inventoryItemsCount", consumerService.getInventoryState().size(),
                "streamsState", streamsService.getStreamsState()
        );
    }

    @Operation(
            summary = "Get non-blocking retry records",
            description = "Inspects processed records and retry exhaustion events on 'orders.nonblocking'."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "List of non-blocking audit records",
                    content = @Content(mediaType = "application/json", array = @ArraySchema(schema = @Schema(implementation = AuditRecord.class))))
    })
    @GetMapping("/nonblocking")
    public List<AuditRecord> getNonblockingRecords() {
        return consumerService.getNonblockingRecords();
    }

    @Operation(
            summary = "Get manual acknowledgment audit records",
            description = "Inspects manual commits and nack re-deliveries handled via AckMode.MANUAL_IMMEDIATE."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "List of manual ack audit records",
                    content = @Content(mediaType = "application/json", array = @ArraySchema(schema = @Schema(implementation = AuditRecord.class))))
    })
    @GetMapping("/manual-ack")
    public List<AuditRecord> getManualAckRecords() {
        return consumerService.getManualAckRecords();
    }

    @Operation(
            summary = "Get current compacted inventory table",
            description = "Returns the latest state of warehouse inventory derived from the compacted topic 'inventory.compacted'."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Current compacted inventory map",
                    content = @Content(mediaType = "application/json"))
    })
    @GetMapping("/inventory")
    public Map<String, InventoryItem> getInventoryState() {
        return consumerService.getInventoryState();
    }

    @Operation(
            summary = "Get all customer analytics from Kafka Streams topology",
            description = "Interactive query against the state store materialized by Kafka Streams topology."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "List of customer analytics",
                    content = @Content(mediaType = "application/json", array = @ArraySchema(schema = @Schema(implementation = StreamAnalytics.class))))
    })
    @GetMapping("/streams/analytics")
    public List<StreamAnalytics> getAllStreamsAnalytics() {
        return streamsService.getAllAnalytics();
    }

    @Operation(
            summary = "Get customer analytics by ID from Kafka Streams",
            description = "Point query on local materialized key-value store for customer metrics."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Customer analytics found",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = StreamAnalytics.class))),
            @ApiResponse(responseCode = "404", description = "Customer not found in state store")
    })
    @GetMapping("/streams/analytics/{customerId}")
    public ResponseEntity<StreamAnalytics> getCustomerStreamsAnalytics(
            @Parameter(description = "Customer ID", example = "CUST-4501")
            @PathVariable String customerId) {
        return streamsService.getAnalyticsForCustomer(customerId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @Operation(
            summary = "Get 1-minute windowed analytics from Kafka Streams",
            description = "Interactive query fetching 1-minute tumbling window metrics aggregated by Kafka Streams."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "List of windowed customer analytics",
                    content = @Content(mediaType = "application/json", array = @ArraySchema(schema = @Schema(implementation = StreamAnalytics.class))))
    })
    @GetMapping("/streams/analytics/windowed")
    public List<StreamAnalytics> getWindowedStreamsAnalytics() {
        return streamsService.getAllWindowedAnalytics();
    }

    @Operation(
            summary = "Get deserialized Apache Avro records",
            description = "Inspects messages ingested from 'orders.avro' that were decoded from binary Avro format."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "List of Avro deserialized audit records",
                    content = @Content(mediaType = "application/json", array = @ArraySchema(schema = @Schema(implementation = AuditRecord.class))))
    })
    @GetMapping("/avro")
    public List<AuditRecord> getAvroRecords() {
        return consumerService.getAvroRecords();
    }

    @Operation(
            summary = "Get official Apache Avro schema specification",
            description = "Returns the canonical JSON representation of the OrderAvroRecord schema."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Canonical Avro schema JSON string",
                    content = @Content(mediaType = "application/json"))
    })
    @GetMapping("/avro/schema")
    public String getAvroSchema() {
        return com.example.springkafka.avro.AvroSerializerService.ORDER_AVRO_SCHEMA_JSON;
    }

    @Operation(
            summary = "Get all registered Schema Registry subjects and versions",
            description = "Lists all schemas registered in the Schema Registry simulator, along with version and compatibility metadata."
    )
    @GetMapping("/schema-registry/subjects")
    public Map<String, List<com.example.springkafka.avro.SchemaRegistryService.SchemaMetadata>> getSchemaRegistrySubjects() {
        return schemaRegistryService.getAllSubjects();
    }

    @Operation(
            summary = "Register new schema version under a subject",
            description = "Registers an Avro schema version under a subject (e.g. 'orders.avro-value') with evolution compatibility enforcement."
    )
    @org.springframework.web.bind.annotation.PostMapping("/schema-registry/subjects/{subject}/versions")
    public com.example.springkafka.avro.SchemaRegistryService.SchemaMetadata registerSchema(
            @PathVariable String subject,
            @org.springframework.web.bind.annotation.RequestBody String schemaJson) {
        return schemaRegistryService.registerSchema(subject, schemaJson);
    }

    @Operation(
            summary = "Get CDC Outbox audit trail",
            description = "Returns all committed outbox events captured from local transactions and streamed to Kafka via CDC."
    )
    @GetMapping("/outbox-cdc")
    public List<com.example.springkafka.dto.OutboxRecord> getOutboxRecords() {
        return outboxService.getAllOutboxRecords();
    }

    @Operation(
            summary = "Get real-time joined stream enrichments (KStream - KTable)",
            description = "Returns enriched orders correlated with live inventory state in the streaming topology."
    )
    @GetMapping("/streams/joined")
    public List<com.example.springkafka.dto.EnrichedOrderEvent> getJoinedEnrichments() {
        return streamsService.getAllJoinedEnrichments();
    }

    @Operation(
            summary = "Get decrypted PII audit trail (Field-Level Encryption)",
            description = "Returns order records consumed from 'orders.encrypted' with customerId decrypted by the consumer."
    )
    @GetMapping("/encrypted")
    public List<AuditRecord> getEncryptedAudit() {
        return consumerService.getEncryptedRecords();
    }

    @Operation(
            summary = "Get tiered / long-term retention topic messages",
            description = "Returns records ingested from 'orders.tiered.retention' designed for long-term cold storage."
    )
    @GetMapping("/tiered")
    public List<AuditRecord> getTieredAudit() {
        return consumerService.getTieredRecords();
    }
}
