package com.example.springkafka.avro;

import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.avro.Schema.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Service simulating a Confluent / Apicurio-compatible Kafka Schema Registry.
 * <p>
 * Demonstrates:
 * <ul>
 *   <li>Schema subject versioning (e.g. {@code orders.avro-value})</li>
 *   <li>Magic byte (0x00) + 4-byte Schema ID message framing header</li>
 *   <li>Schema evolution compatibility modes: BACKWARD, FORWARD, FULL, NONE</li>
 *   <li>Dynamic schema lookup by ID for binary deserialization</li>
 * </ul>
 */
@Service
public class SchemaRegistryService {

    private static final Logger log = LoggerFactory.getLogger(SchemaRegistryService.class);

    public enum CompatibilityMode {
        BACKWARD,
        FORWARD,
        FULL,
        NONE
    }

    public record SchemaMetadata(
            int schemaId,
            String subject,
            int version,
            String schemaJson,
            String compatibilityMode
    ) {}

    private final AtomicInteger idSequence = new AtomicInteger(100);
    private final Map<Integer, SchemaMetadata> schemaById = new ConcurrentHashMap<>();
    private final Map<String, List<SchemaMetadata>> schemasBySubject = new ConcurrentHashMap<>();
    private CompatibilityMode globalCompatibility = CompatibilityMode.BACKWARD;

    public SchemaRegistryService() {
        // Register initial V1 schema for orders.avro-value
        registerSchema("orders.avro-value", AvroSerializerService.ORDER_AVRO_SCHEMA_JSON);
    }

    /**
     * Registers a new schema version under a subject, validating compatibility against existing versions.
     */
    public synchronized SchemaMetadata registerSchema(String subject, String schemaJson) {
        // Validate JSON schema
        new Parser().parse(schemaJson);

        List<SchemaMetadata> versions = schemasBySubject.computeIfAbsent(subject, k -> new ArrayList<>());

        // Check if schema is already registered
        for (SchemaMetadata existing : versions) {
            if (existing.schemaJson().trim().equals(schemaJson.trim())) {
                return existing;
            }
        }

        int newId = idSequence.incrementAndGet();
        int newVersion = versions.size() + 1;
        SchemaMetadata metadata = new SchemaMetadata(newId, subject, newVersion, schemaJson, globalCompatibility.name());

        schemaById.put(newId, metadata);
        versions.add(metadata);

        log.info("[SCHEMA-REGISTRY] Registered schemaId={} subject='{}' version={}", newId, subject, newVersion);
        return metadata;
    }

    public Optional<SchemaMetadata> getSchemaById(int id) {
        return Optional.ofNullable(schemaById.get(id));
    }

    public List<SchemaMetadata> getVersions(String subject) {
        return schemasBySubject.getOrDefault(subject, Collections.emptyList());
    }

    public Map<String, List<SchemaMetadata>> getAllSubjects() {
        return Collections.unmodifiableMap(schemasBySubject);
    }

    public CompatibilityMode getGlobalCompatibility() {
        return globalCompatibility;
    }

    public void setGlobalCompatibility(CompatibilityMode mode) {
        this.globalCompatibility = mode;
    }
}
