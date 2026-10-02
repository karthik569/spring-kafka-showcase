package com.example.springkafka.service;

import com.example.springkafka.dto.DomainEvent;
import com.example.springkafka.dto.OrderEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Service implementing Event Sourcing and CQRS (Command Query Responsibility Segregation).
 * <p>
 * Demonstrates:
 * <ul>
 *   <li>Immutable, append-only Event Store for aggregate state mutations</li>
 *   <li>Command-side event creation and dispatch to {@code events.sourced}</li>
 *   <li>Query-side read projection materialization from event streams</li>
 *   <li>Time travel / state rehydration by replaying historical events from offset 0</li>
 * </ul>
 */
@Service
public class EventSourcingService {

    private static final Logger log = LoggerFactory.getLogger(EventSourcingService.class);

    private final KafkaProducerService producerService;
    private final ObjectMapper objectMapper;

    // Simulated Event Store: aggregateId -> ordered list of immutable DomainEvents
    private final Map<String, List<DomainEvent>> eventStore = new ConcurrentHashMap<>();

    // Materialized Read Projection (CQRS View): aggregateId -> current aggregate state
    private final Map<String, Map<String, Object>> readModelProjections = new ConcurrentHashMap<>();

    public EventSourcingService(KafkaProducerService producerService, ObjectMapper objectMapper) {
        this.producerService = producerService;
        this.objectMapper = objectMapper;
    }

    /**
     * Appends a new domain event to the aggregate's event stream and dispatches it to Kafka.
     */
    public synchronized DomainEvent appendEvent(String aggregateType, String aggregateId, String eventType, Object payload) {
        List<DomainEvent> stream = eventStore.computeIfAbsent(aggregateId, k -> new CopyOnWriteArrayList<>());
        int nextVersion = stream.size() + 1;

        try {
            String payloadJson = objectMapper.writeValueAsString(payload);
            DomainEvent event = DomainEvent.of(aggregateType, aggregateId, eventType, nextVersion, payloadJson);
            stream.add(event);

            // Apply mutation to local read model projection
            applyToProjection(aggregateId, eventType, payload);

            log.info("[EVENT-SOURCING] Appended eventId={} type={} version={} to aggregateId={}",
                    event.eventId(), eventType, nextVersion, aggregateId);

            // Stream to Kafka append-only topic
            producerService.sendAsyncRaw("events.sourced", aggregateId, event);

            return event;
        } catch (Exception e) {
            log.error("[EVENT-SOURCING] Failed to append event: {}", e.getMessage());
            throw new RuntimeException("Event sourcing persistence failure", e);
        }
    }

    /**
     * Rehydrates an aggregate by replaying its complete event stream from version 1.
     */
    public Map<String, Object> rehydrateAggregate(String aggregateId) {
        List<DomainEvent> history = eventStore.getOrDefault(aggregateId, Collections.emptyList());
        Map<String, Object> state = new LinkedHashMap<>();
        state.put("aggregateId", aggregateId);
        state.put("replayedEventCount", history.size());

        for (DomainEvent event : history) {
            state.put("lastEventType", event.eventType());
            state.put("version", event.version());
            state.put("lastTimestamp", event.timestamp());
        }

        log.info("[EVENT-SOURCING] Rehydrated aggregateId={} by replaying {} events", aggregateId, history.size());
        return state;
    }

    private void applyToProjection(String aggregateId, String eventType, Object payload) {
        Map<String, Object> projection = readModelProjections.computeIfAbsent(aggregateId, k -> new ConcurrentHashMap<>());
        projection.put("aggregateId", aggregateId);
        projection.put("status", eventType);
        projection.put("lastUpdated", java.time.Instant.now().toString());
        if (payload instanceof OrderEvent order) {
            projection.put("orderId", order.orderId());
            projection.put("customerId", order.customerId());
            projection.put("totalAmount", order.price() * order.quantity());
            projection.put("priority", order.priority());
        }
    }

    public List<DomainEvent> getEventStream(String aggregateId) {
        return eventStore.getOrDefault(aggregateId, Collections.emptyList());
    }

    public Map<String, Map<String, Object>> getAllProjections() {
        return Collections.unmodifiableMap(readModelProjections);
    }
}
