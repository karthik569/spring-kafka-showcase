package com.example.springkafka.dto;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class OrderEventSerializationTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testOrderEventSerialization() throws Exception {
        OrderEvent event = new OrderEvent(
                "ORD-999", "CUST-1", "MACBOOK", 1, 1999.99, "HIGH", false, null
        ).withTimestamp();

        String json = objectMapper.writeValueAsString(event);
        assertNotNull(json);
        assertTrue(json.contains("ORD-999"));
        assertTrue(json.contains("MACBOOK"));
        assertTrue(json.contains("HIGH"));

        OrderEvent deserialized = objectMapper.readValue(json, OrderEvent.class);
        assertEquals(event.orderId(), deserialized.orderId());
        assertEquals(event.customerId(), deserialized.customerId());
        assertEquals(event.price(), deserialized.price());
    }

    @Test
    void testAuditRecordSerialization() throws Exception {
        OrderEvent event = new OrderEvent("ORD-1", "CUST-1", "IPHONE", 1, 999.0, "NORMAL", false, "2026-08-16T12:00:00Z");
        AuditRecord record = AuditRecord.of("orders.standard", 0, 42L, "key-1", event, "test-group", "PROCESSED");

        String json = objectMapper.writeValueAsString(record);
        assertNotNull(json);
        assertTrue(json.contains("orders.standard"));
        assertTrue(json.contains("PROCESSED"));

        AuditRecord deserialized = objectMapper.readValue(json, AuditRecord.class);
        assertEquals(record.topic(), deserialized.topic());
        assertEquals(record.offset(), deserialized.offset());
    }
}
