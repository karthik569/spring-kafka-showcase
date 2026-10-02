package com.example.springkafka.avro;

import com.example.springkafka.dto.OrderEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Service providing Apache Avro binary serialization and deserialization
 * against formal Avro schemas.
 * <p>
 * Demonstrates:
 * <ul>
 *   <li>Avro Schema definition for domain entities</li>
 *   <li>Compact binary serialization using {@link BinaryEncoder}</li>
 *   <li>Binary deserialization using {@link BinaryDecoder}</li>
 *   <li>Size payload comparisons against equivalent JSON structures</li>
 * </ul>
 */
@Component
public class AvroSerializerService {

    private static final Logger log = LoggerFactory.getLogger(AvroSerializerService.class);

    public static final String ORDER_AVRO_SCHEMA_JSON = """
            {
              "type": "record",
              "name": "OrderAvroRecord",
              "namespace": "com.example.springkafka.avro",
              "fields": [
                {"name": "orderId", "type": "string"},
                {"name": "customerId", "type": "string"},
                {"name": "skuCode", "type": "string"},
                {"name": "quantity", "type": "int"},
                {"name": "price", "type": "double"},
                {"name": "priority", "type": "string"},
                {"name": "timestamp", "type": "string"}
              ]
            }
            """;

    private final Schema schema;

    public AvroSerializerService() {
        this.schema = new Schema.Parser().parse(ORDER_AVRO_SCHEMA_JSON);
    }

    public Schema getSchema() {
        return schema;
    }

    /**
     * Serializes an {@link OrderEvent} into raw Apache Avro binary bytes.
     */
    public byte[] serializeToAvro(OrderEvent event) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("orderId", event.orderId());
        record.put("customerId", event.customerId() != null ? event.customerId() : "UNKNOWN");
        record.put("skuCode", event.skuCode() != null ? event.skuCode() : "N/A");
        record.put("quantity", event.quantity());
        record.put("price", event.price());
        record.put("priority", event.priority() != null ? event.priority() : "NORMAL");
        record.put("timestamp", event.timestamp() != null ? event.timestamp() : java.time.Instant.now().toString());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);

        try {
            writer.write(record, encoder);
            encoder.flush();
            out.close();
            byte[] bytes = out.toByteArray();
            log.debug("[AVRO-SERIALIZER] Serialized orderId={} to {} Avro binary bytes", event.orderId(), bytes.length);
            return bytes;
        } catch (IOException e) {
            throw new RuntimeException("Avro serialization failure", e);
        }
    }

    /**
     * Deserializes raw Apache Avro binary bytes back into a domain {@link OrderEvent}.
     */
    public OrderEvent deserializeFromAvro(byte[] avroBytes) {
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(avroBytes, null);

        try {
            GenericRecord record = reader.read(null, decoder);
            return new OrderEvent(
                    record.get("orderId").toString(),
                    record.get("customerId").toString(),
                    record.get("skuCode").toString(),
                    (Integer) record.get("quantity"),
                    (Double) record.get("price"),
                    record.get("priority").toString(),
                    false,
                    record.get("timestamp").toString()
            );
        } catch (IOException e) {
            throw new RuntimeException("Avro deserialization failure", e);
        }
    }
}
