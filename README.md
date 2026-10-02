# Spring Boot Apache Kafka Showcase 🚀

A comprehensive, production-grade Spring Boot application demonstrating the full suite of **Apache Kafka** capabilities on **Java 21** and **Spring Boot 3.3**.

> 💡 **New to Apache Kafka?** Check out the step-by-step [Getting Started Guide](file:///sdcard/Download/termux/spring-framework-6/spring-kafka-showcase/GETTING_STARTED_WITH_KAFKA.md) for conceptual explanations, interactive tutorials, and CLI cheat sheets.
>
> 🎨 **Interactive Architecture Diagrams**: Open [`kafka-architecture-diagrams.html`](file:///sdcard/Download/termux/spring-framework-6/spring-kafka-showcase/kafka-architecture-diagrams.html) in your browser to view full Mermaid diagrams for Non-blocking Retries, Manual Ack, Kafka Streams, Log Compaction, and Transactions.

---

## 🏛 Architecture & Message Flow

```
                                 +--------------------------------------------+
                                 |         KafkaProducerController            |
                                 +---------------------+----------------------+
                                                       |
                                                       v
                                 +--------------------------------------------+
                                 |            KafkaProducerService            |
                                 |  - Async CompletableFuture callbacks       |
                                 |  - Key Partitioning & Target Partitioning  |
                                 |  - Tracing Record Headers                  |
                                 |  - Kafka Transactions                      |
                                 +---------------------+----------------------+
                                                       |
                         +-----------------------------+-----------------------------+
                         |                             |                             |
                         v                             v                             v
           +---------------------------+ +---------------------------+ +---------------------------+
           |      orders.standard      | |    orders.high-priority   | |      orders.retryable     |
           |   (Partitions: 0, 1, 2)   | |      (Partitions: 0, 1)   | |   (Partitions: 0, 1, 2)   |
           +-------------+-------------+ +-------------+-------------+ +-------------+-------------+
                         |                             |                             |
                         v                             v                             v
           +---------------------------+ +---------------------------+ +---------------------------+
           | Standard Consumer Listener| | Filtered VIP Consumer     | | Retryable Consumer        |
           | (standard-consumers-group)| | (filterContainerFactory)  | | - 2 Retries with BackOff  |
           +-------------+-------------+ +-------------+-------------+ +-------------+-------------+
                         |                             |                             | (Failure)
                         |                             |                             v
                         |                             |               +---------------------------+
                         |                             |               |     orders.retryable.DLT  |
                         |                             |               |     (Dead Letter Topic)   |
                         |                             |               +-------------+-------------+
                         |                             |                             |
                         |                             |                             v
                         |                             |               +---------------------------+
                         |                             |               | DLT Audit Consumer        |
                         |                             |               | (dlt-monitor-group)       |
                         |                             |               +-------------+-------------+
                         v                             v                             v
          +-----------------------------------------------------------------------------------------+
          |                       In-Memory Audit Store & Audit Controller                          |
          |                      (GET /api/kafka/audit/received, /dlt, /summary)                    |
          +-----------------------------------------------------------------------------------------+
```

---

## 🌟 Comprehensive Method-by-Method Breakdown

### 1. [`KafkaProducerService`](file:///sdcard/Download/termux/spring-kafka-showcase/src/main/java/com/example/springkafka/service/KafkaProducerService.java)
- **`CompletableFuture<PublishResponse> sendAsync(String topic, String key, OrderEvent event)`**: Dispatches an order asynchronously with non-blocking `.thenApply()` and `.exceptionally()` callbacks.
- **`CompletableFuture<PublishResponse> sendWithHeaders(String topic, Integer partition, String key, OrderEvent event)`**: Explicit partition targeting injecting `X-Correlation-ID` and `X-Source-Service` distributed tracing headers.
- **`boolean sendInTransaction(String topic, String key, OrderEvent... events)`**: Publishes multiple events inside `kafkaTemplate.executeInTransaction()`, guaranteeing atomic all-or-nothing delivery.
- **`CompletableFuture<PublishResponse> sendInventory(String topic, String skuCode, InventoryItem item)`**: Dispatches inventory updates or tombstone deletion records (`null` value payload) to compacted topics.

### 2. [`KafkaConsumerService`](file:///sdcard/Download/termux/spring-kafka-showcase/src/main/java/com/example/springkafka/service/KafkaConsumerService.java)
- **`void consumeStandard(...)`**: `@KafkaListener(topics = "orders.standard")`; consumes records and logs partition and offset metadata.
- **`void consumeHighPriority(...)`**: Filtered VIP consumer using `RecordFilterStrategy` to discard non-HIGH priority records upstream.
- **`void consumeBatch(List<ConsumerRecord<String, OrderEvent>> records)`**: Batch listener ingesting bulk collections for high I/O efficiency.
- **`void consumeRetryable(...)`**: Resilient consumer with simulated error triggers and automatic DLT fallback.
- **`void consumeNonblocking(...)` & `@DltHandler`**: Non-blocking retry consumer (`@RetryableTopic`) with exponential backoff across separate retry topics.
- **`void consumeManualAck(ConsumerRecord, Acknowledgment)`**: Manual offset management via `AckMode.MANUAL_IMMEDIATE` with `nack()` backpressure.
- **`void consumeCompactedInventory(ConsumerRecord)`**: Materializes compacted key-value state, removing keys upon receiving tombstone records.

### 3. [`OrderStreamsService`](file:///sdcard/Download/termux/spring-kafka-showcase/src/main/java/com/example/springkafka/service/OrderStreamsService.java)
- **Kafka Streams Aggregation Topology**: Consumes from `streams.orders.input`, groups by `customerId`, aggregates total order count, total spend, and average spend, and continuously emits to `streams.analytics.output`.
- **In-Memory Materialized State Store**: Backed by `Stores.inMemoryKeyValueStore("customer-analytics-store")` for interactive queries.

### 4. [`KafkaProducerController`](file:///sdcard/Download/termux/spring-kafka-showcase/src/main/java/com/example/springkafka/controller/KafkaProducerController.java)
- `POST /api/kafka/publish/simple`: Standard asynchronous publish
- `POST /api/kafka/publish/partitioned`: Explicit partition routing with tracing headers
- `POST /api/kafka/publish/priority`: High-priority filtered order
- `POST /api/kafka/publish/retry-dlt`: Simulated error for DLT diversion
- `POST /api/kafka/publish/batch`: High-throughput batch publishing
- `POST /api/kafka/publish/transaction`: Atomic transactional publish
- `POST /api/kafka/publish/nonblocking`: Non-blocking retries with exponential backoff
- `POST /api/kafka/publish/manual-ack`: Manual acknowledgment & backpressure nack
- `POST /api/kafka/publish/inventory`: Log compaction state upsert
- `DELETE /api/kafka/publish/inventory/tombstone/{skuCode}`: Log compaction tombstone deletion
- `POST /api/kafka/publish/streams-order`: Real-time Kafka Streams ingestion
- `POST /api/kafka/publish/avro`: Compact Apache Avro binary serialization
- `POST /api/kafka/publish/outbox-cdc`: Transactional Outbox pattern & CDC streaming
- `POST /api/kafka/publish/encrypted`: Client-side AES envelope encryption for sensitive PII
- `POST /api/kafka/publish/joined-enrichment`: Real-time KStream-KTable order + inventory join
- `POST /api/kafka/publish/tiered-retention`: Cold/tiered retention topic publish

### 5. [`KafkaAuditController`](file:///sdcard/Download/termux/spring-kafka-showcase/src/main/java/com/example/springkafka/controller/KafkaAuditController.java)
- `GET /api/kafka/audit/received`: Ingested standard & priority orders
- `GET /api/kafka/audit/dlt`: Poisoned records in DLT
- `GET /api/kafka/audit/nonblocking`: Non-blocking retry records & DLT exhaustion
- `GET /api/kafka/audit/manual-ack`: Manual acknowledgment & nack redelivery records
- `GET /api/kafka/audit/inventory`: Current compacted inventory key-value table
- `GET /api/kafka/audit/streams/analytics`: All customer metrics aggregated by Kafka Streams
- `GET /api/kafka/audit/streams/analytics/{customerId}`: Point query against local Streams state store
- `GET /api/kafka/audit/streams/analytics/windowed`: 1-minute tumbling window metrics
- `GET /api/kafka/audit/streams/joined`: Real-time KStream-KTable enriched orders
- `GET /api/kafka/audit/avro`: Ingested records decoded from raw Apache Avro
- `GET /api/kafka/audit/avro/schema`: Official JSON specification of OrderAvroRecord
- `GET /api/kafka/audit/schema-registry/subjects`: Registered Schema Registry subjects and versions
- `POST /api/kafka/audit/schema-registry/subjects/{subject}/versions`: Register new schema version
- `GET /api/kafka/audit/outbox-cdc`: Outbox records streamed via CDC
- `GET /api/kafka/audit/encrypted`: Consumer records with decrypted customer PII
- `GET /api/kafka/audit/tiered`: Records in tiered/cold retention topic
- `GET /api/kafka/audit/summary`: Global quantitative metric summary across all pipelines

---

## 🚀 How to Run in Termux

### 1. Build and Run Tests
```bash
cd /sdcard/Download/termux/spring-kafka-showcase
mvn clean test
```

### 2. Start Application on Port 8080
```bash
mvn spring-boot:run
```

---

## 📖 Interactive Swagger UI & OpenAPI Documentation

The application exposes an interactive OpenAPI 3.0 test bench to document and execute requests live:

- **Swagger UI**: [http://localhost:8080/swagger-ui.html](http://localhost:8080/swagger-ui.html) (redirects to `/swagger-ui/index.html`)
- **OpenAPI v3 Spec (JSON)**: [http://localhost:8080/v3/api-docs](http://localhost:8080/v3/api-docs)

---

## 🧪 Interactive API Testing & cURL Commands

### 1. Publish to Standard Topic (Key-based Partitioning)
```bash
curl -i -X POST http://localhost:8080/api/kafka/publish/simple \
  -H "Content-Type: application/json" \
  -d '{"orderId":"ORD-101","customerId":"CUST-88","skuCode":"IPHONE15","quantity":1,"price":999.0,"priority":"NORMAL","simulateFailure":false}'
```

---

### 2. Publish to a Specific Partition with Tracing Headers
```bash
curl -i -X POST "http://localhost:8080/api/kafka/publish/partitioned?partition=2" \
  -H "Content-Type: application/json" \
  -d '{"orderId":"ORD-102","customerId":"CUST-99","skuCode":"MACBOOK-PRO","quantity":1,"price":1999.0,"priority":"NORMAL","simulateFailure":false}'
```

---

### 3. Test Message Filtering (VIP Filter)
```bash
# A) Send HIGH priority order (Will be ACCEPTED and processed)
curl -i -X POST http://localhost:8080/api/kafka/publish/priority \
  -H "Content-Type: application/json" \
  -d '{"orderId":"ORD-VIP-1","customerId":"VIP-1","skuCode":"IPAD-PRO","quantity":1,"price":1200.0,"priority":"HIGH","simulateFailure":false}'

# B) Send NORMAL priority order (Will be FILTERED OUT and discarded)
curl -i -X POST http://localhost:8080/api/kafka/publish/priority \
  -H "Content-Type: application/json" \
  -d '{"orderId":"ORD-LOW-1","customerId":"LOW-1","skuCode":"AIRTAG","quantity":1,"price":29.0,"priority":"NORMAL","simulateFailure":false}'
```

---

### 4. Test Resilient Error Handling & Dead Letter Topic (DLT)
```bash
curl -i -X POST "http://localhost:8080/api/kafka/publish/retry-dlt?fail=true" \
  -H "Content-Type: application/json" \
  -d '{"orderId":"ORD-FAIL-1","customerId":"CUST-BAD","skuCode":"FAULTY-ITEM","quantity":1,"price":50.0,"priority":"NORMAL","simulateFailure":true}'
```

---

### 5. Audit & Message Verification Endpoints
```bash
# Processed messages
curl http://localhost:8080/api/kafka/audit/received

# Dead Letter Topic records
curl http://localhost:8080/api/kafka/audit/dlt

# Summary counts
curl http://localhost:8080/api/kafka/audit/summary
---

### 6. Enterprise Patterns: Event Sourcing, Distributed Sagas, Idempotency & DLT Re-drive

#### A) Event Sourcing & CQRS Rehydration
```bash
# 1. Append state transition events to aggregate
curl -X POST "http://localhost:8080/api/kafka/publish/event-sourced/ORD-100?eventType=ORDER_CREATED" \
  -H "Content-Type: application/json" \
  -d '{"orderId":"ORD-100","customerId":"CUST-10","skuCode":"IPHONE-16","quantity":1,"price":999.0,"priority":"HIGH","simulateFailure":false}'

curl -X POST "http://localhost:8080/api/kafka/publish/event-sourced/ORD-100?eventType=PAYMENT_RESERVED" \
  -H "Content-Type: application/json" \
  -d '{"orderId":"ORD-100","customerId":"CUST-10","skuCode":"IPHONE-16","quantity":1,"price":999.0,"priority":"HIGH","simulateFailure":false}'

# 2. Rehydrate aggregate state by replaying event stream
curl http://localhost:8080/api/kafka/audit/event-sourced/ORD-100/rehydrate
curl http://localhost:8080/api/kafka/audit/event-sourced/ORD-100/events
```

#### B) Distributed Saga Orchestration (Checkout Workflow)
```bash
# Normal successful 3-step Saga (Order -> Payment -> Inventory)
curl -X POST "http://localhost:8080/api/kafka/publish/saga/checkout" \
  -H "Content-Type: application/json" \
  -d '{"orderId":"ORD-SAGA-OK","customerId":"VIP-1","skuCode":"MACBOOK-PRO","quantity":1,"price":2199.0,"priority":"HIGH","simulateFailure":false}'

# Failing Saga with automated compensating transaction rollback
curl -X POST "http://localhost:8080/api/kafka/publish/saga/checkout?forcePaymentFail=true" \
  -H "Content-Type: application/json" \
  -d '{"orderId":"ORD-SAGA-ROLLBACK","customerId":"VIP-2","skuCode":"MACBOOK-AIR","quantity":1,"price":1199.0,"priority":"HIGH","simulateFailure":false}'

# Inspect all Saga instances and step execution history
curl http://localhost:8080/api/kafka/audit/saga/instances
```

#### C) Idempotent Consumer & Deduplication Sliding Window Cache
```bash
# Publish twice with identical orderId: second delivery is intercepted and suppressed
curl -X POST "http://localhost:8080/api/kafka/publish/simple" -H "Content-Type: application/json" \
  -d '{"orderId":"ORD-DEDUP-TEST","customerId":"CUST-1","skuCode":"ITEM-1","quantity":1,"price":25.0,"priority":"NORMAL","simulateFailure":false}'

# Inspect deduplication sliding window cache and suppressed attempts
curl http://localhost:8080/api/kafka/audit/dedup/attempts
```

#### D) Dead Letter Topic (DLT) Administrative Re-drive
```bash
# Re-dispatch failed poison message from DLT to orders.standard with failure flag cleared
curl -X POST "http://localhost:8080/api/kafka/audit/dlt/redrive/ORD-FAIL-1?clearFailureFlag=true"

# View audit history of all executed re-drives
curl http://localhost:8080/api/kafka/audit/dlt/redrive/history
```

#### E) KStream-KStream Sliding-Window Correlation
```bash
# Publish shipment event to correlate with an existing order within a 5-minute sliding window
curl -X POST "http://localhost:8080/api/kafka/publish/shipment" \
  -H "Content-Type: application/json" \
  -d '{"shipmentId":"SHIP-991","orderId":"ORD-TRACE-101","trackingNumber":"TRK-FEDEX-5544","carrier":"FEDEX","estimatedDelivery":"2026-10-06T12:00:00Z","status":"DISPATCHED","timestamp":"2026-10-02T11:13:30Z"}'

# Query correlated orders and shipments
curl http://localhost:8080/api/kafka/audit/streams/joined-shipments
```

#### F) Kafka Streams Session Windows (Inactivity Gaps)
```bash
# Publish multiple interactions for a user within an active session
curl -X POST "http://localhost:8080/api/kafka/publish/user-session" \
  -H "Content-Type: application/json" \
  -d '{"sessionId":"SESS-1","userId":"USER-ALICE","action":"VIEW_CATALOG","pageUrl":"/store/phones","timestamp":"2026-10-02T11:13:00Z"}'

# Retrieve aggregated session window summary
curl http://localhost:8080/api/kafka/audit/streams/sessions/USER-ALICE
curl http://localhost:8080/api/kafka/audit/streams/sessions
```

#### G) W3C Distributed Tracing & Lineage (OpenTelemetry-compatible)
```bash
# View complete transaction trace graphs with parent-child span hops
curl http://localhost:8080/api/kafka/audit/tracing/all
curl http://localhost:8080/api/kafka/audit/tracing/lineage/{traceId}
```

#### H) Debezium CDC Pipeline & S3 Lake Sink
```bash
# Simulate database row mutation with automated PII masking SMTs and S3 Lake batching
curl -X POST "http://localhost:8080/api/kafka/publish/connect/cdc-table?table=orders&op=INSERT" \
  -H "Content-Type: application/json" \
  -d '{"orderId":"ORD-CDC-999","customerId":"CUST-CDC","creditCardNumber":"4111222233334444","email":"alice@enterprise.com","total":1499.0}'

# Inspect SMT transformed messages and Data Lake Parquet batches
curl http://localhost:8080/api/kafka/audit/connect/transformed
curl http://localhost:8080/api/kafka/audit/connect/lake-batches
```

---

## 🪵 Logging & Diagnostic Files

The application features a production-grade Logback mechanism ([`logback-spring.xml`](file:///sdcard/Download/termux/spring-framework-6/spring-kafka-showcase/src/main/resources/logback-spring.xml)) writing categorized logs to the `logs/` directory with size-and-time-based rolling policies:

| Log File | Description | Rolling Policy |
| :--- | :--- | :--- |
| `logs/spring-kafka-showcase.log` | Complete application event log (HTTP requests, producer dispatches, consumer offsets, audit entries) | 10MB per file, 30 days retention (`.gz` compression) |
| `logs/spring-kafka-showcase-error.log` | Dedicated error and exception log (DLT routing, listener errors, broker timeouts) | 10MB per file, filtered to `ERROR` level only |

### Tailing Logs in Real Time
```bash
# Tail all application events
tail -f logs/spring-kafka-showcase.log

# Tail errors and Dead Letter Queue exceptions
tail -f logs/spring-kafka-showcase-error.log
```

