# Apache Kafka Showcase - In-Depth Architecture & Method Guide 🚀

This document provides a comprehensive, method-by-method technical deep dive into `spring-kafka-showcase`. It details the internal mechanics, parameters, error handling strategies, and message flows implemented in the project.

---

## 1. Project Overview & Architectural Flow

The application runs on port `8080` and connects to Termux's local Apache Kafka broker at `localhost:9092`. It demonstrates:
1. **Asynchronous Non-Blocking Event Publishing** with partition key hashing.
2. **Explicit Partition Targeting** with custom distributed tracing headers (`X-Correlation-ID`).
3. **Record Filtering** using `RecordFilterStrategy` to discard non-VIP messages upstream.
4. **High-Throughput Batch Processing** via `setBatchListener(true)`.
5. **Dead Letter Topic (DLT)** diversion via `DeadLetterPublishingRecoverer` and `DefaultErrorHandler`.
6. **Kafka Transactions** ensuring all-or-nothing delivery guarantees.

```
 [Client / cURL] 
       │
       ▼
 [KafkaProducerController]
       │
       ▼
 [KafkaProducerService] ──── (Sends ProducerRecord with tracing headers)
       │
       ├───► orders.standard ───────► KafkaConsumerService.consumeStandard()
       ├───► orders.high-priority ──► KafkaConsumerService.consumeHighPriority() (Filtered)
       ├───► orders.batch ──────────► KafkaConsumerService.consumeBatch() (Bulk List)
       ├───► orders.retryable ──────► KafkaConsumerService.consumeRetryable()
       │                                       │ (Fails & Retries 2x)
       │                                       ▼
       │                             orders.retryable.DLT
       │                                       │
       │                                       ▼
       │                             KafkaConsumerService.consumeDeadLetterTopic()
       │
       └───► In-Memory Audit Ledger (KafkaAuditController)
```

---

## 2. In-Depth Class & Method Breakdown

### A. Configuration Layer

#### [`KafkaTopicConfig.java`](file:///sdcard/Download/termux/spring-kafka-showcase/src/main/java/com/example/springkafka/config/KafkaTopicConfig.java)
- **`NewTopic ordersStandardTopic()`**:
  - *Purpose*: Programmatically initializes the `orders.standard` topic on broker startup if it does not already exist.
  - *Configuration*: 3 partitions, replication factor of 1.
  - *Benefit*: Eliminates manual `kafka-topics.sh` command creation and ensures multi-partition load balancing across consumer instances.
- **`NewTopic ordersPriorityTopic()`**:
  - *Purpose*: Creates `orders.high-priority` with 2 partitions for filtered VIP event traffic.
- **`NewTopic ordersBatchTopic()`**:
  - *Purpose*: Creates `orders.batch` with 3 partitions for bulk batch consumer tests.
- **`NewTopic ordersRetryableTopic()`**:
  - *Purpose*: Creates `orders.retryable` with 3 partitions to test retry policies.
- **`NewTopic ordersRetryableDltTopic()`**:
  - *Purpose*: Creates `orders.retryable.DLT` (Dead Letter Topic) with 3 partitions to receive poisoned or unprocessable messages.

#### [`KafkaProducerConfig.java`](file:///sdcard/Download/termux/spring-kafka-showcase/src/main/java/com/example/springkafka/config/KafkaProducerConfig.java)
- **`BeanPostProcessor kafkaTemplatePostProcessor()`**:
  - *Purpose*: Hooks into `KafkaTemplate` initialization to invoke `template.setAllowNonTransactional(true)`.
  - *Internal Mechanics*: Enables the application to support transactional multi-message sends via `executeInTransaction` (`spring.kafka.producer.transaction-id-prefix: tx-`) while allowing simple fire-and-forget asynchronous publishes (`sendAsync`, `sendWithHeaders`) on the exact same shared `KafkaTemplate` bean without throwing `IllegalStateException`.

#### [`OpenApiConfig.java`](file:///sdcard/Download/termux/spring-kafka-showcase/src/main/java/com/example/springkafka/config/OpenApiConfig.java)
- **`OpenAPI customOpenAPI()`**:
  - *Purpose*: Configures OpenAPI 3.0 specification metadata, Swagger UI display title, version, documentation descriptions, server endpoints, and logical tagging (`Kafka Producer API`, `Kafka Audit API`).
  - *Access URLs*: Interactive Swagger UI at `http://localhost:8080/swagger-ui.html` and raw JSON spec at `http://localhost:8080/v3/api-docs`.

#### [`KafkaConsumerConfig.java`](file:///sdcard/Download/termux/spring-kafka-showcase/src/main/java/com/example/springkafka/config/KafkaConsumerConfig.java)
- **`ConcurrentKafkaListenerContainerFactory<String, OrderEvent> filterContainerFactory(...)`**:
  - *Purpose*: Instantiates a listener container factory with an inline `RecordFilterStrategy`.
  - *Logic*: Inspects `record.value().priority()`. If priority is NOT equal to `"HIGH"`, returns `true` (filter out), discarding the record before any consumer method is invoked.
- **`ConcurrentKafkaListenerContainerFactory<String, OrderEvent> batchContainerFactory(...)`**:
  - *Purpose*: Configures a high-throughput listener container factory with `factory.setBatchListener(true)`.
  - *Behavior*: Ingests records in lists (`List<ConsumerRecord<String, OrderEvent>>`) rather than one by one, reducing thread context switches.
- **`DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> template)`**:
  - *Purpose*: Constructs a centralized error handler combining a `DeadLetterPublishingRecoverer` with a `FixedBackOff(1000L, 2)`.
  - *Execution Trace*: If a consumer throws an exception, it retries 2 times with a 1-second delay. Upon third failure, it publishes the failed record and exception stack trace directly to `<topic>.DLT`.

---

### B. Producer Service Layer

#### [`KafkaProducerService.java`](file:///sdcard/Download/termux/spring-kafka-showcase/src/main/java/com/example/springkafka/service/KafkaProducerService.java)
- **`CompletableFuture<PublishResponse> sendAsync(String topic, String key, OrderEvent event)`**:
  - *Inputs*: `topic` (String), `key` (String partition key), `event` (Order payload).
  - *Operation*: Calls `kafkaTemplate.send(topic, key, enrichedEvent)`. Appends non-blocking `.thenApply()` to transform the broker's `SendResult` into a `PublishResponse` containing the assigned partition and offset.
  - *Error Handling*: Attaches `.exceptionally()` to catch connection timeouts or serialization errors without blocking the calling thread.
- **`CompletableFuture<PublishResponse> sendWithHeaders(String topic, Integer partition, String key, OrderEvent event)`**:
  - *Inputs*: `topic`, `partition` (explicit integer index), `key`, `event`.
  - *Operation*: Constructs a low-level `ProducerRecord`. Generates a UUID `X-Correlation-ID` and `X-Source-Service` byte header and attaches them to `record.headers()`.
  - *Use Case*: Targeted partition testing and distributed tracing across microservices.
- **`boolean sendInTransaction(String topic, String key, OrderEvent... events)`**:
  - *Inputs*: `topic`, `key`, varargs `events`.
  - *Operation*: Calls `kafkaTemplate.executeInTransaction(operations -> { ... })`. Publishes all events inside the transactional context.
  - *Guarantee*: If any message in the varargs array fails, the entire transaction is rolled back, preventing zombie/partial records on the broker.

---

### C. Consumer Service Layer

#### [`KafkaConsumerService.java`](file:///sdcard/Download/termux/spring-kafka-showcase/src/main/java/com/example/springkafka/service/KafkaConsumerService.java)
- **`void consumeStandard(OrderEvent event, int partition, long offset, String topic, String key)`**:
  - *Annotation*: `@KafkaListener(topics = "orders.standard", groupId = "standard-consumers-group")`.
  - *Operation*: Consumes single messages, extracts broker headers (`KafkaHeaders.OFFSET`, `KafkaHeaders.RECEIVED_PARTITION`), logs ingestion details, and saves an `AuditRecord` in the thread-safe `receivedRecords` list.
- **`void consumeHighPriority(OrderEvent event, int partition, long offset, String topic, String key)`**:
  - *Annotation*: `@KafkaListener(topics = "orders.high-priority", containerFactory = "filterContainerFactory")`.
  - *Operation*: Only invoked for high-priority records. Saves processed events with status `"HIGH_PRIORITY_PROCESSED"`.
- **`void consumeBatch(List<ConsumerRecord<String, OrderEvent>> records)`**:
  - *Annotation*: `@KafkaListener(topics = "orders.batch", containerFactory = "batchContainerFactory")`.
  - *Operation*: Iterates over the batch list, logging each item's key, partition, and offset, then appends all items to the audit repository.
- **`void consumeRetryable(OrderEvent event, int partition, long offset, String topic, String key)`**:
  - *Annotation*: `@KafkaListener(topics = "orders.retryable", groupId = "retryable-orders-group")`.
  - *Operation*: If `event.simulateFailure() == true`, throws `new RuntimeException(...)`. This activates the `DefaultErrorHandler`, attempts 2 retries, and forwards to the DLT topic.
- **`void consumeDeadLetterTopic(OrderEvent failedEvent, int partition, long offset, String topic, String key)`**:
  - *Annotation*: `@KafkaListener(topics = "orders.retryable.DLT", groupId = "dlt-monitor-group")`.
  - *Operation*: Intercepts dead letters from the DLT topic, recording them in `dltRecords` for operational alerting.
- **`void consumeNonblocking(OrderEvent event, String topic, int partition, long offset, String key)`**:
  - *Annotations*: `@RetryableTopic(attempts = "3", backoff = @Backoff(delay = 1000, multiplier = 2.0))` and `@KafkaListener(topics = "orders.nonblocking", groupId = "nonblocking-orders-group")`.
  - *Non-Blocking Semantics*: Failed messages are forwarded to separate retry topics with graduated backoff delays without stalling other records on the partition.
- **`void handleNonblockingDlt(OrderEvent failedEvent, ...)`**:
  - *Annotation*: `@DltHandler`.
  - *Operation*: Captures messages after all 3 non-blocking retry attempts are exhausted.
- **`void consumeManualAck(ConsumerRecord<String, OrderEvent> record, Acknowledgment acknowledgment)`**:
  - *Annotation*: `@KafkaListener(topics = "orders.manual-ack", containerFactory = "manualAckContainerFactory")`.
  - *Operation*: Implements programmatic commit via `acknowledgment.acknowledge()` on success, or `acknowledgment.nack(Duration.ofMillis(1000))` backpressure redelivery on failure.
- **`void consumeCompactedInventory(ConsumerRecord<String, InventoryItem> record)`**:
  - *Annotation*: `@KafkaListener(topics = "inventory.compacted", groupId = "inventory-compacted-group")`.
  - *Log Compaction & Tombstones*: Updates in-memory inventory state map for non-null items; deletes the key when `record.value() == null` (tombstone record).

---

### D. Kafka Streams Real-Time Processing Layer

#### [`OrderStreamsService.java`](file:///sdcard/Download/termux/spring-kafka-showcase/src/main/java/com/example/springkafka/service/OrderStreamsService.java)
- **`void startTopology()`**:
  - *Topology Flow*:
    1. Consumes `OrderEvent` stream from `streams.orders.input`.
    2. Maps records to key by `customerId`.
    3. Groups by key (`groupByKey()`).
    4. Aggregates cumulative `orderCount`, `totalSpend`, and `averageSpend`.
    5. Materializes results into an in-memory key-value state store (`customer-analytics-store`).
    6. Emits updated `StreamAnalytics` records downstream to `streams.analytics.output`.
  - *Robustness*: Configured with in-memory state stores (`Stores.inMemoryKeyValueStore(...)`) to run seamlessly across all environments including ARM64 PRoot without requiring native C++ RocksDB binaries.
- **`Optional<StreamAnalytics> getAnalyticsForCustomer(String customerId)`**:
  - Performs real-time interactive point queries against the local materialized state store.
- **`List<StreamAnalytics> getAllAnalytics()`**:
  - Iterates over all materialized customer records in the state store.

---

### E. REST Controllers Layer

#### [`KafkaProducerController.java`](file:///sdcard/Download/termux/spring-kafka-showcase/src/main/java/com/example/springkafka/controller/KafkaProducerController.java)
- **`POST /api/kafka/publish/simple`**: Publishes asynchronous order to `orders.standard`.
- **`POST /api/kafka/publish/partitioned?partition=N`**: Publishes to specific partition with distributed tracing headers.
- **`POST /api/kafka/publish/priority`**: Publishes high-priority order (filtered by consumer).
- **`POST /api/kafka/publish/retry-dlt?fail=true`**: Publishes with failure simulation flag set.
- **`POST /api/kafka/publish/batch`**: Ingests JSON array of orders and publishes to `orders.batch`.
- **`POST /api/kafka/publish/transaction`**: Ingests JSON array and dispatches atomically in `sendInTransaction()`.
- **`POST /api/kafka/publish/nonblocking?fail=true`**: Publishes to `orders.nonblocking` to test `@RetryableTopic` exponential backoff.
- **`POST /api/kafka/publish/manual-ack?fail=false`**: Publishes to `orders.manual-ack` to demonstrate `AckMode.MANUAL_IMMEDIATE` and `nack()`.
- **`POST /api/kafka/publish/inventory`**: Publishes state update to compacted topic `inventory.compacted`.
- **`DELETE /api/kafka/publish/inventory/tombstone/{skuCode}`**: Sends tombstone (`null` value) record to delete SKU key from compacted topic.
- **`POST /api/kafka/publish/streams-order`**: Publishes orders to `streams.orders.input` for real-time Kafka Streams aggregation.
- **`POST /api/kafka/publish/avro`**: Encodes domain `OrderEvent` into raw Apache Avro binary bytes conforming to `OrderAvroRecord` schema and publishes to `orders.avro`.
- **`POST /api/kafka/publish/outbox-cdc`**: Commits order state change to local ACID outbox table and streams via CDC poller to `orders.outbox.cdc`.
- **`POST /api/kafka/publish/encrypted`**: Client-side AES envelope encryption on sensitive PII customer fields before publishing to `orders.encrypted`.
- **`POST /api/kafka/publish/joined-enrichment`**: Real-time KStream-KTable join correlating orders with warehouse inventory item metadata and publishing to `orders.joined.output`.
- **`POST /api/kafka/publish/tiered-retention`**: Publishes orders to `orders.tiered.retention` configured with 7-day retention and 10MB segments for cold object storage offloading.

#### [`KafkaAuditController.java`](file:///sdcard/Download/termux/spring-kafka-showcase/src/main/java/com/example/springkafka/controller/KafkaAuditController.java)
- **`GET /api/kafka/audit/received`**: Returns list of all processed events with partitions and offsets.
- **`GET /api/kafka/audit/dlt`**: Returns list of all dead letters.
- **`GET /api/kafka/audit/nonblocking`**: Returns non-blocking retry processed and DLT-exhausted records.
- **`GET /api/kafka/audit/manual-ack`**: Returns manually committed and nacked audit records.
- **`GET /api/kafka/audit/inventory`**: Returns current state of warehouse inventory derived from the compacted log.
- **`GET /api/kafka/audit/streams/analytics`**: Returns all customer analytics computed by Kafka Streams topology.
- **`GET /api/kafka/audit/streams/analytics/{customerId}`**: Interactive point query on Kafka Streams state store for a specific customer.
- **`GET /api/kafka/audit/streams/analytics/windowed`**: Returns 1-minute tumbling window metrics.
- **`GET /api/kafka/audit/streams/joined`**: Returns real-time joined stream enrichments (KStream - KTable).
- **`GET /api/kafka/audit/avro`**: Returns deserialized Apache Avro records received from `orders.avro`.
- **`GET /api/kafka/audit/avro/schema`**: Returns official JSON specification of the Apache Avro schema.
- **`GET /api/kafka/audit/schema-registry/subjects`**: Returns all registered schemas and versions in Schema Registry simulator.
- **`POST /api/kafka/audit/schema-registry/subjects/{subject}/versions`**: Registers new schema version with evolution compatibility check.
- **`GET /api/kafka/audit/outbox-cdc`**: Returns all transactional outbox events dispatched to Kafka via CDC.
- **`GET /api/kafka/audit/encrypted`**: Returns consumed encrypted records with customer PII decrypted by authorized consumers.
- **`GET /api/kafka/audit/tiered`**: Returns records stored in long-term cold/tiered retention topic.
- **`GET /api/kafka/audit/summary`**: Returns total counts and operational health summary across all pipelines.

---

### F. Enterprise Kafka Capabilities Added

1. **Transactional Outbox & CDC (`OutboxCdcService.java`, `OutboxRecord.java`)**:
   - Solves dual-write inconsistency between local databases and Kafka brokers.
   - Atomic state change + Outbox record persistence in the same local transaction.
   - CDC poller streams committed records reliably to `orders.outbox.cdc`.

2. **KStream-KTable Real-Time Joins (`EnrichedOrderEvent.java`, `OrderStreamsService.java`)**:
   - Joins fast-moving order streams with reference inventory state stores in memory.
   - Generates enriched real-time operational views published to `orders.joined.output`.

3. **Schema Registry & Evolution (`SchemaRegistryService.java`)**:
   - Centralized subject versioning (`orders.avro-value`).
   - Compatibility validation (BACKWARD, FORWARD, FULL) preventing contract breakage.
   - Schema ID and canonical JSON retrieval.

4. **Client-Side Field-Level Encryption (`FieldEncryptionService.java`)**:
   - AES-128 client-side envelope encryption for sensitive PII/GDPR fields (`customerId`).
   - Brokers remain zero-knowledge intermediaries storing encrypted ciphertexts (`[ENC]...`).
   - Consumers decrypt transparently upon ingestion.

5. **Tiered Storage Architecture (KIP-405) (`orders.tiered.retention`)**:
   - Topic configured with extended retention and customized segment roll intervals.
   - Facilitates transparent historical offset playback and offloading to cold object stores (S3/GCS).

6. **Apache Avro Binary Serialization (`AvroSerializerService.java`)**:
   - Canonical `OrderAvroRecord` schema with compact binary encoding.

7. **Custom Geographic Routing (`RegionAwarePartitioner.java`)**:
   - Deterministic partition routing: `US-*` -> Partition 0, `EU-*` -> Partition 1, `APAC-*` -> Partition 2.

8. **Cooperative Sticky Rebalancing (`CooperativeStickyAssignor`)**:
   - Zero-downtime, non-stop partition reassignment without "stop-the-world" latency spikes.

---

### H. Enterprise Kafka Patterns: Event Sourcing, Distributed Sagas, Idempotency & DLT Re-Drive

1. **Event Sourcing & CQRS (`EventSourcingService.java`, `DomainEvent.java`)**:
   - **Immutable Event Stream**: State changes are written sequentially to the append-only topic `events.sourced`.
   - **CQRS Projections**: Commands update read models asynchronously while queries read materialized views without locking write models.
   - **State Rehydration**: Allows complete reconstruction ("time travel") of any aggregate's current state by replaying all historical domain events from offset 0 / version 1.
   - **Endpoints**:
     - `POST /api/kafka/publish/event-sourced/{aggregateId}`
     - `GET /api/kafka/audit/event-sourced/{aggregateId}/events`
     - `GET /api/kafka/audit/event-sourced/{aggregateId}/rehydrate`

2. **Distributed Saga Orchestrator (`SagaOrchestratorService.java`, `SagaInstance.java`)**:
   - **Orchestration Workflow**: Manages long-lived distributed transactions spanning microservice boundaries (Order Placed -> Payment Authorized -> Warehouse Inventory Reserved).
   - **Automated Compensating Transactions**: If any downstream step fails (e.g. `forcePaymentFail=true`), the orchestrator dispatches compensating commands to rollback preceding steps (e.g. order cancellation, inventory reservation release), restoring data consistency across services.
   - **Audit Trail**: Every executed step and compensating action is immutably timestamped in the Saga instance history.
   - **Endpoints**:
     - `POST /api/kafka/publish/saga/checkout`
     - `GET /api/kafka/audit/saga/instances`

3. **Idempotent Consumer & Sliding-Window Deduplication (`IdempotentDeduplicationService.java`)**:
   - **Duplicate Interception**: Intercepts duplicate messages caused by consumer restarts, network timeouts, or at-least-once retries using an in-memory sliding window cache.
   - **Keyed Evaluation**: Compares unique idempotency keys (e.g. `orderId` or custom headers). First-time deliveries are processed; duplicates are safely suppressed without executing redundant business logic.
   - **Auditing**: Records both successful first-time keys and suppressed duplicate attempts.
   - **Endpoint**:
     - `GET /api/kafka/audit/dedup/attempts`

4. **Dead Letter Topic (DLT) Management & Administrative Re-Drive (`DeadLetterRedriveService.java`)**:
   - **Poison Message Recovery**: Inspects poisoned messages that have exhausted their retry budget in `orders.retryable.DLT`.
   - **Re-drive Replay**: Allows platform operators to re-dispatch dead-letter records to a destination topic (e.g. `orders.standard`), with options to strip transient failure simulation flags or correct payloads.
   - **Re-drive Audit**: Records complete audit history of all administrative redrive operations with unique `redriveId` correlation tags.
   - **Endpoints**:
     - `POST /api/kafka/audit/dlt/redrive/{orderId}`
     - `GET /api/kafka/audit/dlt/redrive/history`

---

### I. Interactive Architecture Diagrams (HTML)

The project includes an interactive, browser-ready architectural visualizer with zoom/pan controls:
- **File**: [`kafka-architecture-diagrams.html`](file:///sdcard/Download/termux/spring-framework-6/spring-kafka-showcase/kafka-architecture-diagrams.html)
- Contains rendered Mermaid.js diagrams for:
  1. Complete System Architecture Overview
  2. Non-Blocking Retries (`@RetryableTopic`) Sequence
  3. Manual Acknowledgment & Nack Backpressure Flow
  4. Real-Time Kafka Streams Topology & State Store
  5. Log Compaction & Tombstone Deletion Mechanics
  6. Exactly-Once Transaction Lifecycle (2-Phase Commit)
  7. Geographic Region-Aware Partitioner Flow
  8. Cooperative Sticky Assignor vs Eager Rebalancing
  9. Schema Registry, Avro & CDC Pipeline
  10. Tiered Storage & MirrorMaker 2.0 Disaster Recovery

