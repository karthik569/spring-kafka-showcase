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

---

### D. REST Controllers Layer

#### [`KafkaProducerController.java`](file:///sdcard/Download/termux/spring-kafka-showcase/src/main/java/com/example/springkafka/controller/KafkaProducerController.java)
- **`POST /api/kafka/publish/simple`**: Calls `producerService.sendAsync("orders.standard", order.orderId(), order)`.
- **`POST /api/kafka/publish/partitioned?partition=N`**: Calls `producerService.sendWithHeaders("orders.standard", partition, order.orderId(), order)`.
- **`POST /api/kafka/publish/priority`**: Calls `producerService.sendAsync("orders.high-priority", order.orderId(), order)`.
- **`POST /api/kafka/publish/retry-dlt?fail=true`**: Publishes with failure simulation flag set.
- **`POST /api/kafka/publish/batch`**: Ingests JSON array of orders and publishes to `orders.batch`.
- **`POST /api/kafka/publish/transaction`**: Ingests JSON array and dispatches atomically in `sendInTransaction()`.

#### [`KafkaAuditController.java`](file:///sdcard/Download/termux/spring-kafka-showcase/src/main/java/com/example/springkafka/controller/KafkaAuditController.java)
- **`GET /api/kafka/audit/received`**: Returns list of all processed events with partitions and offsets.
- **`GET /api/kafka/audit/dlt`**: Returns list of all dead letters.
- **`GET /api/kafka/audit/summary`**: Returns total counts and operational health.
