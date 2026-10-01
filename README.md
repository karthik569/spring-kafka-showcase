# Spring Boot Apache Kafka Showcase 🚀

A comprehensive, production-grade Spring Boot application demonstrating the full suite of **Apache Kafka** capabilities on **Java 21** and **Spring Boot 3.3**.

> 💡 **New to Apache Kafka?** Check out the step-by-step [Getting Started Guide](file:///sdcard/Download/termux/spring-framework-6/spring-kafka-showcase/GETTING_STARTED_WITH_KAFKA.md) for conceptual explanations, interactive tutorials, and CLI cheat sheets.

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
- **`CompletableFuture<SendResult<String, OrderEvent>> sendSimpleOrder(OrderEvent event)`**: Dispatches an order to `orders.standard` using the `orderId` as the partition key. Attaches non-blocking completion callbacks (`whenComplete`) to log partition assignment, topic offset, and latency.
- **`CompletableFuture<SendResult<String, OrderEvent>> sendOrderToPartition(OrderEvent event, int partition)`**: Constructs a low-level `ProducerRecord` explicitly designating the target partition number and injecting diagnostic metadata headers (`X-Correlation-ID`, `X-Timestamp`).
- **`CompletableFuture<SendResult<String, OrderEvent>> sendPriorityOrder(OrderEvent event)`**: Routes high-priority orders to `orders.high-priority`. Records with `priority != HIGH` are dropped upstream by the consumer filter.
- **`CompletableFuture<SendResult<String, OrderEvent>> sendRetryableOrder(OrderEvent event)`**: Emits messages to `orders.retryable` to test resilience pipelines and Dead-Letter-Topic diversion.
- **`void sendBatchOrders(List<OrderEvent> events)`**: Iterates over a collection of orders, publishing each asynchronously with tracking callbacks to demonstrate high-throughput publishing.
- **`boolean sendTransactionalOrders(List<OrderEvent> events)`**: Executes atomic message batch publishing inside `kafkaTemplate.executeInTransaction()`, ensuring all-or-nothing delivery semantics.

### 2. [`KafkaConsumerService`](file:///sdcard/Download/termux/spring-kafka-showcase/src/main/java/com/example/springkafka/service/KafkaConsumerService.java)
- **`void consumeStandardOrder(ConsumerRecord<String, OrderEvent> record)`**: `@KafkaListener(topics = "orders.standard", groupId = "standard-consumers-group")`; ingests standard orders, extracts topic/partition/offset, and appends to the in-memory audit store.
- **`void consumePriorityOrder(ConsumerRecord<String, OrderEvent> record)`**: `@KafkaListener(topics = "orders.high-priority", containerFactory = "filterContainerFactory")`; consumes filtered VIP events passed by `RecordFilterStrategy`.
- **`void consumeRetryableOrder(ConsumerRecord<String, OrderEvent> record)`**: Ingests test messages. If `event.simulateFailure() == true`, throws an exception to trigger the retry policy (2 retries) and automatic routing to `.DLT`.
- **`void consumeDltOrder(ConsumerRecord<String, OrderEvent> record)`**: `@KafkaListener(topics = "orders.retryable.DLT")`; intercepts poisoned records diverted to the dead-letter queue and records failure metrics.
- **`void consumeBatchOrders(List<ConsumerRecord<String, OrderEvent>> records)`**: `@KafkaListener(containerFactory = "batchContainerFactory")`; consumes batches of messages in bulk, drastically improving I/O throughput.

### 3. [`KafkaProducerController`](file:///sdcard/Download/termux/spring-kafka-showcase/src/main/java/com/example/springkafka/controller/KafkaProducerController.java)
- **`POST /api/kafka/publish/simple`**: REST trigger delegating to `sendSimpleOrder`.
- **`POST /api/kafka/publish/partitioned`**: REST trigger accepting explicit `partition` integer query parameter.
- **`POST /api/kafka/publish/priority`**: REST trigger publishing to the VIP filtered pipeline.
- **`POST /api/kafka/publish/retry-dlt`**: REST trigger injecting synthetic failure flags to test DLT recovery.
- **`POST /api/kafka/publish/batch`**: REST trigger accepting an array of order JSON objects.
- **`POST /api/kafka/publish/transaction`**: REST trigger demonstrating transactional outbox publishing.

### 4. [`KafkaAuditController`](file:///sdcard/Download/termux/spring-kafka-showcase/src/main/java/com/example/springkafka/controller/KafkaAuditController.java)
- **`GET /api/kafka/audit/received`**: Returns list of all processed events with partitions and offsets.
- **`GET /api/kafka/audit/dlt`**: Returns poisoned events captured by the Dead-Letter-Topic consumer.
- **`GET /api/kafka/audit/summary`**: Returns quantitative metrics (total processed, total DLT, etc.).

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

