package com.example.springkafka.controller;

import com.example.springkafka.dto.OrderEvent;
import com.example.springkafka.dto.PublishResponse;
import com.example.springkafka.service.KafkaProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/kafka/publish")
public class KafkaProducerController {

    private final KafkaProducerService producerService;

    public KafkaProducerController(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    // 1. Simple Publish to Standard Topic
    @PostMapping("/simple")
    public CompletableFuture<PublishResponse> publishSimple(@RequestBody OrderEvent event) {
        String key = event.customerId() != null ? event.customerId() : UUID.randomUUID().toString();
        return producerService.sendAsync("orders.standard", key, event);
    }

    // 2. Publish to Specific Partition with Tracing Headers
    @PostMapping("/partitioned")
    public CompletableFuture<PublishResponse> publishPartitioned(
            @RequestParam(defaultValue = "0") int partition,
            @RequestBody OrderEvent event) {
        String key = event.customerId() != null ? event.customerId() : "key-" + partition;
        return producerService.sendWithHeaders("orders.standard", partition, key, event);
    }

    // 3. Publish to Filtered High-Priority Topic
    @PostMapping("/priority")
    public CompletableFuture<PublishResponse> publishPriority(@RequestBody OrderEvent event) {
        String key = event.orderId();
        return producerService.sendAsync("orders.high-priority", key, event);
    }

    // 4. Publish to Retryable Topic (Simulates 2 retries -> Dead Letter Topic)
    @PostMapping("/retry-dlt")
    public CompletableFuture<PublishResponse> publishRetryDlt(
            @RequestParam(defaultValue = "true") boolean fail,
            @RequestBody OrderEvent event) {
        OrderEvent testEvent = new OrderEvent(
                event.orderId(),
                event.customerId(),
                event.skuCode(),
                event.quantity(),
                event.price(),
                event.priority(),
                fail,
                null
        );
        return producerService.sendAsync("orders.retryable", event.orderId(), testEvent);
    }

    // 5. Batch Publish
    @PostMapping("/batch")
    public ResponseEntity<String> publishBatch(@RequestBody List<OrderEvent> events) {
        for (OrderEvent event : events) {
            producerService.sendAsync("orders.batch", event.orderId(), event);
        }
        return ResponseEntity.ok("Dispatched " + events.size() + " events to orders.batch topic");
    }

    // 6. Transactional Multi-Message Publish
    @PostMapping("/transaction")
    public ResponseEntity<String> publishTransaction(@RequestBody List<OrderEvent> events) {
        boolean committed = producerService.sendInTransaction(
                "events.transactional",
                "tx-batch",
                events.toArray(new OrderEvent[0])
        );
        return committed
                ? ResponseEntity.ok("Successfully committed transaction with " + events.size() + " events")
                : ResponseEntity.internalServerError().body("Transaction rollback occurred");
    }
}
