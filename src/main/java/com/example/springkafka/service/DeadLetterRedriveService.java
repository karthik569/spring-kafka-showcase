package com.example.springkafka.service;

import com.example.springkafka.dto.AuditRecord;
import com.example.springkafka.dto.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Service managing Dead Letter Topics (DLT), providing inspection and administrative
 * Re-drive (replay) capabilities to recover and fix failed poison messages.
 */
@Service
public class DeadLetterRedriveService {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterRedriveService.class);

    private final KafkaProducerService producerService;
    private final KafkaConsumerService consumerService;
    private final List<Map<String, Object>> redriveHistory = new CopyOnWriteArrayList<>();

    public DeadLetterRedriveService(KafkaProducerService producerService, KafkaConsumerService consumerService) {
        this.producerService = producerService;
        this.consumerService = consumerService;
    }

    /**
     * Re-drives (re-publishes) a poison message from the DLT back to its target destination topic.
     *
     * @param orderId the orderId of the dead letter
     * @param targetTopic target topic to re-dispatch to (defaults to orders.standard)
     * @param clearFailureFlag whether to remove simulateFailure=true so the re-drive succeeds
     * @return summary of the redrive operation
     */
    public Optional<Map<String, Object>> redriveDeadLetter(String orderId, String targetTopic, boolean clearFailureFlag) {
        List<AuditRecord> dltList = consumerService.getDeadLetterRecords();

        for (AuditRecord deadRecord : dltList) {
            if (deadRecord.payload() != null && orderId.equals(deadRecord.payload().orderId())) {
                OrderEvent original = deadRecord.payload();
                OrderEvent redrivenEvent = clearFailureFlag
                        ? new OrderEvent(original.orderId(), original.customerId(), original.skuCode(), original.quantity(), original.price(), original.priority(), false, java.time.Instant.now().toString())
                        : original.withTimestamp();

                String dest = (targetTopic != null && !targetTopic.isEmpty()) ? targetTopic : "orders.standard";
                log.info("[DLT-REDRIVE] Re-driving failed orderId={} to targetTopic={}", orderId, dest);

                producerService.sendAsync(dest, redrivenEvent.customerId(), redrivenEvent);

                Map<String, Object> audit = new LinkedHashMap<>();
                audit.put("redriveId", "RDRV-" + UUID.randomUUID().toString().substring(0, 8));
                audit.put("orderId", orderId);
                audit.put("sourceTopic", deadRecord.topic());
                audit.put("targetTopic", dest);
                audit.put("clearedFailureFlag", clearFailureFlag);
                audit.put("redrivenAt", java.time.Instant.now().toString());

                redriveHistory.add(audit);
                return Optional.of(audit);
            }
        }

        return Optional.empty();
    }

    public List<Map<String, Object>> getRedriveHistory() {
        return redriveHistory;
    }
}
