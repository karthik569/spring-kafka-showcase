package com.example.springkafka.controller;

import com.example.springkafka.dto.AuditRecord;
import com.example.springkafka.service.KafkaConsumerService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/kafka/audit")
public class KafkaAuditController {

    private final KafkaConsumerService consumerService;

    public KafkaAuditController(KafkaConsumerService consumerService) {
        this.consumerService = consumerService;
    }

    @GetMapping("/received")
    public List<AuditRecord> getReceivedMessages() {
        return consumerService.getReceivedRecords();
    }

    @GetMapping("/dlt")
    public List<AuditRecord> getDeadLetterMessages() {
        return consumerService.getDltRecords();
    }

    @GetMapping("/summary")
    public Map<String, Object> getSummary() {
        return Map.of(
                "totalReceived", consumerService.getReceivedRecords().size(),
                "totalDeadLetter", consumerService.getDltRecords().size()
        );
    }
}
