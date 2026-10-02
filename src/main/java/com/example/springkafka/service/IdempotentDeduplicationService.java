package com.example.springkafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service implementing Idempotent Consumer pattern via a sliding-window deduplication store.
 * <p>
 * Ensures that duplicate messages delivered across network retries or consumer restarts
 * are intercepted and suppressed without executing redundant business logic.
 */
@Service
public class IdempotentDeduplicationService {

    private static final Logger log = LoggerFactory.getLogger(IdempotentDeduplicationService.class);

    public record DedupEntry(String idempotencyKey, String payloadHash, long timestamp, String status) {}

    // In-memory sliding window cache: idempotencyKey -> DedupEntry
    private final Map<String, DedupEntry> processedCache = new ConcurrentHashMap<>();
    private final List<DedupEntry> duplicateAttempts = new java.util.concurrent.CopyOnWriteArrayList<>();

    /**
     * Checks if the message key has already been processed.
     *
     * @param idempotencyKey unique key (e.g. orderId or header Idempotency-Key)
     * @param payloadHash checksum or summary of payload
     * @return true if this is the FIRST time seeing this message (execute logic); false if it is a DUPLICATE (skip logic)
     */
    public boolean checkAndSet(String idempotencyKey, String payloadHash) {
        if (idempotencyKey == null) return true;

        if (processedCache.containsKey(idempotencyKey)) {
            log.warn("[DEDUP-INTERCEPTOR] Detected duplicate message for key={}! Suppressing duplicate business action.", idempotencyKey);
            duplicateAttempts.add(new DedupEntry(idempotencyKey, payloadHash, System.currentTimeMillis(), "DUPLICATE_SUPPRESSED"));
            return false;
        }

        DedupEntry entry = new DedupEntry(idempotencyKey, payloadHash, System.currentTimeMillis(), "FIRST_TIME_PROCESSED");
        processedCache.put(idempotencyKey, entry);
        log.debug("[DEDUP-INTERCEPTOR] Registered new idempotency key={}", idempotencyKey);
        return true;
    }

    public boolean isProcessed(String idempotencyKey) {
        return processedCache.containsKey(idempotencyKey);
    }

    public List<DedupEntry> getAllProcessedKeys() {
        return new ArrayList<>(processedCache.values());
    }

    public List<DedupEntry> getDuplicateAttempts() {
        return duplicateAttempts;
    }
}
