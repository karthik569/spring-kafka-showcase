package com.example.springkafka.service;

import com.example.springkafka.dto.W3cTraceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Service providing OpenTelemetry-compatible W3C Distributed Tracing and Lineage Tracking.
 * <p>
 * Manages traceparent propagation across Kafka Producer records, topic hops, and Consumer listeners,
 * reconstructing the end-to-end distributed transaction graph.
 */
@Service
public class DistributedTracingService {

    private static final Logger log = LoggerFactory.getLogger(DistributedTracingService.class);

    // Trace ID -> Ordered list of trace spans representing transaction hops
    private final Map<String, List<W3cTraceContext>> traceLedger = new ConcurrentHashMap<>();

    public W3cTraceContext startTrace(String serviceName) {
        W3cTraceContext root = W3cTraceContext.newRoot(serviceName);
        recordSpan(root);
        log.info("[DISTRIBUTED-TRACING] Started root traceId={} spanId={} for service={}",
                root.traceId(), root.spanId(), serviceName);
        return root;
    }

    public W3cTraceContext recordHop(String traceparentHeader, String serviceName) {
        W3cTraceContext child = W3cTraceContext.fromTraceParentHeader(traceparentHeader, serviceName);
        recordSpan(child);
        log.info("[DISTRIBUTED-TRACING] Recorded hop traceId={} spanId={} parentSpanId={} in service={}",
                child.traceId(), child.spanId(), child.parentSpanId(), serviceName);
        return child;
    }

    public void recordSpan(W3cTraceContext span) {
        traceLedger.computeIfAbsent(span.traceId(), k -> new CopyOnWriteArrayList<>()).add(span);
    }

    public Optional<List<W3cTraceContext>> getTraceLineage(String traceId) {
        return Optional.ofNullable(traceLedger.get(traceId));
    }

    public Map<String, List<W3cTraceContext>> getAllTraces() {
        return Collections.unmodifiableMap(traceLedger);
    }
}
