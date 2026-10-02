package com.example.springkafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Custom Kafka Partitioner implementing deterministic, region-based routing.
 * <p>
 * Routes records based on key prefixes or customer attributes:
 * <ul>
 *   <li>Key starting with "US-" -> Partition 0</li>
 *   <li>Key starting with "EU-" -> Partition 1</li>
 *   <li>Key starting with "APAC-" or other -> Partition 2</li>
 * </ul>
 */
public class RegionAwarePartitioner implements Partitioner {

    private static final Logger log = LoggerFactory.getLogger(RegionAwarePartitioner.class);

    @Override
    public void configure(Map<String, ?> configs) {
        // Configuration parameters if any
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = (partitions != null && !partitions.isEmpty()) ? partitions.size() : 3;

        if (key == null) {
            return 0;
        }

        String keyStr = key.toString().toUpperCase();
        int assignedPartition;

        if (keyStr.startsWith("US-")) {
            assignedPartition = 0;
        } else if (keyStr.startsWith("EU-")) {
            assignedPartition = 1 % numPartitions;
        } else if (keyStr.startsWith("APAC-")) {
            assignedPartition = 2 % numPartitions;
        } else {
            // Consistent fallback hashing
            assignedPartition = Math.abs(keyStr.hashCode()) % numPartitions;
        }

        log.debug("[REGION-PARTITIONER] Key '{}' routed deterministically to partition {}", keyStr, assignedPartition);
        return assignedPartition;
    }

    @Override
    public void close() {
        // Cleanup resources
    }
}
