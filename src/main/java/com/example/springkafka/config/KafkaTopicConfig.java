package com.example.springkafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic standardOrdersTopic() {
        return TopicBuilder.name("orders.standard")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic highPriorityOrdersTopic() {
        return TopicBuilder.name("orders.high-priority")
                .partitions(2)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic retryableOrdersTopic() {
        return TopicBuilder.name("orders.retryable")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic deadLetterOrdersTopic() {
        return TopicBuilder.name("orders.retryable.DLT")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic batchOrdersTopic() {
        return TopicBuilder.name("orders.batch")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic transactionalEventsTopic() {
        return TopicBuilder.name("events.transactional")
                .partitions(2)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic nonblockingOrdersTopic() {
        return TopicBuilder.name("orders.nonblocking")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic manualAckOrdersTopic() {
        return TopicBuilder.name("orders.manual-ack")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic compactedInventoryTopic() {
        return TopicBuilder.name("inventory.compacted")
                .partitions(3)
                .replicas(1)
                .config(org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG,
                        org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_COMPACT)
                .config(org.apache.kafka.common.config.TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "0")
                .config(org.apache.kafka.common.config.TopicConfig.SEGMENT_MS_CONFIG, "10000")
                .build();
    }

    @Bean
    public NewTopic streamsInputTopic() {
        return TopicBuilder.name("streams.orders.input")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic streamsOutputTopic() {
        return TopicBuilder.name("streams.analytics.output")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic ordersAvroTopic() {
        return TopicBuilder.name("orders.avro")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic outboxCdcTopic() {
        return TopicBuilder.name("orders.outbox.cdc")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic joinedOrdersTopic() {
        return TopicBuilder.name("orders.joined.output")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic encryptedOrdersTopic() {
        return TopicBuilder.name("orders.encrypted")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic tieredRetentionTopic() {
        return TopicBuilder.name("orders.tiered.retention")
                .partitions(3)
                .replicas(1)
                .config(org.apache.kafka.common.config.TopicConfig.RETENTION_MS_CONFIG, "604800000") // 7 days
                .config(org.apache.kafka.common.config.TopicConfig.SEGMENT_BYTES_CONFIG, "10485760") // 10MB segments
                .build();
    }

    @Bean
    public NewTopic eventSourcedTopic() {
        return TopicBuilder.name("events.sourced")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic sagaOrdersCommandsTopic() {
        return TopicBuilder.name("saga.orders.commands")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic sagaInventoryCommandsTopic() {
        return TopicBuilder.name("saga.inventory.commands")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic sagaCompletedTopic() {
        return TopicBuilder.name("saga.events.completed")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
