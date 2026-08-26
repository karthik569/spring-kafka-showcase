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
}
