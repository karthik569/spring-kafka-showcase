package com.example.springkafka.config;

import com.example.springkafka.dto.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    // 1. Batch Listener Container Factory
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> batchContainerFactory(
            ConsumerFactory<String, OrderEvent> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.setConcurrency(2);
        return factory;
    }

    // 2. Filtered Listener Container Factory (Only processes HIGH priority messages)
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> filterContainerFactory(
            ConsumerFactory<String, OrderEvent> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setRecordFilterStrategy(record -> {
            boolean isHighPriority = record.value() != null && "HIGH".equalsIgnoreCase(record.value().priority());
            if (!isHighPriority) {
                log.info("[KAFKA-FILTER] Discarding non-HIGH priority record key={}", record.key());
                return true; // Return true to FILTER OUT / discard
            }
            return false; // Return false to ACCEPT
        });
        return factory;
    }

    // 3. Resilient Error Handler with Dead Letter Topic (DLT) Publishing
    @Bean
    public DefaultErrorHandler dltErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
        // Retry 2 times with 1000ms delay before sending to Dead Letter Topic
        FixedBackOff backOff = new FixedBackOff(1000L, 2);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.warn("[KAFKA-RETRY] Delivery attempt #{} failed for topic={} offset={} due to: {}",
                        deliveryAttempt, record.topic(), record.offset(), ex.getMessage()));
        return errorHandler;
    }
}
