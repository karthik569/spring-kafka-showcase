package com.example.springkafka.config;

import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
public class KafkaProducerConfig {

    /**
     * Enables KafkaTemplate to execute both transactional operations (executeInTransaction)
     * and non-transactional fire-and-forget publishes seamlessly on the same template.
     */
    @Bean
    public static BeanPostProcessor kafkaTemplatePostProcessor() {
        return new BeanPostProcessor() {
            @Override
            public Object postProcessAfterInitialization(Object bean, String beanName) {
                if (bean instanceof KafkaTemplate<?, ?> template) {
                    template.setAllowNonTransactional(true);
                }
                return bean;
            }
        };
    }

    @Bean
    @org.springframework.context.annotation.Primary
    public KafkaTemplate<String, Object> kafkaTemplate(
            org.springframework.kafka.core.ProducerFactory<String, Object> producerFactory) {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(producerFactory);
        template.setAllowNonTransactional(true);
        return template;
    }

    /**
     * Dedicated KafkaTemplate for publishing raw binary payloads (e.g. Apache Avro byte arrays).
     */
    @Bean
    public KafkaTemplate<String, byte[]> byteArrayKafkaTemplate(
            org.springframework.boot.autoconfigure.kafka.KafkaProperties kafkaProperties) {
        java.util.Map<String, Object> props = kafkaProperties.buildProducerProperties(null);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArraySerializer.class);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.PARTITIONER_CLASS_CONFIG,
                com.example.springkafka.partitioner.RegionAwarePartitioner.class.getName());

        org.springframework.kafka.core.DefaultKafkaProducerFactory<String, byte[]> factory =
                new org.springframework.kafka.core.DefaultKafkaProducerFactory<>(props);
        KafkaTemplate<String, byte[]> template = new KafkaTemplate<>(factory);
        template.setAllowNonTransactional(true);
        return template;
    }
}
