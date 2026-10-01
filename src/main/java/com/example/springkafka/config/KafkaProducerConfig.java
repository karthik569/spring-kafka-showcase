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
}
