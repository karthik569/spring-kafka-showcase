package com.example.springkafka.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;
import io.swagger.v3.oas.models.tags.Tag;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Spring Boot Apache Kafka Showcase API")
                        .version("1.0.0")
                        .description("Comprehensive REST API and testing workbench for Spring Boot Apache Kafka capabilities: "
                                + "simple asynchronous publishing, partition-directed routing with tracing headers, "
                                + "VIP priority record filtering, resilient Dead Letter Topic (DLT) retry pipelines, "
                                + "high-throughput batching, and atomic Kafka transactions.")
                        .contact(new Contact()
                                .name("Spring Kafka Showcase")
                                .url("https://github.com/spring-projects/spring-kafka"))
                        .license(new License()
                                .name("Apache 2.0")
                                .url("https://www.apache.org/licenses/LICENSE-2.0.html")))
                .servers(List.of(
                        new Server().url("http://localhost:8080").description("Local Live Server")
                ))
                .tags(List.of(
                        new Tag().name("Kafka Producer API")
                                .description("Publish messages to various Kafka topics with routing, partition targeting, retries, batches, and transactions"),
                        new Tag().name("Kafka Audit API")
                                .description("Inspect received, dead-letter, and statistical records processed by the Kafka consumer listeners")
                ));
    }
}
