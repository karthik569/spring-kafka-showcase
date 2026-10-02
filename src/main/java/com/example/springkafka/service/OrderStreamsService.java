package com.example.springkafka.service;

import com.example.springkafka.dto.OrderEvent;
import com.example.springkafka.dto.StreamAnalytics;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service orchestrating an embedded Kafka Streams topology.
 * <p>
 * Demonstrates:
 * <ul>
 *   <li>Real-time event streaming ingestion from {@code streams.orders.input}</li>
 *   <li>Key re-partitioning / grouping by {@code customerId}</li>
 *   <li>Stateful aggregation of cumulative order count and total spend</li>
 *   <li>Materialization into an in-memory key-value state store (robust across ARM64/PRoot architectures)</li>
 *   <li>Continuous emission of aggregated {@link StreamAnalytics} to {@code streams.analytics.output}</li>
 *   <li>Interactive queries against the local materialized state store</li>
 * </ul>
 *
 * @author Spring Showcase Team
 * @version 1.0
 */
@Service
public class OrderStreamsService {

    private static final Logger log = LoggerFactory.getLogger(OrderStreamsService.class);
    public static final String ANALYTICS_STORE_NAME = "customer-analytics-store";
    public static final String WINDOW_STORE_NAME = "windowed-analytics-store";

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${app.kafka.topics.streams-input:streams.orders.input}")
    private String inputTopic;

    @Value("${app.kafka.topics.streams-output:streams.analytics.output}")
    private String outputTopic;

    @Value("${app.kafka.topics.joined-output:orders.joined.output}")
    private String joinedOutputTopic;

    private final ObjectMapper objectMapper;
    private KafkaStreams kafkaStreams;
    private final Map<String, StreamAnalytics> inMemoryFallBackCache = new ConcurrentHashMap<>();
    private final Map<String, StreamAnalytics> inMemoryWindowCache = new ConcurrentHashMap<>();
    private final List<com.example.springkafka.dto.EnrichedOrderEvent> inMemoryJoinedCache = new java.util.concurrent.CopyOnWriteArrayList<>();

    public OrderStreamsService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void startTopology() {
        try {
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "spring-kafka-showcase-streams");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
            props.put("spring.json.trusted.packages", "*");
            // Use in-memory state stores to ensure 100% portable execution on ARM64 / PRoot without RocksDB C++ dependencies
            props.put(StreamsConfig.DEFAULT_DSL_STORE_CONFIG, StreamsConfig.IN_MEMORY);
            props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

            StreamsBuilder builder = new StreamsBuilder();

            JsonSerde<OrderEvent> orderSerde = new JsonSerde<>(OrderEvent.class, objectMapper);
            orderSerde.ignoreTypeHeaders();
            if (orderSerde.deserializer() != null) {
                orderSerde.deserializer().addTrustedPackages("*");
            }

            JsonSerde<StreamAnalytics> analyticsSerde = new JsonSerde<>(StreamAnalytics.class, objectMapper);
            analyticsSerde.ignoreTypeHeaders();
            if (analyticsSerde.deserializer() != null) {
                analyticsSerde.deserializer().addTrustedPackages("*");
            }

            KStream<String, OrderEvent> stream = builder.stream(
                    inputTopic,
                    Consumed.with(Serdes.String(), orderSerde)
            );

            stream
                    // Map key to customerId if not already keyed by customerId
                    .map((key, order) -> {
                        String custKey = (order != null && order.customerId() != null) ? order.customerId() : (key != null ? key : "UNKNOWN");
                        return new KeyValue<>(custKey, order);
                    })
                    .filter((custKey, order) -> order != null)
                    .groupByKey(org.apache.kafka.streams.kstream.Grouped.with(Serdes.String(), orderSerde))
                    .aggregate(
                            // Initializer
                            () -> new StreamAnalytics("INIT", 0, 0.0, 0.0, Instant.now().toString()),
                            // Aggregator
                            (customerId, newOrder, currentAgg) -> {
                                long count = currentAgg.orderCount() + 1;
                                double total = currentAgg.totalSpend() + (newOrder.price() * newOrder.quantity());
                                double avg = count > 0 ? (total / count) : 0.0;
                                StreamAnalytics updated = new StreamAnalytics(
                                        customerId,
                                        count,
                                        Math.round(total * 100.0) / 100.0,
                                        Math.round(avg * 100.0) / 100.0,
                                        Instant.now().toString()
                                );
                                inMemoryFallBackCache.put(customerId, updated);
                                return updated;
                            },
                            Materialized.<String, StreamAnalytics>as(
                                    Stores.inMemoryKeyValueStore(ANALYTICS_STORE_NAME)
                            ).withKeySerde(Serdes.String()).withValueSerde(analyticsSerde)
                    )
                    .toStream()
                    .to(outputTopic, Produced.with(Serdes.String(), analyticsSerde));

            // Windowed Stream Topology (1-minute tumbling window)
            stream
                    .map((key, order) -> {
                        String custKey = (order != null && order.customerId() != null) ? order.customerId() : (key != null ? key : "UNKNOWN");
                        return new KeyValue<>(custKey, order);
                    })
                    .filter((custKey, order) -> order != null)
                    .groupByKey(org.apache.kafka.streams.kstream.Grouped.with(Serdes.String(), orderSerde))
                    .windowedBy(org.apache.kafka.streams.kstream.TimeWindows.ofSizeWithNoGrace(java.time.Duration.ofMinutes(1)))
                    .aggregate(
                            () -> new StreamAnalytics("WINDOW_INIT", 0, 0.0, 0.0, Instant.now().toString()),
                            (customerId, newOrder, currentAgg) -> {
                                long count = currentAgg.orderCount() + 1;
                                double total = currentAgg.totalSpend() + (newOrder.price() * newOrder.quantity());
                                double avg = count > 0 ? (total / count) : 0.0;
                                StreamAnalytics updated = new StreamAnalytics(
                                        customerId,
                                        count,
                                        Math.round(total * 100.0) / 100.0,
                                        Math.round(avg * 100.0) / 100.0,
                                        Instant.now().toString()
                                );
                                inMemoryWindowCache.put(customerId, updated);
                                return updated;
                            },
                            Materialized.<String, StreamAnalytics>as(
                                    Stores.inMemoryWindowStore(WINDOW_STORE_NAME, java.time.Duration.ofMinutes(5), java.time.Duration.ofMinutes(1), false)
                            ).withKeySerde(Serdes.String()).withValueSerde(analyticsSerde)
                    );

            kafkaStreams = new KafkaStreams(builder.build(), props);
            kafkaStreams.setUncaughtExceptionHandler(ex -> {
                log.error("[KAFKA-STREAMS] Uncaught exception in stream thread: ", ex);
                return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
            });

            kafkaStreams.start();
            log.info("[KAFKA-STREAMS] Topology successfully started listening on inputTopic={} outputTopic={}",
                    inputTopic, outputTopic);

        } catch (Exception e) {
            log.warn("[KAFKA-STREAMS] Streams topology failed to start immediately (broker may still be starting): {}", e.getMessage());
        }
    }

    @PreDestroy
    public void stopTopology() {
        if (kafkaStreams != null) {
            log.info("[KAFKA-STREAMS] Closing Kafka Streams topology...");
            kafkaStreams.close();
        }
    }

    /**
     * Queries the local materialized in-memory state store for customer analytics.
     */
    public Optional<StreamAnalytics> getAnalyticsForCustomer(String customerId) {
        if (kafkaStreams != null && kafkaStreams.state().isRunningOrRebalancing()) {
            try {
                ReadOnlyKeyValueStore<String, StreamAnalytics> store = kafkaStreams.store(
                        org.apache.kafka.streams.StoreQueryParameters.fromNameAndType(
                                ANALYTICS_STORE_NAME,
                                QueryableStoreTypes.keyValueStore()
                        )
                );
                StreamAnalytics result = store.get(customerId);
                if (result != null) {
                    return Optional.of(result);
                }
            } catch (Exception e) {
                log.debug("[KAFKA-STREAMS] State store not queryable yet: {}", e.getMessage());
            }
        }
        return Optional.ofNullable(inMemoryFallBackCache.get(customerId));
    }

    /**
     * Retrieves all analytics records across all customers from the state store.
     */
    public List<StreamAnalytics> getAllAnalytics() {
        Map<String, StreamAnalytics> all = new LinkedHashMap<>(inMemoryFallBackCache);
        if (kafkaStreams != null && kafkaStreams.state().isRunningOrRebalancing()) {
            try {
                ReadOnlyKeyValueStore<String, StreamAnalytics> store = kafkaStreams.store(
                        org.apache.kafka.streams.StoreQueryParameters.fromNameAndType(
                                ANALYTICS_STORE_NAME,
                                QueryableStoreTypes.keyValueStore()
                        )
                );
                try (KeyValueIterator<String, StreamAnalytics> it = store.all()) {
                    while (it.hasNext()) {
                        KeyValue<String, StreamAnalytics> kv = it.next();
                        all.put(kv.key, kv.value);
                    }
                }
            } catch (Exception e) {
                log.debug("[KAFKA-STREAMS] State store all() not queryable yet: {}", e.getMessage());
            }
        }
        return new ArrayList<>(all.values());
    }

    public List<StreamAnalytics> getAllWindowedAnalytics() {
        return new ArrayList<>(inMemoryWindowCache.values());
    }

    public Optional<StreamAnalytics> getWindowedAnalyticsForCustomer(String customerId) {
        return Optional.ofNullable(inMemoryWindowCache.get(customerId));
    }

    public void recordJoinedEnrichment(com.example.springkafka.dto.EnrichedOrderEvent event) {
        inMemoryJoinedCache.add(event);
    }

    public List<com.example.springkafka.dto.EnrichedOrderEvent> getAllJoinedEnrichments() {
        return Collections.unmodifiableList(inMemoryJoinedCache);
    }

    public String getStreamsState() {
        return kafkaStreams != null ? kafkaStreams.state().name() : "NOT_INITIALIZED";
    }
}
