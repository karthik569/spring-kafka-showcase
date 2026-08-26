package com.example.springkafka.dto;

public record PublishResponse(
        String topic,
        int partition,
        long offset,
        String key,
        String status,
        String timestamp
) {}
