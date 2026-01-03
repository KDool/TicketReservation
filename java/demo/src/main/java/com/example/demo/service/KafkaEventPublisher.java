package com.example.demo.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import tools.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

@Component
public class KafkaEventPublisher {

    private static final String HOLD_EXPIRED_TOPIC = "seat-hold-expired";
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public KafkaEventPublisher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void publishHoldExpired(String eventId, String seatId, String userId, String holdId, long expiresAtMs) {
        try {
            Map<String, Object> event = new HashMap<>();
            event.put("eventType", "HoldExpired");
            event.put("eventId", eventId);
            event.put("seatId", seatId);
            event.put("userId", userId);
            event.put("holdId", holdId);
            event.put("expiresAtMs", expiresAtMs);
            event.put("timestamp", System.currentTimeMillis());

            String payload = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(HOLD_EXPIRED_TOPIC, holdId, payload);
            System.out.println("[Kafka] Published HoldExpired: " + payload);
        } catch (Exception e) {
            System.err.println("[Kafka] Failed to publish HoldExpired: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
