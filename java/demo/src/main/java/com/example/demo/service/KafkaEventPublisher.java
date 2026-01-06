package com.example.demo.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String holdExpiredTopic;

    public KafkaEventPublisher(KafkaTemplate<String, String> kafkaTemplate,
                               ObjectMapper objectMapper,
                               @Value("${app.kafka.hold-expired-topic:hold.expired}") String holdExpiredTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.holdExpiredTopic = holdExpiredTopic;
    }

    public void publishHoldExpired(String eventId, String seatId, String userId, String holdId, Long expiredAt) {
        try {
            HoldExpiredEvent event = new HoldExpiredEvent(eventId, seatId, userId, holdId, expiredAt);
            String eventJson = objectMapper.writeValueAsString(event);
            
            kafkaTemplate.send(holdExpiredTopic, userId, eventJson)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to publish hold_expired event for userId={} holdId={}", 
                                    userId, holdId, ex);
                        } else {
                            log.info("Published hold_expired event: userId={} eventId={} seatId={} holdId={}", 
                                    userId, eventId, seatId, holdId);
                        }
                    });
        } catch (Exception e) {
            log.error("Error serializing hold_expired event", e);
        }
    }

    public record HoldExpiredEvent(String eventId, 
                                   String seatId, 
                                   String userId, 
                                   String holdId, 
                                   Long expiredAt) {}
}
