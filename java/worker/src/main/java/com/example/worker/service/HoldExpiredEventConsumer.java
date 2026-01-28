package com.example.worker.service;

import jakarta.annotation.PostConstruct;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class HoldExpiredEventConsumer {

    @PostConstruct
    public void init() {
        System.out.println("[Worker] HoldExpiredEventConsumer initialized and ready to consume from seat-hold-expired topic");
    }

    @KafkaListener(topics = "seat-hold-expired", groupId = "notification-worker")
    public void handleHoldExpired(String eventPayload) {
        System.out.println("[Worker] Received HoldExpired event: " + eventPayload);
        
        try {
            // Parse JSON event
            Map<String, Object> event = parseEvent(eventPayload);
            
            String eventId = (String) event.get("eventId");
            String seatId = (String) event.get("seatId");
            String userId = (String) event.get("userId");
            String holdId = (String) event.get("holdId");
            long expiresAtMs = ((Number) event.get("expiresAtMs")).longValue();
            
            System.out.println("[Worker] Processing HoldExpired for user=" + userId + " seat=" + eventId + ":" + seatId);
            
            // Simulate email notification
            sendNotification(userId, eventId, seatId, holdId, expiresAtMs);
            
        } catch (Exception e) {
            System.err.println("[Worker] Failed to process event: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void sendNotification(String userId, String eventId, String seatId, String holdId, long expiresAtMs) {
        // Simulate sending email/SMS/push notification
        System.out.println("=".repeat(60));
        System.out.println("[EMAIL NOTIFICATION]");
        System.out.println("To: " + userId + "@example.com");
        System.out.println("Subject: Your seat hold has expired");
        System.out.println("Body:");
        System.out.println("  Hold ID: " + holdId);
        System.out.println("  Event: " + eventId);
        System.out.println("  Seat: " + seatId);
        System.out.println("  Expired at: " + expiresAtMs);
        System.out.println("  Action: Please reserve again if interested");
        System.out.println("=".repeat(60));
    }

    private Map<String, Object> parseEvent(String payload) throws Exception {
        // Simple JSON parsing (you could use ObjectMapper for production)
        // For demo, just log it
        return Map.of(
            "eventId", extractField(payload, "eventId"),
            "seatId", extractField(payload, "seatId"),
            "userId", extractField(payload, "userId"),
            "holdId", extractField(payload, "holdId"),
            "expiresAtMs", Long.parseLong(extractNumericField(payload, "expiresAtMs"))
        );
    }

    private String extractField(String json, String fieldName) {
        String key = "\"" + fieldName + "\":\"";
        int start = json.indexOf(key);
        if (start == -1) return "";
        start += key.length();
        int end = json.indexOf("\"", start);
        return json.substring(start, end);
    }

    private String extractNumericField(String json, String fieldName) {
        String key = "\"" + fieldName + "\":";
        int start = json.indexOf(key);
        if (start == -1) return "0";
        start += key.length();
        int end = start;
        while (end < json.length() && Character.isDigit(json.charAt(end))) {
            end++;
        }
        return json.substring(start, end);
    }
}
