package com.example.chatconsumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public final class ChatConsumer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        String bootstrap = getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String confirmTopic = getenv("KAFKA_CONFIRM_TOPIC", "reservation.confirmations");
        String expiredTopic = getenv("KAFKA_EXPIRED_TOPIC", "hold.expired");
        String webhook = System.getenv("GOOGLE_CHAT_WEBHOOK_URL");
        System.out.println("[ChatConsumer] Config: bootstrap=" + bootstrap + ", confirmTopic=" + confirmTopic + ", expiredTopic=" + expiredTopic + ", webhook=" + (webhook != null ? "SET" : "NOT_SET"));
        if (webhook == null || webhook.isBlank()) {
            System.err.println("GOOGLE_CHAT_WEBHOOK_URL is not set; exiting.");
            System.exit(1);
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "chat-consumer-" + System.currentTimeMillis());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000");

        HttpClient http = HttpClient.newHttpClient();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            System.out.println("[ChatConsumer] Subscribing to topics: " + List.of(confirmTopic, expiredTopic));
            consumer.subscribe(List.of(confirmTopic, expiredTopic));
            System.out.println("Listening on " + bootstrap);
            System.out.println("  - confirmations: " + confirmTopic);
            System.out.println("  - hold expired: " + expiredTopic);
            
            // Poll multiple times to trigger partition assignment
            System.out.println("[ChatConsumer] Waiting for partition assignment...");
            for (int i = 0; i < 10; i++) {
                consumer.poll(java.time.Duration.ofMillis(500));
                if (!consumer.assignment().isEmpty()) {
                    System.out.println("[ChatConsumer] Got assignment: " + consumer.assignment());
                    break;
                }
            }
            
            // Seek to beginning if we have partitions
            if (!consumer.assignment().isEmpty()) {
                System.out.println("[ChatConsumer] Seeking to beginning of all partitions...");
                consumer.seekToBeginning(consumer.assignment());
                System.out.println("[ChatConsumer] Seeked to beginning");
            } else {
                System.err.println("[ChatConsumer] ERROR: No partitions assigned!");
            }

            long pollCount = 0;
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                if (++pollCount % 5 == 0) {
                    System.out.println("[ChatConsumer] Poll #" + pollCount + " - got " + records.count() + " messages, assigned: " + consumer.assignment());
                }
                if (records.count() > 0) {
                    System.out.println("[ChatConsumer] Received " + records.count() + " messages");
                }
                records.forEach(record -> {
                    System.out.println("[ChatConsumer] Processing topic=" + record.topic() + " value=" + record.value());
                    String text = buildMessage(record.topic(), record.value());
                    System.out.println("[ChatConsumer] Message text: " + text);
                    postToChat(http, webhook, text);
                });
            }
        }
    }

    private static String buildMessage(String topic, String json) {
        System.out.println("[ChatConsumer] Raw JSON input: " + json);
        System.out.println("[ChatConsumer] JSON length: " + json.length() + ", first char: " + (json.length() > 0 ? json.charAt(0) : "EMPTY"));
        try {
            JsonNode root = MAPPER.readTree(json);
            java.util.List<String> fieldNames = new java.util.ArrayList<>();
            root.fieldNames().forEachRemaining(fieldNames::add);
            System.out.println("[ChatConsumer] Parsed JSON - fields: " + fieldNames + ", root type: " + root.getNodeType());
            
            // Handle hold.expired events
            if (topic.contains("expired")) {
                String userId = getText(root, "userId");
                String eventId = getText(root, "eventId");
                String seatId = getText(root, "seatId");
                String holdId = getText(root, "holdId");
                System.out.println("[ChatConsumer] Extracted - userId=" + userId + ", eventId=" + eventId + ", seatId=" + seatId + ", holdId=" + holdId);
                
                return "[Email Simulation] Hold expired: Your hold for seat " + seatId 
                        + " at event " + eventId + " has expired. HoldId: " + holdId 
                        + " (userId: " + userId + ")";
            }
            
            // Handle reservation.confirmations events
            String userId = getText(root, "userId");
            String eventId = getText(root, "eventId");
            String seatId = getText(root, "seatId");
            String orderId = getText(root, "orderId");
            String error = getText(root, "error");

            if (orderId != null && !orderId.isBlank()) {
                return "Reservation confirmed: userId=" + userId
                        + ", eventId=" + eventId
                        + ", seatId=" + seatId
                        + ", orderId=" + orderId;
            }
            return "Reservation confirm failed: userId=" + userId
                    + ", eventId=" + eventId
                    + ", seatId=" + seatId
                    + ", error=" + error;
        } catch (Exception e) {
            System.err.println("[ChatConsumer] Error parsing JSON: " + e.getMessage());
            return "Event received (raw): " + json;
        }
    }

    private static String getText(JsonNode root, String field) {
        JsonNode node = root.get(field);
        return node == null || node.isNull() ? null : node.asText();
    }

    private static void postToChat(HttpClient http, String webhook, String text) {
        System.out.println("[ChatConsumer] Posting to webhook: " + text);
        String body;
        try {
            body = MAPPER.writeValueAsString(new TextPayload(text));
        } catch (Exception e) {
            System.err.println("Failed to serialize webhook payload: " + e.getMessage());
            return;
        }

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(webhook))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        try {
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() >= 300) {
                System.err.println("Webhook error " + resp.statusCode() + ": " + resp.body());
            } else {
                System.out.println("[ChatConsumer] Webhook sent successfully: " + resp.statusCode());
            }
        } catch (IOException | InterruptedException e) {
            System.err.println("Webhook send failed: " + e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    private static String getenv(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    private record TextPayload(String text) {}
}
