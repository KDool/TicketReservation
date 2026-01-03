package com.example.chatconsumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public final class ChatConsumer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        String bootstrap = getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String topic = getenv("KAFKA_TOPIC", "reservation.confirmations");
        String webhook = System.getenv("GOOGLE_CHAT_WEBHOOK_URL");
        if (webhook == null || webhook.isBlank()) {
            System.err.println("GOOGLE_CHAT_WEBHOOK_URL is not set; exiting.");
            System.exit(1);
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "chat-consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        HttpClient http = HttpClient.newHttpClient();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            System.out.println("Listening on " + bootstrap + " topic=" + topic);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                records.forEach(record -> {
                    String text = buildMessage(record.value());
                    postToChat(http, webhook, text);
                });
            }
        }
    }

    private static String buildMessage(String json) {
        try {
            JsonNode root = MAPPER.readTree(json);
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
            return "Reservation confirm event (raw): " + json;
        }
    }

    private static String getText(JsonNode root, String field) {
        JsonNode node = root.get(field);
        return node == null || node.isNull() ? null : node.asText();
    }

    private static void postToChat(HttpClient http, String webhook, String text) {
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
