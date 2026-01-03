package com.example.demo.controller;

import com.example.demo.service.JInterfaceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/reservations")
public class ReservationController {

    private static final Logger log = LoggerFactory.getLogger(ReservationController.class);

    private final JInterfaceClient client;
    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final String confirmTopic;

    public ReservationController(JInterfaceClient client,
                                 KafkaTemplate<Object, Object> kafkaTemplate,
                                 @Value("${app.kafka.confirm-topic}") String confirmTopic) {
        this.client = client;
        this.kafkaTemplate = kafkaTemplate;
        this.confirmTopic = confirmTopic;
    }

    public record ConfirmRequest(String userId, String holdId) {}
    public record ConfirmEvent(String userId, String eventId, String seatId, String orderId, String error) {}

    @PostMapping("/confirm")
    public ResponseEntity<?> confirm(@RequestBody ConfirmRequest request) throws Exception {
        if (request == null || isBlank(request.userId()) || isBlank(request.holdId())) {
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "error",
                    "code", HttpStatus.BAD_REQUEST.value(),
                    "error", "userId and holdId are required"
            ));
        }

        var res = client.confirmHold(request.userId(), request.holdId());
        if (res.ok()) {
            publishConfirmEvent(request.userId(), res.eventId(), res.seatId(), res.orderId(), null);
            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "code", HttpStatus.OK.value(),
                    "data", Map.of("orderId", res.orderId())
            ));
        }

        publishConfirmEvent(request.userId(), res.eventId(), res.seatId(), null, res.error());
        HttpStatus status = mapErrorToStatus(res.error());
        return ResponseEntity.status(status).body(Map.of(
                "status", "error",
                "code", status.value(),
                "error", res.error()
        ));
    }

    private void publishConfirmEvent(String userId, String eventId, String seatId, String orderId, String error) {
        ConfirmEvent event = new ConfirmEvent(userId, eventId, seatId, orderId, error);
        kafkaTemplate.send(confirmTopic, userId, event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.warn("Failed to publish confirm event for userId={} holdResult orderId={} error={}",
                                userId, orderId, error, ex);
                    }
                });
    }

    private static HttpStatus mapErrorToStatus(String error) {
        if (error == null) {
            return HttpStatus.BAD_GATEWAY;
        }
        return switch (error) {
            case "hold_expired", "seat_already_sold" -> HttpStatus.CONFLICT;
            case "user_mismatch" -> HttpStatus.FORBIDDEN;
            case "hold_not_found" -> HttpStatus.NOT_FOUND;
            default -> HttpStatus.BAD_GATEWAY;
        };
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
