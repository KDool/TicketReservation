package com.example.demo.controller;

import com.example.demo.service.JInterfaceClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.Map;

@RestController
public class HoldController {

    private final JInterfaceClient client;

    public HoldController(JInterfaceClient client) {
        this.client = client;
    }

    // POST /hold -> writes seat "E1","A1" as held
    @PostMapping("/hold")
    public ResponseEntity<?> hold(@RequestParam(defaultValue = "E1") String eventId,
                                  @RequestParam(defaultValue = "A1") String seatId,
                                  @RequestParam(defaultValue = "user1") String userId,
                                  @RequestParam(defaultValue = "30") long holdSeconds) throws Exception {

        if (holdSeconds <= 0) {
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "FAILED",
                    "error", "holdSeconds must be > 0"
            ));
        }

        var res = client.writeHold(eventId, seatId, userId, Duration.ofSeconds(holdSeconds));

        if (res.ok()) {
            return ResponseEntity.ok(Map.of(
                    "status", "OK",
                    "eventId", eventId,
                    "seatId", seatId,
                    "userId", userId,
                    "holdId", res.holdId(),
                    "expiresAtMillis", res.expiresAtMillis(),
                    "correlationId", res.correlationId(),
                    "reply", res.rawReply()
            ));
        }
        return ResponseEntity.status(502).body(Map.of(
                "status", "FAILED",
                "error", res.error(),
                "correlationId", res.correlationId(),
                "reply", String.valueOf(res.rawReply())
        ));
    }

    // GET /check -> check seat state
    @GetMapping("/check")
    public ResponseEntity<?> checkSeat(@RequestParam(defaultValue = "E1") String eventId,
                                       @RequestParam(defaultValue = "A1") String seatId) throws Exception {
        var res = client.checkSeat(eventId, seatId);
        
        if (res.ok()) {
            return ResponseEntity.ok(Map.of(
                    "status", "OK",
                    "seatStatus", res.status(),
                    "eventId", res.eventId() != null ? res.eventId() : "",
                    "seatId", res.seatId() != null ? res.seatId() : "",
                    "userId", res.userId() != null ? res.userId() : "",
                    "holdId", res.holdId() != null ? res.holdId() : "",
                    "expiresAtMillis", res.expiresAt() != null ? res.expiresAt() : 0,
                    "orderId", res.orderId() != null ? res.orderId() : "",
                    "correlationId", res.correlationId()
            ));
        }
        return ResponseEntity.status(502).body(Map.of(
                "status", "FAILED",
                "error", res.error(),
                "correlationId", res.correlationId()
        ));
    }
}