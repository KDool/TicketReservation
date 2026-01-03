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
        ));    }

    // GET /hold/check -> check if a hold is still active
    @GetMapping("/hold/check")
    public ResponseEntity<?> checkHold(@RequestParam String eventId,
                                       @RequestParam String seatId) throws Exception {
        long now = System.currentTimeMillis();
        var res = client.checkSeat(eventId, seatId);

        if (res.ok()) {
            return ResponseEntity.ok(Map.of(
                    "status", "OK",
                    "eventId", eventId,
                    "seatId", seatId,
                    "seatState", res.seatState(),
                    "userId", res.userId(),
                    "holdId", res.holdId(),
                    "expiresAtMillis", res.expiresAtMillis(),
                    "isExpired", res.expiresAtMillis() != null && res.expiresAtMillis() <= now,
                    "correlationId", res.correlationId()
            ));
        }
        return ResponseEntity.status(502).body(Map.of(
                "status", "FAILED",
                "error", res.error(),
                "correlationId", res.correlationId()
        ));
    }    }
    