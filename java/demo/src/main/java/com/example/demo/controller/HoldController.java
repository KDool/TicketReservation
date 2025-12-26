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
                                  @RequestParam(defaultValue = "user1") String userId) throws Exception {

        var res = client.writeHold(eventId, seatId, userId, Duration.ofSeconds(3));

        if (res.ok()) {
            return ResponseEntity.ok(Map.of(
                    "status", "OK",
                    "eventId", eventId,
                    "seatId", seatId,
                    "userId", userId,
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
}