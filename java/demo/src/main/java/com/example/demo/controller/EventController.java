package com.example.demo.controller;

import com.example.demo.service.JInterfaceClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class EventController {

    private final JInterfaceClient client;

    public EventController(JInterfaceClient client) {
        this.client = client;
    }

    @GetMapping("/api/events/{eventId}/seats")
    public ResponseEntity<?> listEventSeats(@PathVariable String eventId) throws Exception {
        var res = client.listEventSeats(eventId);
        if (res.ok()) {
            return ResponseEntity.ok(Map.of(
                    "status", "OK",
                    "eventId", eventId,
                    "seats", res.seats()
            ));
        }
        return ResponseEntity.status(502).body(Map.of(
                "status", "FAILED",
                "error", res.error(),
                "correlationId", res.correlationId()
        ));
    }
}
