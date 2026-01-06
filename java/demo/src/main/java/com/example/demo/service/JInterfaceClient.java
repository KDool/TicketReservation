package com.example.demo.service;

import com.ericsson.otp.erlang.*;
import com.example.demo.config.SeatRoutingProperties;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class JInterfaceClient {

    // Erlang registered process
    private static final String REMOTE_REG_NAME = "seat_srv";
    private static final int PING_TIMEOUT_MS = 2000;
    private static final int CONFIRM_TIMEOUT_MS = 3000;
    private static final long DOWN_COOLDOWN_MS = 10_000L;

    private OtpNode node;
    private final SeatRoutingProperties routingProps;
    private final KafkaEventPublisher kafkaEventPublisher;
    private final Map<String, NodeHealth> healthByNode = new ConcurrentHashMap<>();

    public JInterfaceClient(SeatRoutingProperties routingProps, 
                           KafkaEventPublisher kafkaEventPublisher) {
        this.routingProps = routingProps;
        this.kafkaEventPublisher = kafkaEventPublisher;
    }

    @PostConstruct
    public void init() throws IOException {
        // Create exactly ONE node for the whole Spring app lifecycle
        this.node = new OtpNode(routingProps.getLocalNode(), routingProps.getCookie());
        System.out.println("[JInterface] node up: " + node.node());
        System.out.println("[JInterface] ping " + routingProps.getNodeRes1() + " => " + node.ping(routingProps.getNodeRes1(), PING_TIMEOUT_MS));
        System.out.println("[JInterface] ping " + routingProps.getNodeRes2() + " => " + node.ping(routingProps.getNodeRes2(), PING_TIMEOUT_MS));
        System.out.println("[JInterface] ping " + routingProps.getNodeRes3() + " => " + node.ping(routingProps.getNodeRes3(), PING_TIMEOUT_MS));
        
        // Create a persistent mailbox for receiving hold_expired notifications
        OtpMbox notificationMbox = node.createMbox();
        System.out.println("[JInterface] notification mbox: " + notificationMbox.self());
        
        // Register this gateway with all Erlang nodes
        registerWithErlangNodes(notificationMbox);
        
        // Start background thread to listen for hold_expired messages
        startHoldExpiredListener(notificationMbox);
    }
    
    private void registerWithErlangNodes(OtpMbox mbox) {
        List<String> nodes = List.of(
            routingProps.getNodeRes1(),
            routingProps.getNodeRes2(),
            routingProps.getNodeRes3()
        );
        
        for (String remoteNode : nodes) {
            try {
                OtpErlangTuple msg = new OtpErlangTuple(new OtpErlangObject[]{
                    new OtpErlangAtom("register_gateway"),
                    mbox.self()
                });
                System.out.println("[JInterface] sending register_gateway to " + remoteNode + " with mbox " + mbox.self());
                mbox.send(REMOTE_REG_NAME, remoteNode, msg);
                System.out.println("[JInterface] sent register_gateway to " + remoteNode);
            } catch (Exception e) {
                System.err.println("[JInterface] Failed to register with " + remoteNode + ": " + e.getMessage());
            }
        }
    }
    
    private void startHoldExpiredListener(OtpMbox mbox) {
        Thread listener = new Thread(() -> {
            System.out.println("[JInterface] hold_expired listener started on mbox: " + mbox.self());
            while (true) {
                try {
                    OtpErlangObject msg = mbox.receive();
                    if (msg instanceof OtpErlangTuple tup && tup.arity() >= 2) {
                        OtpErlangObject atom = tup.elementAt(0);
                        if (atom instanceof OtpErlangAtom a && "hold_expired".equals(a.atomValue())) {
                            handleHoldExpired(tup.elementAt(1));
                        }
                    }
                } catch (Exception e) {
                    System.err.println("[JInterface] Error in hold_expired listener: " + e.getMessage());
                }
            }
        }, "hold-expired-listener");
        listener.setDaemon(true);
        listener.start();
    }
    
    private void handleHoldExpired(OtpErlangObject metadataObj) {
        try {
            System.out.println("[JInterface] Received hold_expired: " + metadataObj);
            
            if (metadataObj instanceof OtpErlangMap map) {
                String eventId = extractMapValue(map, "event_id");
                String seatId = extractMapValue(map, "seat_id");
                String userId = extractMapValue(map, "user_id");
                String holdId = extractMapValue(map, "hold_id");
                Long expiredAt = extractMapLong(map, "expired_at");
                
                System.out.println("[JInterface] Hold expired - eventId=" + eventId + 
                    " seatId=" + seatId + " userId=" + userId + " holdId=" + holdId);
                
                // Publish to Kafka for worker to process
                kafkaEventPublisher.publishHoldExpired(eventId, seatId, userId, holdId, expiredAt);
            }
        } catch (Exception e) {
            System.err.println("[JInterface] Error handling hold_expired: " + e.getMessage());
        }
    }
    
    private String extractMapValue(OtpErlangMap map, String key) {
        try {
            OtpErlangObject value = map.get(new OtpErlangAtom(key));
            return asString(value);
        } catch (Exception e) {
            return null;
        }
    }
    
    private Long extractMapLong(OtpErlangMap map, String key) {
        try {
            OtpErlangObject value = map.get(new OtpErlangAtom(key));
            return asLong(value);
        } catch (Exception e) {
            return null;
        }
    }

    public WriteResult writeHold(String eventId, String seatId, String userId, Duration holdDuration) throws Exception {
        String corr = UUID.randomUUID().toString();
        String holdId = UUID.randomUUID().toString();
        long expiresAt = System.currentTimeMillis() + holdDuration.toMillis();
        String lastError = null;

        for (String remoteNode : candidateNodesForSeatId(seatId)) {
            if (!shouldTryNode(remoteNode)) {
                continue;
            }

            boolean reachable = node.ping(remoteNode, PING_TIMEOUT_MS);
            if (!reachable) {
                markDown(remoteNode);
                lastError = "node_unreachable:" + remoteNode;
                continue;
            }

            // mailbox per attempt to avoid stale replies from previous node
            OtpMbox mbox = node.createMbox();

            OtpErlangTuple msg = new OtpErlangTuple(new OtpErlangObject[]{
                    new OtpErlangAtom("hold_seat"),
                    mbox.self(),
                    new OtpErlangString(eventId),
                    new OtpErlangString(seatId),
                    new OtpErlangString(userId),
                    new OtpErlangString(holdId),
                    new OtpErlangLong(expiresAt)
            });

            System.out.println("[JIF] route seatId=" + seatId + " -> " + remoteNode + " reachable=" + reachable);
            System.out.println("[JIF] myPid=" + mbox.self() + " send=" + msg);

            try {
                mbox.send(REMOTE_REG_NAME, remoteNode, msg);
                OtpErlangObject reply = mbox.receive(holdDuration.toMillis());
                System.out.println("[JIF] rawReply=" + reply);

                if (reply == null) {
                    markDown(remoteNode);
                    lastError = "timeout_waiting_reply:" + remoteNode;
                    continue;
                }

                markUp(remoteNode);
                return WriteResult.fromErlang(corr, reply);
            } catch (Exception e) {
                markDown(remoteNode);
                lastError = "send_failed:" + remoteNode + ":" + e.getClass().getSimpleName();
            }
        }

        if (lastError == null) {
            lastError = "no_candidate_nodes";
        }
        return new WriteResult(false, corr, null, lastError, holdId, expiresAt);
    }

    public CheckSeatResult checkSeat(String eventId, String seatId) throws Exception {
        String corr = UUID.randomUUID().toString();
        String lastError = null;

        for (String remoteNode : candidateNodesForSeatId(seatId)) {
            if (!shouldTryNode(remoteNode)) {
                continue;
            }

            boolean reachable = node.ping(remoteNode, PING_TIMEOUT_MS);
            if (!reachable) {
                markDown(remoteNode);
                lastError = "node_unreachable:" + remoteNode;
                continue;
            }

            OtpMbox mbox = node.createMbox();
            OtpErlangTuple msg = new OtpErlangTuple(new OtpErlangObject[]{
                    new OtpErlangAtom("check_seat"),
                    mbox.self(),
                    new OtpErlangString(eventId),
                    new OtpErlangString(seatId)
            });

            System.out.println("[JIF] check seatId=" + seatId + " -> " + remoteNode);

            try {
                mbox.send(REMOTE_REG_NAME, remoteNode, msg);
                OtpErlangObject reply = mbox.receive(2000);
                System.out.println("[JIF] check_seat rawReply=" + reply);

                if (reply == null) {
                    markDown(remoteNode);
                    lastError = "timeout_waiting_reply:" + remoteNode;
                    continue;
                }

                markUp(remoteNode);
                return CheckSeatResult.fromErlang(corr, reply);
            } catch (Exception e) {
                markDown(remoteNode);
                lastError = "send_failed:" + remoteNode + ":" + e.getClass().getSimpleName();
            }
        }

        if (lastError == null) {
            lastError = "no_candidate_nodes";
        }
        return new CheckSeatResult(false, corr, null, lastError, null, null, null, null, null, null, null);
    }

    public ConfirmResult confirmHold(String userId, String holdId) throws Exception {
        String corr = UUID.randomUUID().toString();
        String lastError = null;
        List<String> nodes = List.of(
                routingProps.getNodeRes1(),
                routingProps.getNodeRes2(),
                routingProps.getNodeRes3()
        );

        for (String remoteNode : nodes) {
            if (!shouldTryNode(remoteNode)) {
                continue;
            }

            boolean reachable = node.ping(remoteNode, PING_TIMEOUT_MS);
            if (!reachable) {
                markDown(remoteNode);
                lastError = "node_unreachable:" + remoteNode;
                continue;
            }

            OtpMbox mbox = node.createMbox();
            OtpErlangTuple msg = new OtpErlangTuple(new OtpErlangObject[]{
                    new OtpErlangAtom("confirm_hold"),
                    mbox.self(),
                    new OtpErlangString(userId),
                    new OtpErlangString(holdId)
            });

            System.out.println("[JIF] confirm holdId=" + holdId + " -> " + remoteNode + " reachable=" + reachable);
            System.out.println("[JIF] myPid=" + mbox.self() + " send=" + msg);

            try {
                mbox.send(REMOTE_REG_NAME, remoteNode, msg);
                OtpErlangObject reply = mbox.receive(CONFIRM_TIMEOUT_MS);
                System.out.println("[JIF] rawReply=" + reply);

                if (reply == null) {
                    markDown(remoteNode);
                    lastError = "timeout_waiting_reply:" + remoteNode;
                    continue;
                }

                markUp(remoteNode);
                return ConfirmResult.fromErlang(corr, reply);
            } catch (Exception e) {
                markDown(remoteNode);
                lastError = "send_failed:" + remoteNode + ":" + e.getClass().getSimpleName();
            }
        }

        if (lastError == null) {
            lastError = "no_candidate_nodes";
        }
        return new ConfirmResult(false, corr, null, lastError, null, null, null);
    }

    private String routeNodeForSeatId(String seatId) {
        if (seatId == null || seatId.isEmpty()) {
            throw new IllegalArgumentException("seatId is required");
        }
        char prefix = Character.toUpperCase(seatId.charAt(0));
        return switch (prefix) {
            case 'A', 'B' -> routingProps.getNodeRes1();
            case 'C', 'D' -> routingProps.getNodeRes2();
            case 'E', 'F' -> routingProps.getNodeRes3();
            default -> throw new IllegalArgumentException("Unsupported seat prefix: " + prefix);
        };
    }

    private List<String> candidateNodesForSeatId(String seatId) {
        String primary = routeNodeForSeatId(seatId);
        if (primary.equals(routingProps.getNodeRes1())) {
            return List.of(routingProps.getNodeRes1(), routingProps.getNodeRes2(), routingProps.getNodeRes3());
        }
        if (primary.equals(routingProps.getNodeRes2())) {
            return List.of(routingProps.getNodeRes2(), routingProps.getNodeRes3(), routingProps.getNodeRes1());
        }
        return List.of(routingProps.getNodeRes3(), routingProps.getNodeRes1(), routingProps.getNodeRes2());
    }

    private boolean shouldTryNode(String nodeName) {
        NodeHealth health = healthByNode.get(nodeName);
        if (health == null) return true;
        if (health.up) return true;
        return (System.currentTimeMillis() - health.lastFailureMillis) >= DOWN_COOLDOWN_MS;
    }

    private void markDown(String nodeName) {
        healthByNode.compute(nodeName, (k, v) -> {
            if (v == null) v = new NodeHealth();
            v.up = false;
            v.lastFailureMillis = System.currentTimeMillis();
            return v;
        });
    }

    private void markUp(String nodeName) {
        healthByNode.compute(nodeName, (k, v) -> {
            if (v == null) v = new NodeHealth();
            v.up = true;
            return v;
        });
    }

    private static final class NodeHealth {
        boolean up = true;
        long lastFailureMillis;
    }

    public record WriteResult(boolean ok,
                              String correlationId,
                              String rawReply,
                              String error,
                              String holdId,
                              Long expiresAtMillis) {
        static WriteResult timeout(String corr) {
            return new WriteResult(false, corr, null, "timeout_waiting_reply", null, null);
        }

        static WriteResult fromErlang(String corr, OtpErlangObject obj) {
            String raw = obj.toString();
            if (obj instanceof OtpErlangTuple tup && tup.arity() >= 3) {
                OtpErlangObject payload = tup.elementAt(2);
                if (payload instanceof OtpErlangTuple pt && pt.arity() >= 1) {
                    OtpErlangObject status = pt.elementAt(0);
                    if (status instanceof OtpErlangAtom a && "ok".equals(a.atomValue())) {
                        String holdId = asString(pt.elementAt(1));
                        Long expiresAt = asLong(pt.elementAt(2));
                        return new WriteResult(true, corr, raw, null, holdId, expiresAt);
                    }
                    if (status instanceof OtpErlangAtom a && "error".equals(a.atomValue())) {
                        String reason = pt.arity() > 1 ? pt.elementAt(1).toString() : "unknown_error";

                        // Check for user_already_holding_seat error tuple
                        if (pt.arity() > 1) {
                            OtpErlangObject reasonObj = pt.elementAt(1);
                            if (reasonObj instanceof OtpErlangTuple rt && rt.arity() >= 1) {
                                OtpErlangObject errorType = rt.elementAt(0);
                                if (errorType instanceof OtpErlangAtom err && "user_already_holding_seat".equals(err.atomValue())) {
                                    reason = "user_already_holding_seat";
                                    if (rt.arity() >= 4) {
                                        String expEventId = asString(rt.elementAt(1));
                                        String expSeatId = asString(rt.elementAt(2));
                                        reason += ":" + expEventId + ":" + expSeatId;
                                    }
                                }
                            }
                        }

                        return new WriteResult(false, corr, raw, reason, null, null);
                    }
                }
            }
            return new WriteResult(false, corr, raw, "unexpected_reply_format", null, null);
        }
    }

    public record ConfirmResult(boolean ok,
                                String correlationId,
                                String rawReply,
                                String error,
                                String orderId,
                                String eventId,
                                String seatId) {
        static ConfirmResult fromErlang(String corr, OtpErlangObject obj) {
            String raw = obj == null ? null : obj.toString();
            if (obj instanceof OtpErlangTuple tup && tup.arity() >= 3) {
                String eventId = null;
                String seatId = null;
                OtpErlangObject keyObj = tup.elementAt(1);
                if (keyObj instanceof OtpErlangTuple keyTup && keyTup.arity() >= 2) {
                    eventId = normalizeUndefined(asString(keyTup.elementAt(0)));
                    seatId = normalizeUndefined(asString(keyTup.elementAt(1)));
                }

                OtpErlangObject payload = tup.elementAt(2);
                if (payload instanceof OtpErlangTuple pt && pt.arity() >= 1) {
                    OtpErlangObject status = pt.elementAt(0);
                    if (status instanceof OtpErlangAtom a && "ok".equals(a.atomValue())) {
                        String orderId = pt.arity() > 1 ? asString(pt.elementAt(1)) : null;
                        return new ConfirmResult(true, corr, raw, null, orderId, eventId, seatId);
                    }
                    if (status instanceof OtpErlangAtom a && "error".equals(a.atomValue())) {
                        String reason = pt.arity() > 1 ? asString(pt.elementAt(1)) : "unknown_error";
                        return new ConfirmResult(false, corr, raw, reason, null, eventId, seatId);
                    }
                }
            }
            return new ConfirmResult(false, corr, raw, "unexpected_reply_format", null, null, null);
        }
    }

    private static String asString(OtpErlangObject obj) {
        if (obj instanceof OtpErlangString s) return s.stringValue();
        if (obj instanceof OtpErlangAtom a) return a.atomValue();
        return obj == null ? null : obj.toString();
    }

    private static Long asLong(OtpErlangObject obj) {
        if (obj instanceof OtpErlangLong l) return l.longValue();
        return null;
    }

    private static String normalizeUndefined(String value) {
        if ("undefined".equals(value)) {
            return null;
        }
        return value;
    }

    public record CheckSeatResult(boolean ok,
                                  String correlationId,
                                  String rawReply,
                                  String error,
                                  String status,
                                  String eventId,
                                  String seatId,
                                  String userId,
                                  String holdId,
                                  Long expiresAt,
                                  String orderId) {
        static CheckSeatResult fromErlang(String corr, OtpErlangObject obj) {
            String raw = obj == null ? null : obj.toString();
            if (obj instanceof OtpErlangTuple tup && tup.arity() >= 3) {
                OtpErlangObject payload = tup.elementAt(2);
                if (payload instanceof OtpErlangTuple pt && pt.arity() >= 2) {
                    OtpErlangObject statusAtom = pt.elementAt(0);
                    if (statusAtom instanceof OtpErlangAtom a && "ok".equals(a.atomValue())) {
                        // Parse the map
                        OtpErlangObject mapObj = pt.elementAt(1);
                        if (mapObj instanceof OtpErlangMap map) {
                            String status = extractMapString(map, "status");
                            String eventId = extractMapString(map, "event_id");
                            String seatId = extractMapString(map, "seat_id");
                            String userId = extractMapString(map, "user_id");
                            String holdId = extractMapString(map, "hold_id");
                            Long expiresAt = extractMapLong(map, "expires_at");
                            String orderId = extractMapString(map, "order_id");
                            return new CheckSeatResult(true, corr, raw, null, status, eventId, seatId, userId, holdId, expiresAt, orderId);
                        }
                    }
                    if (statusAtom instanceof OtpErlangAtom a && "error".equals(a.atomValue())) {
                        String reason = pt.arity() > 1 ? asString(pt.elementAt(1)) : "unknown_error";
                        return new CheckSeatResult(false, corr, raw, reason, null, null, null, null, null, null, null);
                    }
                }
            }
            return new CheckSeatResult(false, corr, raw, "unexpected_reply_format", null, null, null, null, null, null, null);
        }

        private static String extractMapString(OtpErlangMap map, String key) {
            try {
                OtpErlangObject value = map.get(new OtpErlangAtom(key));
                return normalizeUndefined(asString(value));
            } catch (Exception e) {
                return null;
            }
        }

        private static Long extractMapLong(OtpErlangMap map, String key) {
            try {
                OtpErlangObject value = map.get(new OtpErlangAtom(key));
                return asLong(value);
            } catch (Exception e) {
                return null;
            }
        }
    }
}
