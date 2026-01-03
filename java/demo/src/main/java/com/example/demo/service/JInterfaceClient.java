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
    private static final long DOWN_COOLDOWN_MS = 10_000L;

    private OtpNode node;
    private OtpMbox notificationMbox;
    private final SeatRoutingProperties routingProps;
    private final Map<String, NodeHealth> healthByNode = new ConcurrentHashMap<>();
    private final KafkaEventPublisher kafkaPublisher;

    public JInterfaceClient(SeatRoutingProperties routingProps, KafkaEventPublisher kafkaPublisher) {
        this.routingProps = routingProps;
        this.kafkaPublisher = kafkaPublisher;
    }

    @PostConstruct
    public void init() throws IOException {
        // Create exactly ONE node for the whole Spring app lifecycle
        this.node = new OtpNode(routingProps.getLocalNode(), routingProps.getCookie());
        System.out.println("[JInterface] node up: " + node.node());
        System.out.println("[JInterface] ping " + routingProps.getNodeRes1() + " => " + node.ping(routingProps.getNodeRes1(), PING_TIMEOUT_MS));
        System.out.println("[JInterface] ping " + routingProps.getNodeRes2() + " => " + node.ping(routingProps.getNodeRes2(), PING_TIMEOUT_MS));
        System.out.println("[JInterface] ping " + routingProps.getNodeRes3() + " => " + node.ping(routingProps.getNodeRes3(), PING_TIMEOUT_MS));

        // Register notification mailbox and start listening for hold_expired events
        this.notificationMbox = node.createMbox("java_gateway_notify");
        System.out.println("[JInterface] notification mbox: " + notificationMbox.self());
        
        // Register with seat_srv on all nodes using the notification mailbox
        for (String remoteNode : List.of(routingProps.getNodeRes1(), routingProps.getNodeRes2(), routingProps.getNodeRes3())) {
            try {
                OtpErlangTuple msg = new OtpErlangTuple(new OtpErlangObject[]{
                        new OtpErlangAtom("register_gateway"),
                        notificationMbox.self()
                });
                System.out.println("[JInterface] sending register_gateway to " + remoteNode + " with mbox " + notificationMbox.self());
                notificationMbox.send(REMOTE_REG_NAME, remoteNode, msg);
                System.out.println("[JInterface] sent register_gateway to " + remoteNode);
            } catch (Exception e) {
                System.err.println("[JInterface] failed to register with " + remoteNode + ": " + e.getMessage());
                e.printStackTrace();
            }
        }

        // Start listener thread for hold_expired events
        startHoldExpiredListener();
    }

    private void startHoldExpiredListener() {
        Thread listener = new Thread(() -> {
            System.out.println("[JInterface] hold_expired listener started on mbox: " + notificationMbox.self());
            while (true) {
                try {
                    OtpErlangObject msg = notificationMbox.receive(5000);
                    if (msg != null) {
                        System.out.println("[JInterface] received msg: " + msg);
                        if (msg instanceof OtpErlangTuple tup) {
                            System.out.println("[JInterface] tuple arity: " + tup.arity());
                            if (tup.arity() == 6) {
                                OtpErlangObject cmd = tup.elementAt(0);
                                if (cmd instanceof OtpErlangAtom atom && "hold_expired".equals(atom.atomValue())) {
                                    String eventId = asString(tup.elementAt(1));
                                    String seatId = asString(tup.elementAt(2));
                                    String userId = asString(tup.elementAt(3));
                                    String holdId = asString(tup.elementAt(4));
                                    long expiresAt = asLong(tup.elementAt(5));
                                    
                                    System.out.println("[JInterface] received hold_expired for " + eventId + ":" + seatId + " user=" + userId);
                                    kafkaPublisher.publishHoldExpired(eventId, seatId, userId, holdId, expiresAt);
                                }
                            }
                        }
                    }
                } catch (OtpErlangExit e) {
                    System.err.println("[JInterface] mailbox exit: " + e.getMessage());
                } catch (Exception e) {
                    System.err.println("[JInterface] listener error: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }, "HoldExpiredListener");
        listener.setDaemon(true);
        listener.start();
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

            System.out.println("[JIF] checkSeat route seatId=" + seatId + " -> " + remoteNode);

            try {
                mbox.send(REMOTE_REG_NAME, remoteNode, msg);
                OtpErlangObject reply = mbox.receive(3000);

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
        return new CheckSeatResult(false, corr, null, lastError, null, null, null, null);
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

    private static String asString(OtpErlangObject obj) {
        if (obj instanceof OtpErlangString s) return s.stringValue();
        if (obj instanceof OtpErlangAtom a) return a.atomValue();
        return obj == null ? null : obj.toString();
    }

    private static Long asLong(OtpErlangObject obj) {
        if (obj instanceof OtpErlangLong l) return l.longValue();
        return null;
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

        private static String asString(OtpErlangObject obj) {
            if (obj instanceof OtpErlangString s) return s.stringValue();
            if (obj instanceof OtpErlangAtom a) return a.atomValue();
            return obj == null ? null : obj.toString();
        }

        private static Long asLong(OtpErlangObject obj) {
            if (obj instanceof OtpErlangLong l) return l.longValue();
            return null;
        }
    }

    public record CheckSeatResult(boolean ok,
                                  String correlationId,
                                  String rawReply,
                                  String error,
                                  String seatState,
                                  String userId,
                                  String holdId,
                                  Long expiresAtMillis) {

        static CheckSeatResult fromErlang(String corr, OtpErlangObject obj) {
            String raw = obj.toString();
            if (obj instanceof OtpErlangTuple tup && tup.arity() >= 3) {
                OtpErlangObject payload = tup.elementAt(2);
                if (payload instanceof OtpErlangTuple pt && pt.arity() >= 1) {
                    OtpErlangObject status = pt.elementAt(0);
                    if (status instanceof OtpErlangAtom a && "ok".equals(a.atomValue())) {
                        String state = pt.arity() > 1 ? asString(pt.elementAt(1)) : null;
                        String userId = pt.arity() > 2 ? asString(pt.elementAt(2)) : null;
                        String holdId = pt.arity() > 3 ? asString(pt.elementAt(3)) : null;
                        Long expiresAt = pt.arity() > 4 ? asLong(pt.elementAt(4)) : null;
                        return new CheckSeatResult(true, corr, raw, null, state, userId, holdId, expiresAt);
                    }
                    if (status instanceof OtpErlangAtom a && "error".equals(a.atomValue())) {
                        String reason = pt.arity() > 1 ? pt.elementAt(1).toString() : "unknown_error";
                        return new CheckSeatResult(false, corr, raw, reason, null, null, null, null);
                    }
                }
            }
            return new CheckSeatResult(false, corr, raw, "unexpected_reply_format", null, null, null, null);
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
    }
}
