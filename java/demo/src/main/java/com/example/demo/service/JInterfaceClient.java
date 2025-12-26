package com.example.demo.service;

import com.ericsson.otp.erlang.*;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;

@Component
public class JInterfaceClient {

    // must match Erlang cookie
    private static final String COOKIE = "ticketcookie";

    // Erlang node + registered process
    private static final String REMOTE_NODE = "res1@res1";
    private static final String REMOTE_REG_NAME = "seat_srv";

    // IMPORTANT: inside docker, use container hostname, NOT localhost
    // If your spring container is named "gateway" -> java_gateway@gateway
    private static final String LOCAL_NODE = "java_gateway@gateway";

    private OtpNode node;

    @PostConstruct
    public void init() throws IOException {
        // Create exactly ONE node for the whole Spring app lifecycle
        this.node = new OtpNode(LOCAL_NODE, COOKIE);
        System.out.println("[JInterface] node up: " + node.node());
        System.out.println("[JInterface] ping " + REMOTE_NODE + " => " + node.ping(REMOTE_NODE, 2000));
    }

    public WriteResult writeHold(String eventId, String seatId, String userId, Duration timeout) throws Exception {
        // mailbox per request
        OtpMbox mbox = node.createMbox();
        String corr = UUID.randomUUID().toString();

        // Erlang seat_srv currently expects: {write_seat, FromPid, EventId, SeatId}
        // so DO NOT send extra fields unless you update Erlang side.
        OtpErlangTuple msg = new OtpErlangTuple(new OtpErlangObject[]{
                new OtpErlangAtom("write_seat"),
                mbox.self(),
//                new OtpErlangBinary(eventId.getBytes()),
//                new OtpErlangBinary(seatId.getBytes()),
                new OtpErlangString(eventId),
                new OtpErlangString(seatId)
        });

        System.out.println("[JIF] myPid=" + mbox.self() + " send=" + msg);

        mbox.send(REMOTE_REG_NAME, REMOTE_NODE, msg);

        OtpErlangObject reply = mbox.receive(timeout.toMillis());
        System.out.println("[JIF] rawReply=" + reply);

        if (reply == null) return WriteResult.timeout(corr);
        return WriteResult.fromErlang(corr, reply);
    }

    public record WriteResult(boolean ok, String correlationId, String rawReply, String error) {
        static WriteResult timeout(String corr) {
            return new WriteResult(false, corr, null, "timeout_waiting_reply");
        }

        static WriteResult fromErlang(String corr, OtpErlangObject obj) {
            String raw = obj.toString();
            if (obj instanceof OtpErlangTuple tup && tup.arity() >= 3) {
                OtpErlangObject status = tup.elementAt(2);
                if (status instanceof OtpErlangAtom a && "ok".equals(a.atomValue())) {
                    return new WriteResult(true, corr, raw, null);
                }
                return new WriteResult(false, corr, raw, "write_failed:" + status);
            }
            return new WriteResult(false, corr, raw, "unexpected_reply_format");
        }
    }
}
