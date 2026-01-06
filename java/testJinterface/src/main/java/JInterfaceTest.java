import com.ericsson.otp.erlang.*;

public class JInterfaceTest {

    public static void main(String[] args) throws Exception {

        String cookie = "ticketcookie";
        String localNode = "java_client@gateway";
        String remoteNode = "res1@res1";
        String remoteProcess = "seat_srv";

        OtpNode node = new OtpNode(localNode, cookie);
        OtpMbox mbox = node.createMbox();

        System.out.println("Java node up: " + node.node());

        String holdId = "hold-" + System.currentTimeMillis();
        long expiresAt = System.currentTimeMillis() + 5000;

        // Message: {hold_seat, FromPid, <<"E1">>, <<"A1">>, <<"user1">>, <<"hold-x">>, 1700...}
        OtpErlangTuple msg = new OtpErlangTuple(new OtpErlangObject[]{
                new OtpErlangAtom("hold_seat"),
                mbox.self(),
                new OtpErlangString("E1"),
                new OtpErlangString("A1"),
                new OtpErlangString("user1"),
                new OtpErlangString(holdId),
                new OtpErlangLong(expiresAt)
        });

        mbox.send(remoteProcess, remoteNode, msg);
        System.out.println("Sent hold_seat id=" + holdId + " exp=" + expiresAt);

        OtpErlangObject reply = mbox.receive(3000);
        System.out.println("Reply = " + reply);
    }
}
