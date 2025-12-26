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

        // Message:
        // {write_seat, FromPid, <<"E1">>, <<"A1">>, <<"user1">>}
        OtpErlangTuple msg = new OtpErlangTuple(new OtpErlangObject[]{
                new OtpErlangAtom("write_seat"),
                mbox.self(),
                new OtpErlangString("E1"),
                new OtpErlangString("A1"),
                new OtpErlangString("user1")
        });

        mbox.send(remoteProcess, remoteNode, msg);
        System.out.println("Sent write_seat");

        OtpErlangObject reply = mbox.receive(3000);
        System.out.println("Reply = " + reply);
    }
}
