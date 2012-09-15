import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 *
 * Get Fully Qualified Domain Name of this host
 *
 */

public class GetHostname {

    public static void main( String[] args ) throws UnknownHostException
    {
        try {
            InetAddress addr = InetAddress.getLocalHost();
            System.out.println( addr.getCanonicalHostName() );
        } catch (UnknownHostException e) {
            System.err.println("Caught UnknownHostException: " + e.getMessage());
        }
    }

}
