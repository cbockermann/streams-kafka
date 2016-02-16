/**
 * 
 */
package streams.kafka;

import java.io.File;
import java.net.URL;

/**
 * @author chris
 *
 */
public class TestSubscribe {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        URL url = new File("subscribe-test.xml").toURI().toURL();
        stream.run.main(url);
    }
}
