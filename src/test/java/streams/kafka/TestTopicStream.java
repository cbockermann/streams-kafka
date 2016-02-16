/**
 * 
 */
package streams.kafka;

import java.util.HashMap;
import java.util.Map;

import stream.Data;
import stream.runtime.setup.ParameterInjection;
import stream.util.Variables;

/**
 * @author chris
 *
 */
public class TestTopicStream {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {

        try {

            Map<String, String> p = new HashMap<String, String>();
            p.put("id", "topic");
            p.put("zookeeper", "ls8cb01.cs.uni-dortmund.de");
            p.put("broker", "ls8cb01.cs.uni-dortmund.de:9092");
            p.put("topic", "parted-test");

            TopicStream ts = new TopicStream();
            ParameterInjection.inject(ts, p, new Variables());
            ts.init();

            Data item = ts.read();
            while (item != null) {
                System.out.println("" + item);
                item = ts.read();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
