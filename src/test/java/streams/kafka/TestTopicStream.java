/**
 * 
 */
package streams.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.BeforeClass;

import stream.Data;
import stream.runtime.setup.ParameterInjection;
import stream.util.Variables;
import test.KafkaLocal;

/**
 * @author chris
 *
 */
public class TestTopicStream {

	static KafkaLocal kafka;

	@BeforeClass
	public static void startKafka() {
		Properties kafkaProperties = new Properties();
		Properties zkProperties = new Properties();

		try {
			// load properties
			kafkaProperties.load(Class.class.getResourceAsStream("/kafkalocal.properties"));
			zkProperties.load(Class.class.getResourceAsStream("/zklocal.properties"));

			// start kafka
			kafka = new KafkaLocal(kafkaProperties, zkProperties);
			Thread.sleep(5000);
		} catch (Exception e) {
			e.printStackTrace(System.out);
			// fail("Error running local Kafka broker");
			e.printStackTrace(System.out);
		}

		// do other things
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		try {

			Map<String, String> p = new HashMap<String, String>();
			p.put("id", "topic");
			p.put("zookeeper", "s876gn01.cs.uni-dortmund.de:2181");
			p.put("broker", "s876gn01.cs.uni-dortmund.de:9092");
			p.put("topic", "bob");

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
