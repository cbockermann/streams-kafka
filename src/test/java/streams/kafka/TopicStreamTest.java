/**
 * 
 */
package streams.kafka;

import static org.junit.Assert.fail;

import java.io.File;
import java.net.URL;
import java.util.Properties;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

/**
 * @author chris
 *
 */
public class TopicStreamTest {

	static Logger log = LoggerFactory.getLogger(TopicStreamTest.class);

	@Test
	public void test() {
		try {
			File tmp = new File("/tmp/junit-kafka");
			tmp.mkdirs();

			ZkTestServer.zk();

			URL url = TopicStreamTest.class.getResource("/kafka-server.properties");
			Properties p = new Properties();
			p.load(url.openStream());
			p.put("broker.id", "1");
			p.put("log.dirs", tmp.getCanonicalPath());
			p.put("zookeeper.connect", "127.0.0.1:2181");

			KafkaConfig config = new KafkaConfig(p);
			KafkaServerStartable kafka = new KafkaServerStartable(config);
			kafka.startup();

		} catch (Exception e) {
			fail("Test failed: " + e.getMessage());
		}
	}
}