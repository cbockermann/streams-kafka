/**
 * 
 */
package streams.kafka;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import stream.Data;
import stream.data.DataFactory;
import stream.io.AbstractStream;

/**
 * @author chris
 *
 */
public class TopicStreamNew extends AbstractStream {

	static Logger log = LoggerFactory.getLogger(TopicStreamNew.class);

	String zookeeper = "192.168.56.101:2181";
	String broker = "192.168.56.101:9092";
	String topic = "test";

	final AtomicLong index = new AtomicLong(0L);
	KafkaConsumer<byte[], byte[]> consumer;

	/**
	 * @see stream.io.AbstractStream#init()
	 */
	@Override
	public void init() throws Exception {
		super.init();

		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);
		props.put("metadata.broker.list", broker);
		props.put("bootstrap.servers", broker);
		props.put("group.id", "test");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("session.timeout.ms", "1000");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "10000");
		props.put("partition.assignment.strategy", "range");

		log.info("Creating kafka consumer...");
		consumer = new KafkaConsumer<byte[], byte[]>(props);

		log.info("subscribing to topic '{}'", topic);
		consumer.subscribe(topic);
	}

	/**
	 * @see stream.io.AbstractStream#readNext()
	 */
	@Override
	public Data readNext() throws Exception {
		Map<String, ConsumerRecords<byte[], byte[]>> messages = consumer.poll(100L);
		log.info("Polled new messages: {}", messages);
		while (messages == null) {
			messages = consumer.poll(100L);
			log.info("Polled new messages: {}", messages);
		}

		Data item = DataFactory.create();
		item.put("@id", index.incrementAndGet());
		return item;
	}
}
