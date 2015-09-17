/**
 * 
 */
package streams.kafka;

import java.io.ByteArrayOutputStream;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import stream.AbstractProcessor;
import stream.Data;
import stream.ProcessContext;
import stream.util.JavaSerializer;

/**
 * @author chris
 *
 */
public class Publish extends AbstractProcessor {

	String zookeeper = "192.168.56.101:2181";
	String broker = "192.168.56.101:9092";
	String topic = "test";

	KafkaProducer<byte[], byte[]> producer;

	/**
	 * @see stream.AbstractProcessor#init(stream.ProcessContext)
	 */
	@Override
	public void init(ProcessContext ctx) throws Exception {
		super.init(ctx);

		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);
		props.put("metadata.broker.list", broker);
		props.put("bootstrap.servers", broker);
		props.put("group.id", "test");
		props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("session.timeout.ms", "1000");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "10000");

		producer = new KafkaProducer<byte[], byte[]>(props);
	}

	public ProducerRecord<byte[], byte[]> createMessage(Data item) {
		try {
			JavaSerializer serializer = new JavaSerializer();

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			serializer.write(item, baos);
			baos.close();

			byte[] value = baos.toByteArray();
			return new ProducerRecord<byte[], byte[]>(topic, value);

		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

	}

	/**
	 * @see stream.Processor#process(stream.Data)
	 */
	@Override
	public Data process(Data input) {
		try {
			ProducerRecord<byte[], byte[]> message = this.createMessage(input);
			if (message != null) {
				producer.send(message);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return input;
	}

	/**
	 * @return the zookeeper
	 */
	public String getZookeeper() {
		return zookeeper;
	}

	/**
	 * @param zookeeper
	 *            the zookeeper to set
	 */
	public void setZookeeper(String zookeeper) {
		this.zookeeper = zookeeper;
	}

	/**
	 * @return the broker
	 */
	public String getBroker() {
		return broker;
	}

	/**
	 * @param broker
	 *            the broker to set
	 */
	public void setBroker(String broker) {
		this.broker = broker;
	}

	/**
	 * @return the topic
	 */
	public String getTopic() {
		return topic;
	}

	/**
	 * @param topic
	 *            the topic to set
	 */
	public void setTopic(String topic) {
		this.topic = topic;
	}
}