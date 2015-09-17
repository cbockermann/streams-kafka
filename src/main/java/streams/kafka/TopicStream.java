package streams.kafka;

/*
 * #%L
 * Streams Kafka
 * %%
 * Copyright (C) 2013 Thomas Scharrenbach
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import stream.io.AbstractStream;
import streams.kafka.util.DataItemDecoder;
import streams.kafka.util.StringObjectDecoder;

/**
 * Kafka consumer for the streams framework.
 * 
 * @author Thomas Scharrenbach
 * @version 0.0.1
 * @since 0.0.1
 * 
 */
public class TopicStream extends AbstractStream {

	static Logger log = LoggerFactory.getLogger(TopicStream.class);

	/**
	 * Streams parameter.
	 */
	private String _id;

	/**
	 * Streams parameter.
	 */
	private Long _limit;

	/**
	 * Created in init.
	 */
	private ConsumerConnector _consumer;

	/**
	 * Created in init.
	 */
	private Decoder<stream.Data> _decoder;

	/**
	 * Streams parameter.
	 */
	private String _zooKeeper = "192.168.56.101:2181";

	/**
	 * Streams parameter.
	 */
	private String _groupId = "test";

	/**
	 * Streams parameter.
	 */
	private String _topic = "test";

	/**
	 * Streams parameter.
	 */
	private KafkaStream<String, stream.Data> _kafkaStream;

	/**
	 * Streams parameter.
	 */
	private String _decoderFactory;

	/**
	 * 
	 * @version 0.0.1
	 * @since 0.0.1
	 * 
	 * @throws Exception
	 */
	public void init() throws Exception {

		final ConsumerConfig kafkaConsumerConfig = createConsumerConfig(getZooKeeper(), getGroupId());
		// Connect to Kafka via the high-level consumer API.
		_consumer = kafka.consumer.Consumer.createJavaConsumerConnector(kafkaConsumerConfig);

		// Create a new ObjectDecoder from the specified ObjectDecoderFactory.
		_decoder = new DataItemDecoder();

		// Declare how many threads we are going to use for the single topic
		// which is exposed to the Streams framework.
		// TODO Currently this is only possible single-threaded.
		final Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(getTopic(), 1);

		// Currently, we allow for String keys only.
		final Decoder<String> keyDecoder = new StringObjectDecoder();

		log.info("Subscribing to topic {}", getTopic());
		// Extract the single Kafka stream.
		final List<KafkaStream<String, stream.Data>> kafkaStreamsList = _consumer
				.createMessageStreams(topicCountMap, keyDecoder, _decoder).get(getTopic());
		_kafkaStream = kafkaStreamsList.get(0);

	}

	/**
	 * <p>
	 * Read data from the Kafka consumer and convert it to a {@link stream.Data}
	 * item.
	 * </p>
	 * <p>
	 * This implementation is non-blocking, i.e., it returns null, if the Kafka
	 * queue has no items.
	 * </p>
	 * 
	 * @version 0.0.1
	 * @since 0.0.1
	 */
	@Override
	public stream.Data readNext() throws Exception {
		final ConsumerIterator<String, stream.Data> kafkaIt = _kafkaStream.iterator();
		if (kafkaIt.hasNext()) {
			MessageAndMetadata<String, stream.Data> nextMessage = kafkaIt.next();
			return nextMessage.message();
		} else {
			return null;
		}
	}

	/**
	 * 
	 * @version 0.0.1
	 * @since 0.0.1
	 */
	@Override
	@stream.annotations.Parameter
	public void setId(String id) {
		_id = id;
	}

	/**
	 * 
	 * @version 0.0.1
	 * @since 0.0.1
	 */
	@Override
	public String getId() {
		return _id;
	}

	/**
	 * 
	 * @version 0.0.1
	 * @since 0.0.1
	 */
	@Override
	@stream.annotations.Parameter
	public void setLimit(Long limit) {
		_limit = limit;
	}

	/**
	 * 
	 * @version 0.0.1
	 * @since 0.0.1
	 */
	@Override
	public Long getLimit() {
		return _limit;
	}

	/**
	 * 
	 * @version 0.0.1
	 * @since 0.0.1
	 */
	@stream.annotations.Parameter
	public void setDecoder(String decoder) {
		_decoderFactory = decoder;
	}

	/**
	 * 
	 * @version 0.0.1
	 * @since 0.0.1
	 */
	public String getDecoder() {
		return _decoderFactory;
	}

	/**
	 * 
	 * @version 0.0.1
	 * @since 0.0.1
	 */
	@stream.annotations.Parameter(description = "The Kafka topic to be consumed as a Streams stream.", required = true)
	public void setTopic(String topic) {
		_topic = topic;
	}

	/**
	 * 
	 * @version 0.0.1
	 * @since 0.0.1
	 */
	public String getTopic() {
		return _topic;
	}

	/**
	 * 
	 * @version 0.0.1
	 * @since 0.0.1
	 */
	@stream.annotations.Parameter
	public void setZooKeeper(String zooKeeper) {
		_zooKeeper = zooKeeper;
	}

	/**
	 * 
	 * @version 0.0.1
	 * @since 0.0.1
	 */
	public String getZooKeeper() {
		return _zooKeeper;
	}

	/**
	 * 
	 * @version 0.0.1
	 * @since 0.0.1
	 */
	@stream.annotations.Parameter
	public void setGroupId(String groupId) {
		_groupId = groupId;
	}

	/**
	 * 
	 * @version 0.0.1
	 * @since 0.0.1
	 */
	public String getGroupId() {
		return _groupId;
	}

	//
	// Static methods
	//

	/**
	 * Create {@link ConsumerConfig}.
	 * 
	 * @param zookeeperChroot
	 * @param groupId
	 * @return
	 */
	private static ConsumerConfig createConsumerConfig(String zookeeperChroot, String groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeperChroot);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		return new ConsumerConfig(props);
	}
}
