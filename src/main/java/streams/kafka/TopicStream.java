/**
 * 
 */
package streams.kafka;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import stream.Data;
import stream.annotations.Parameter;
import stream.data.DataFactory;
import stream.io.AbstractStream;
import stream.io.PartitionedStream;
import stream.io.Stream;
import streams.codec.Codec;
import streams.kafka.util.CodecUtils;
import streams.kafka.util.StringObjectDecoder;

/**
 * @author chris
 *
 */
public class TopicStream extends AbstractStream implements PartitionedStream {

    static Logger log = LoggerFactory.getLogger(TopicStream.class);

    final Data endOfStream = DataFactory.create();

    @Parameter(required = true, description = "Zookeeper servers to connect to.")
    String zookeeper;

    @Parameter(required = true, description = "Brokers to connect to.")
    String broker;

    @Parameter(required = true, description = "The topic to which this stream subscribes.")
    String topic = "syslog";

    final AtomicLong index = new AtomicLong(0L);
    KafkaConsumer<byte[], byte[]> consumer;

    StringObjectDecoder keyCodec = new StringObjectDecoder();
    Codec<Data> valueCodec = null;

    final Map<String, TopicPartitionStream<byte[], byte[]>> partitions = new LinkedHashMap<String, TopicPartitionStream<byte[], byte[]>>();
    final LinkedBlockingQueue<Data> queue = new LinkedBlockingQueue<Data>();

    final AtomicBoolean running = new AtomicBoolean(true);
    boolean closed = false;

    /**
     * @see stream.io.AbstractStream#init()
     */
    @Override
    public void init() throws Exception {
        super.init();

        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("group.id", "syslog-group");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "10000");

        log.info("Creating kafka consumer...");
        final KafkaConsumer<byte[], byte[]> kc = new KafkaConsumer<byte[], byte[]>(props);
        consumer = kc;

        List<PartitionInfo> partitionInfo = consumer.partitionsFor(topic);
        log.info("topic '{}' has {} partitions", topic, partitionInfo.size());

        for (PartitionInfo info : partitionInfo) {
            log.debug("partition  '{}:{}' => {}", topic, info.partition(), info.leader());
            TopicPartitionStream<byte[], byte[]> substream = new TopicPartitionStream<byte[], byte[]>(props, topic,
                    info.partition());
            // substream.init();
            if (valueCodec != null) {
                String clazz = valueCodec.getClass().getCanonicalName();
                log.info("codec class used: {}", clazz);
                substream.setCodec(clazz);
            }
            substream.setId(getId() + ":" + info.partition());
            this.partitions.put("" + info.partition(), substream);
        }
    }

    /**
     * @see stream.io.AbstractStream#readNext()
     */
    @Override
    public Data readNext() throws Exception {
        return null;
    }

    /**
     * @see stream.io.multi.MultiStream#getStreams()
     */
    @Override
    public Map<String, Stream> partitions() {
        Map<String, Stream> map = new LinkedHashMap<String, Stream>(partitions);
        return Collections.unmodifiableMap(map);
    }

    @Parameter(required = false, description = "Codec to de-serialize Data item from message bytes, by default message bytes are stored in a new item in key '@kafka:value'.")
    public void setCodec(String codec) {
        if (codec != null) {
            try {
                this.valueCodec = CodecUtils.create(codec);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("Failed to create codec from '" + codec + "': " + e.getMessage());
            }
        }
    }
}