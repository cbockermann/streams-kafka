/**
 * 
 */
package streams.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import stream.Data;
import stream.annotations.Parameter;
import stream.data.DataFactory;
import stream.io.AbstractStream;
import stream.io.Stream;
import stream.io.multi.MultiStream;
import streams.kafka.util.DataItemDecoder;
import streams.kafka.util.StringObjectDecoder;

/**
 * @author chris
 *
 */
public class TopicStream extends AbstractStream implements MultiStream {

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

    final LinkedBlockingQueue<Data> queue = new LinkedBlockingQueue<Data>();

    StringObjectDecoder keyCodec = new StringObjectDecoder();
    DataItemDecoder valueCodec = new DataItemDecoder();

    final Map<String, Stream> partitions = new LinkedHashMap<String, Stream>();
    final ArrayList<PartitionInfo> partitionInfos = new ArrayList<PartitionInfo>();

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
        props.put("group.id", "syslog-group");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // props.put("session.timeout.ms", "1000");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "10000");
        // props.put("partition.assignment.strategy", "range");
        // props.put("client.id", "syslog-client");

        log.info("Creating kafka consumer...");
        final KafkaConsumer<byte[], byte[]> kc = new KafkaConsumer<byte[], byte[]>(props);
        consumer = kc;

        log.info("subscribing to topic '{}'", topic);
        // consumer.subscribe(topic);
        consumer.subscribe(Arrays.asList(topic));
        List<PartitionInfo> partitions = consumer.partitionsFor(topic);

        this.partitionInfos.clear();
        this.partitionInfos.addAll(partitions);

        for (PartitionInfo info : partitionInfos) {
            log.info("partition  '{}:{}' => {}", topic, info.partition(), info.leader());
        }

        Thread t = new Thread() {
            public void run() {
                try {
                    while (true) {
                        ConsumerRecords<byte[], byte[]> messages = consumer.poll(100L);
                        log.info("Polled new messages: {}", messages);
                        while (messages == null || messages.count() < 1) {
                            messages = consumer.poll(1000L);
                            log.info("Polled new messages: {}", messages);
                        }

                        Iterator<ConsumerRecord<byte[], byte[]>> it = messages.iterator();
                        while (it.hasNext()) {
                            ConsumerRecord<byte[], byte[]> record = it.next();

                            String key = keyCodec.fromBytes(record.key());
                            Data item = valueCodec.fromBytes(record.value());

                            int part = record.partition();

                            log.info("{}:{}   =>   {}, {}", topic, part, key, item);
                            enqueue(key, item);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                queue.add(endOfStream);
            }
        };

        t.setDaemon(true);
        t.start();
    }

    protected void enqueue(String key, Data item) {
        queue.add(item);
    }

    /**
     * @see stream.io.AbstractStream#readNext()
     */
    @Override
    public Data readNext() throws Exception {
        Data item = queue.take();
        if (item == endOfStream) {
            return null;
        }
        return item;
    }

    /**
     * @see stream.io.multi.MultiStream#addStream(java.lang.String,
     *      stream.io.Stream)
     */
    @Override
    public void addStream(String id, Stream stream) {
        throw new UnsupportedOperationException("Adding new substreams is not allowed for this stream implementation!");
    }

    /**
     * @see stream.io.multi.MultiStream#getStreams()
     */
    @Override
    public Map<String, Stream> getStreams() {
        return Collections.unmodifiableMap(partitions);
    }
}