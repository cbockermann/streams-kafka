/**
 * 
 */
package streams.kafka;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

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
import streams.codec.Codec;
import streams.kafka.util.CodecUtils;
import streams.kafka.util.StringObjectDecoder;
import streams.runtime.Hook;
import streams.runtime.Signals;

/**
 * This class implements a single-threaded consumer for a Kafka topic. It will
 * subscribe to a given topic and consume messages from all partitions.
 * 
 * @author Christian Bockermann
 *
 */
public class SingleTopicStream extends AbstractStream {

    static Logger log = LoggerFactory.getLogger(SingleTopicStream.class);

    final Data endOfStream = DataFactory.create();

    @Parameter(required = true, description = "Zookeeper servers to connect to.")
    String zookeeper;

    @Parameter(required = true, description = "Brokers to connect to.")
    String broker;

    @Parameter(required = true, description = "The topic to which this stream subscribes.")
    String topic;

    @Parameter(required = false, description = "The group id of the consumer - if not provided, a random identified will be generated.")
    String group;

    KafkaConsumer<byte[], byte[]> consumer;
    StringObjectDecoder keyCodec = new StringObjectDecoder();

    Codec<Data> valueCodec;

    final LinkedBlockingQueue<Data> queue = new LinkedBlockingQueue<Data>();

    Thread pollThread = null;
    final AtomicBoolean running = new AtomicBoolean(true);
    boolean closed = false;

    /**
     * @see stream.io.AbstractStream#init()
     */
    @Override
    public void init() throws Exception {
        super.init();

        if (group == null) {
            group = UUID.randomUUID().toString();
        }

        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("metadata.broker.list", broker);
        props.put("bootstrap.servers", broker);
        props.put("group.id", group);
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "10000");

        log.debug("Creating kafka consumer...");
        final KafkaConsumer<byte[], byte[]> kc = new KafkaConsumer<byte[], byte[]>(props);
        consumer = kc;

        log.info("Subscribing to topic '{}'", topic);
        consumer.subscribe(Arrays.asList(topic));
        log.info("Using codec {}", valueCodec);

        List<PartitionInfo> partitionInfo = consumer.partitionsFor(topic);
        log.debug("topic '{}' has {} partitions", topic, partitionInfo.size());

        pollThread = new Thread() {
            public void run() {
                try {
                    running.set(true);
                    while (running.get()) {
                        ConsumerRecords<byte[], byte[]> messages = consumer.poll(100L);
                        log.debug("Polled new messages: {}", messages);
                        while (messages == null || messages.count() < 1) {
                            messages = consumer.poll(1000L);
                            log.debug("Polled new messages: {}", messages);
                        }

                        if (messages != null) {
                            Iterator<ConsumerRecord<byte[], byte[]>> it = messages.iterator();
                            while (it.hasNext()) {
                                ConsumerRecord<byte[], byte[]> record = it.next();

                                Object key = record.key();
                                byte[] data = record.value();

                                Data item = null;
                                if (valueCodec != null) {
                                    log.debug("de-serializing item using {}", valueCodec);
                                    item = valueCodec.decode(data);
                                } else {
                                    item = DataFactory.create();
                                    if (key != null) {
                                        item.put("@kafka:key", (Serializable) key);
                                    }
                                    item.put("@kafka:value", data);
                                }
                                item.put("@kafka:topic", record.topic());
                                item.put("@kafka:partition", record.partition());
                                queue.add(item);
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    log.info("Closing topic-stream reader...");
                    queue.add(endOfStream);
                }
            }
        };

        pollThread.setDaemon(true);
        pollThread.start();

        Signals.register(new Hook() {
            @Override
            public void signal(int flags) {
                log.info("Received shutdown-signal!");
                running.set(false);
                pollThread.interrupt();
                log.info("Adding EOF to queue...");
                queue.add(endOfStream);
            }
        });
    }

    /**
     * @see stream.io.AbstractStream#readNext()
     */
    @Override
    public synchronized Data readNext() throws Exception {
        if (pollThread == null) {
            init();
        }

        if (closed) {
            return null;
        }

        Data item = queue.take();
        if (item == endOfStream) {
            closed = true;
            return null;
        }
        return item;
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