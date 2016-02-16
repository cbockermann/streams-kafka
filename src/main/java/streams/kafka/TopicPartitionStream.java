/**
 * 
 */
package streams.kafka;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import stream.Data;
import stream.annotations.Parameter;
import stream.data.DataFactory;
import stream.io.AbstractStream;
import streams.codec.Codec;
import streams.kafka.util.CodecUtils;
import streams.runtime.Hook;
import streams.runtime.Signals;

/**
 * @author chris
 *
 */
public class TopicPartitionStream<K, V> extends AbstractStream {

    static Logger log = LoggerFactory.getLogger(TopicPartitionStream.class);

    final Data endOfStream = DataFactory.create();
    final LinkedBlockingQueue<Data> queue;
    final AtomicBoolean polling = new AtomicBoolean(false);
    boolean closed = false;

    Thread pollThread = null;
    Properties config;
    KafkaConsumer<K, V> consumer;
    Codec<Data> codec;

    String topic;
    final List<TopicPartition> partitions = new ArrayList<TopicPartition>();

    protected TopicPartitionStream(Properties config, String topic, int... parts) {
        this(new LinkedBlockingQueue<Data>(), config, topic, parts);
    }

    private TopicPartitionStream(LinkedBlockingQueue<Data> queue, Properties config, String topic, int... parts) {
        this.queue = queue;
        this.config = config;

        this.topic = topic;

        for (int part : parts) {
            partitions.add(new TopicPartition(topic, part));
        }
    }

    /**
     * @see stream.io.AbstractStream#init()
     */
    @Override
    public void init() throws Exception {
        super.init();
        consumer = new KafkaConsumer<K, V>(config);

        log.info("");
        log.info("Initializing...");
        log.info("");

        log.debug("Subscribing to topic-partitions: {}", partitions);
        consumer.assign(partitions);
        log.info("Using codec {}", codec);

        final TopicPartitionStream<K, V> part = this;

        pollThread = new Thread() {
            public void run() {
                try {
                    polling.set(true);
                    log.info("Starting to poll...");
                    while (polling.get()) {
                        ConsumerRecords<K, V> rec = consumer.poll(1000L);
                        log.debug("{} => polled records: {}", part, rec.count());
                        if (!rec.isEmpty()) {
                            Iterator<ConsumerRecord<K, V>> it = rec.iterator();
                            while (it.hasNext()) {
                                ConsumerRecord<K, V> record = it.next();

                                int partition = record.partition();
                                String topic = record.topic();

                                enqueue(record.key(), record.value(), topic, partition);
                            }
                        }
                    }

                    log.debug("Adding end-of-stream item...");
                    queue.add(endOfStream);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        pollThread.setDaemon(true);
        pollThread.start();

        Signals.register(new Hook() {
            @Override
            public void signal(int flags) {
                log.debug("Signaling end-of-stream...");
                polling.set(false);
            }
        });
    }

    public void enqueue(K key, V value, String topic, int partition) {
        log.debug("{}  >>> polled message::   key: {}, topic: {}, partition: {}", this, key, topic, partition);

        byte[] bytes = (byte[]) value;

        try {
            Data item = null;
            if (codec != null) {
                log.debug("de-serializing item using {}", codec);
                item = codec.decode(bytes);
            } else {
                item = DataFactory.create();
                item.put("@kafka:value", bytes);
            }

            item.put("@kafka:topic", topic);
            item.put("@kafka:partition", partition);
            queue.add(item);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * @see stream.io.AbstractStream#readNext()
     */
    @Override
    public Data readNext() throws Exception {

        if (pollThread == null) {
            log.error("TopicPartitionStream not initialized!");
            init();
        }

        if (closed) {
            log.debug("stream already closed!");
            return null;
        }

        Data item = queue.take();
        if (item == endOfStream) {
            log.debug("Found end-of-stream!");
            closed = true;
            return null;
        }
        return item;
    }

    /**
     * @see stream.io.AbstractStream#close()
     */
    @Override
    public void close() throws Exception {
        super.close();
        queue.add(endOfStream);
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return super.toString() + "[" + getId() + "]";
    }

    @Parameter(required = false, description = "Codec to de-serialize Data item from message bytes, by default message bytes are stored in a new item in key '@kafka:value'.")
    public void setCodec(String codec) {
        if (codec != null) {
            try {
                this.codec = CodecUtils.create(codec);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("Failed to create codec from '" + codec + "': " + e.getMessage());
            }
        }
    }
}