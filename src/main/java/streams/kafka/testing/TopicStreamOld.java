package streams.kafka.testing;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import stream.Data;
import stream.annotations.Parameter;
import stream.data.DataFactory;
import stream.io.AbstractStream;
import streams.codec.Codec;
import streams.kafka.util.CodecUtils;
import streams.kafka.util.KafkaItemDecoder;
import streams.kafka.util.ShutdownListener;
import streams.kafka.util.ShutdownListener.Hook;
import streams.kafka.util.StringObjectDecoder;

/**
 * Kafka consumer for the streams framework.
 * 
 * @author Thomas Scharrenbach, Christian Bockermann
 * 
 */
public class TopicStreamOld extends AbstractStream {

    static Logger log = LoggerFactory.getLogger(TopicStreamOld.class);

    /**
     * Streams parameter.
     */
    @Parameter(description = "The zookeeper servers to check for topic brokers, defaults to 'localhost:2181'.")
    String zookeeper = "localhost:2181";

    /**
     * Streams parameter.
     */
    String groupId = "test";

    /**
     * Streams parameter.
     */
    @Parameter(description = "The Kafka topic to be consumed as a Streams stream.", required = true)
    String topic = "test";

    @Parameter(description = "The codec use to de-serialize data items from the message bytes, default is 'streams.codec.DefaultCodec'.")
    String codec = "streams.codec.DefaultCodec";

    private ConsumerConnector _consumer;

    private Decoder<stream.Data> _decoder;

    private KafkaStream<String, stream.Data> _kafkaStream;

    private Thread fetcher;
    private LinkedBlockingQueue<Data> buffer = new LinkedBlockingQueue<Data>(10);
    protected boolean running = false;

    final static Data eofItem = DataFactory.create();

    /**
     * @throws Exception
     */
    public void init() throws Exception {

        Properties props = new Properties();
        props.put("zookeeper.connect", this.getZookeeper());
        props.put("group.id", this.getGroupId());
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        final ConsumerConfig kafkaConsumerConfig = new ConsumerConfig(props);

        // Connect to Kafka via the high-level consumer API.
        _consumer = kafka.consumer.Consumer.createJavaConsumerConnector(kafkaConsumerConfig);

        // Create a new ObjectDecoder from the specified ObjectDecoderFactory.
        Codec<Data> codecImpl = CodecUtils.create(codec);
        _decoder = new KafkaItemDecoder(codecImpl);

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

        ShutdownListener.register(new Hook() {
            @Override
            public void shutdownSignaled() {
                log.debug("Need to interrupt TopicStream...");
                shutdownSignal();
            }
        });

        fetcher = new Thread() {
            public void run() {
                running = true;
                while (running) {
                    try {
                        final ConsumerIterator<String, stream.Data> kafkaIt = _kafkaStream.iterator();
                        if (kafkaIt.hasNext()) {
                            MessageAndMetadata<String, stream.Data> nextMessage = kafkaIt.next();
                            buffer.add(nextMessage.message());
                        }
                    } catch (Exception e) {
                        if (log.isDebugEnabled()) {
                            e.printStackTrace();
                        }
                    }
                }
                log.debug("Fetcher exiting...");
            }
        };
        fetcher.setDaemon(true);
        fetcher.start();
    }

    protected void shutdownSignal() {
        log.debug("shutting down...");
        running = false;
        buffer.add(eofItem);
        log.debug("Interrupting fetcher thread...");
        fetcher.interrupt();
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
     */
    @Override
    public stream.Data readNext() throws Exception {
        Data item = buffer.take();
        if (item == eofItem) {
            log.debug("Found EOF item!");
            return null;
        }
        return item;
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
     * @return the groupId
     */
    public String getGroupId() {
        return groupId;
    }

    /**
     * @param groupId
     *            the groupId to set
     */
    public void setGroupId(String groupId) {
        this.groupId = groupId;
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

    /**
     * @return the codec
     */
    public String getCodec() {
        return codec;
    }

    /**
     * @param codec
     *            the codec to set
     */
    public void setCodec(String codec) {
        this.codec = codec;
    }

    public static void main(String[] args) throws Exception {
        TopicStreamOld stream = new TopicStreamOld();
        stream.setZookeeper("cb02.virtual:2181");
        stream.setTopic("syslog");
        stream.init();

        Data item = stream.read();
        while (item != null) {
            System.out.println(item);
            item = stream.read();
        }
    }
}