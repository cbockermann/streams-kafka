/**
 * 
 */
package streams.kafka;

import java.io.Serializable;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import stream.AbstractProcessor;
import stream.Data;
import stream.ProcessContext;
import stream.annotations.Parameter;
import streams.codec.Codec;
import streams.kafka.util.CodecUtils;

/**
 * @author chris
 *
 */
public class Publish extends AbstractProcessor {

    static Logger log = LoggerFactory.getLogger(Publish.class);

    @Parameter(description = "This parameter specifies the key, which is used to determine the partition to which an item is publisehd, default is random partitioning.", required = false)
    String partitionKey = null;

    @Parameter(description = "The zookeeper server(s), which should be used, separated by commas.", required = true)
    String zookeeper = null;

    @Parameter(description = "The list of brokers, to which to connect, separated by commas.", required = false)
    String broker = null;

    @Parameter(description = "The topic to which messages should be published.", required = true)
    String topic = null;

    @Parameter(description = "The group id of the publisher, default is 'test'.")
    String groupId = "test";

    @Parameter(description = "The class used for encoding data items to byte messages, default is 'stream.io.JavaCodec'.")
    String codec = "streams.codec.DefaultCodec";

    Codec<Data> encoder;

    KafkaProducer<Serializable, byte[]> producer;

    /**
     * @see stream.AbstractProcessor#init(stream.ProcessContext)
     */
    @Override
    public void init(ProcessContext ctx) throws Exception {
        super.init(ctx);

        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        if (broker != null) {
            props.put("metadata.broker.list", broker);
            props.put("bootstrap.servers", broker);
        }
        props.put("group.id", groupId);
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("session.timeout.ms", "1000");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "10000");

        // ZkClient zk = new ZkClient(zookeeper);
        //
        // if (!AdminUtils.topicExists(zk, topic)) {
        // log.error("Topic '{}' does not exists, if we believe Mr. Zookeeper at
        // {}.", topic, zookeeper);
        // throw new Exception("Topic '" + topic + "' does not exist!");
        // }
        //
        producer = new KafkaProducer<Serializable, byte[]>(props);

        log.info("Creating codec from '{}'", this.codec);
        encoder = CodecUtils.create(codec);
        // List<PartitionInfo> parts = producer.partitionsFor(topic);
    }

    public ProducerRecord<Serializable, byte[]> createMessage(Data item) {
        try {
            byte[] value = encoder.encode(item);

            Serializable key = null;

            if (partitionKey != null) {
                key = item.get(partitionKey);
                if (key == null) {
                    key = "null";
                }
                return new ProducerRecord<Serializable, byte[]>(topic, key, value);

            } else {
                return new ProducerRecord<Serializable, byte[]>(topic, value);
            }

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
            ProducerRecord<Serializable, byte[]> message = this.createMessage(input);
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
     * @return the partitionKey
     */
    public String getPartitionKey() {
        return partitionKey;
    }

    /**
     * @param partitionKey
     *            the partitionKey to set
     */
    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
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
}