/**
 * 
 */
package streams.kafka.testing;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import stream.Data;
import stream.runtime.setup.ParameterInjection;
import stream.util.Variables;
import streams.kafka.TopicStream;

/**
 * @author chris
 *
 */
public class Test {

    static Logger log = LoggerFactory.getLogger(Test.class);

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {

        String zookeeper = "ls8cb01.cs.uni-dortmund.de";
        ZkClient zk = new ZkClient(zookeeper, Integer.MAX_VALUE, 60000, kafka.utils.ZKStringSerializer$.MODULE$);
        String topic = "test-with-4";

        ZkUtils zkUtils = new ZkUtils(zk, new ZkConnection(zookeeper), false);

        boolean exists = AdminUtils.topicExists(zkUtils, topic);
        System.out.println("topic '" + topic + "' exists? " + exists);

        if (!exists) {
            int partitions = 4;
            int replication = 1;
            AdminUtils.createTopic(zkUtils, topic, partitions, replication, new Properties());
        }

        // Map<String, Properties> configs =
        // AdminUtils.fetchAllTopicConfigs(zkUtils);
        // Iterator<String> it = configs.keySet().iterator();
        // while (it.hasNext()) {
        // String cfg = it.next();
        // Option<Properties> opt = configs.get(cfg);
        // Properties p = opt.get();
        // for (Object k : p.keySet()) {
        // System.out.println(" '" + k + "' = '" + p.getProperty(k.toString()) +
        // "'");
        // }
        // }

        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("metadata.broker.list", "ls8cb01.cs.uni-dortmund.de");
        props.put("bootstrap.servers", "ls8cb01.cs.uni-dortmund.de:9092");
        // props.put("group.id", "syslog-group");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // props.put("session.timeout.ms", "1000");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "10000");
        // props.put("partition.assignment.strategy", "range");
        // props.put("client.id", "syslog-client");

        TopicStream ts = new TopicStream();

        Map<String, String> ps = new HashMap<String, String>();
        ps.put("broker", "ls8cb01.cs.uni-dortmund.de:9092");
        ps.put("topic", topic);
        ps.put("zookeeper", zookeeper);
        ps.put("id", "ts");
        ParameterInjection.inject(ts, ps, new Variables());

        ts.init();

        Data item = ts.read();
        while (item != null) {
            System.out.println("message: " + item);
            item = ts.read();
        }

        // KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[],
        // byte[]>(props);
        // consumer.subscribe(Arrays.asList(topic));
        //
        // while (true) {
        // ConsumerRecords<byte[], byte[]> records = consumer.poll(100L);
        // if (records != null && records.count() > 0) {
        // log.info("polled {} records", records.count());
        // } else {
        // log.info("poll returned 'null'!");
        // }
        // }
    }
}
