/**
 * 
 */
package streams.kafka.testing;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import stream.Data;
import stream.runtime.ProcessContextImpl;
import stream.runtime.setup.ParameterInjection;
import stream.util.Variables;
import streams.kafka.Publish;

/**
 * @author chris
 *
 */
public class GenericProducer extends Publish {

    static Logger log = LoggerFactory.getLogger(GenericProducer.class);
    byte[] payload = new byte[1024];
    int idx = 0;

    /**
     * @see streams.kafka.Publish#createMessage(stream.Data)
     */
    @Override
    public ProducerRecord<Serializable, byte[]> createMessage(Data item) {
        return new ProducerRecord<Serializable, byte[]>(this.topic, (idx++) % 4, "" + idx, payload);
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {

        int kb = 4 * 1024;
        String zk = "ls8cb01.cs.uni-dortmund.de:2181";
        String broker = "ls8cb01.cs.uni-dortmund.de:9092";

        GenericProducer p = new GenericProducer();
        p.setZookeeper(zk);
        p.setBroker(broker);

        Map<String, String> ps = new HashMap<String, String>();
        ps.put("topic", "throughput");
        ps.put("group", "test123");
        ps.put("partitionKey", "flag");
        ps.put("codec", "streams.codec.Kryo");

        ParameterInjection.inject(p, ps, new Variables());
        p.topic = "throughput";
        p.init(new ProcessContextImpl());

        p.payload = new byte[kb];
        for (int i = 0; i < p.payload.length; i++) {
            p.payload[i] = (byte) 0xFF;
        }

        Double sent = 0.0;
        Long start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            p.process(null);
            sent += p.payload.length;

            if (i % 1000 == 0) {
                long now = System.currentTimeMillis();
                Double sec = new Double(now - start) / 1000.0;
                Double mb = sent / 1024.0 / 1024.0;
                Double byteRate = mb / sec;
                log.info("{} MB/sec", byteRate);
                sent = 0.0;
                start = now;
            }
        }
    }

}
