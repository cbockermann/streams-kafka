/**
 * 
 */
package streams.kafka.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.serializer.Decoder;
import stream.Data;
import stream.data.DataFactory;
import streams.codec.Codec;

/**
 * @author chris
 *
 */
public class KafkaItemDecoder implements Decoder<Data> {

    static Logger log = LoggerFactory.getLogger(KafkaItemDecoder.class);
    final Codec<Data> codec;

    public KafkaItemDecoder(Codec<Data> codec) {
        this.codec = codec;
    }

    /**
     * @see kafka.serializer.Decoder#fromBytes(byte[])
     */
    @Override
    public Data fromBytes(byte[] arg0) {
        try {
            return codec.decode(arg0);
        } catch (Exception e) {
            log.error("Failed to decode item: {}", e.getMessage());
            e.printStackTrace();
            Data item = DataFactory.create();
            item.put("bytes", arg0);
            item.put("decoding.error", e.getMessage());
            return item;
        }
    }
}
