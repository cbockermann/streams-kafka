/**
 * 
 */
package streams.kafka.testing;

import java.util.Random;

import stream.Data;
import stream.annotations.Parameter;
import stream.data.DataFactory;
import stream.io.AbstractStream;

/**
 * @author chris
 *
 */
public class SyntheticStream extends AbstractStream {

    long id = 0L;
    int samples = 1000;

    @Parameter
    String[] flags = "1,2,3,4,5,6".split(",");

    Random rnd = new Random(2015L);

    double[] payload;

    /**
     * @see stream.io.AbstractStream#init()
     */
    @Override
    public void init() throws Exception {
        super.init();

        payload = new double[samples];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = rnd.nextDouble();
        }
    }

    /**
     * @see stream.io.AbstractStream#readNext()
     */
    @Override
    public Data readNext() throws Exception {

        Data item = DataFactory.create();

        double[] bytes = new double[samples];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = payload[i];
        }

        int index = (int) id;
        item.put("@id", id);
        item.put("flag", flags[index % flags.length]);
        item.put("samples", bytes);
        id++;
        return item;
    }

    /**
     * @return the samples
     */
    public int getSamples() {
        return samples;
    }

    /**
     * @param samples
     *            the samples to set
     */
    public void setSamples(int samples) {
        this.samples = samples;
    }
}