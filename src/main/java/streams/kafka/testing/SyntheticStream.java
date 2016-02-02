/**
 * 
 */
package streams.kafka.testing;

import java.util.Random;

import stream.Data;
import stream.data.DataFactory;
import stream.io.AbstractStream;

/**
 * @author chris
 *
 */
public class SyntheticStream extends AbstractStream {

	long id = 0L;
	int samples = 1000;

	Random rnd = new Random(2015L);

	/**
	 * @see stream.io.AbstractStream#readNext()
	 */
	@Override
	public Data readNext() throws Exception {

		Data item = DataFactory.create();

		double[] bytes = new double[samples];
		for (int i = 0; i < bytes.length; i++) {
			bytes[i] = rnd.nextDouble();
		}

		item.put("@id", ++id);
		item.put("samples", bytes);

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