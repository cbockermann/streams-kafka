/**
 * 
 */
package streams.kafka.testing;

import stream.Data;
import stream.ProcessContext;
import stream.generator.RandomStream;
import stream.io.Stream;
import stream.runtime.ProcessContextImpl;
import streams.kafka.Publish;

/**
 * @author chris
 *
 */
public class Producer {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		ProcessContext ctx = new ProcessContextImpl();

		Stream stream = new RandomStream();
		stream.setLimit(1000L);
		stream.init();

		Publish prod = new Publish();
		prod.init(ctx);

		Data item = stream.read();
		while (item != null) {
			prod.process(item);
			item = stream.read();
		}

		stream.close();
	}
}
