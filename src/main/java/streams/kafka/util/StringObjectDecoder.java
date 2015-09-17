/**
 * 
 */
package streams.kafka.util;

import kafka.serializer.Decoder;

/**
 * @author Christian Bockermann
 *
 */
public class StringObjectDecoder implements Decoder<String> {

	/**
	 * @see kafka.serializer.Decoder#fromBytes(byte[])
	 */
	@Override
	public String fromBytes(byte[] arg0) {
		if (arg0 == null)
			return null;

		return new String(arg0);
	}

}
