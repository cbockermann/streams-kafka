/**
 * 
 */
package streams.kafka.util;

import stream.Data;
import streams.codec.Codec;

/**
 * @author chris
 *
 */
public class CodecUtils {

	public static Codec<Data> create(String name) throws Exception {
		@SuppressWarnings("unchecked")
		Class<Codec<Data>> clazz = (Class<Codec<Data>>) Class.forName(name);
		return clazz.newInstance();
	}
}