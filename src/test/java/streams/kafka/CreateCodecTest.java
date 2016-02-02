/**
 * 
 */
package streams.kafka;

import static org.junit.Assert.fail;

import org.junit.Test;

import stream.Data;
import streams.codec.Codec;
import streams.kafka.util.CodecUtils;

/**
 * @author chris
 *
 */
public class CreateCodecTest {

	@Test
	public void testDefaultCodec() {
		try {
			Codec<Data> codec = CodecUtils.create("streams.codec.DefaultCodec");
			assert(codec != null);
		} catch (Exception e) {
			fail("Failed to create codec: " + e.getMessage());
		}
	}

	@Test
	public void testKryoCodec() {
		try {
			Codec<Data> codec = CodecUtils.create("streams.codec.Kryo");
			assert(codec != null);
		} catch (Exception e) {
			fail("Failed to create codec: " + e.getMessage());
		}
	}

}
