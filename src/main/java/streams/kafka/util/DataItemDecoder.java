/**
 * 
 */
package streams.kafka.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import kafka.serializer.Decoder;
import stream.Data;

/**
 * @author chris
 *
 */
public class DataItemDecoder implements Decoder<Data> {

	/**
	 * @see kafka.serializer.Decoder#fromBytes(byte[])
	 */
	@Override
	public Data fromBytes(byte[] arg0) {
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(arg0);
			ObjectInputStream ois = new ObjectInputStream(bais);
			Data item = (Data) ois.readObject();
			ois.close();
			return item;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public byte[] toBytes(Data item) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(item);
			oos.close();
			return baos.toByteArray();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}
