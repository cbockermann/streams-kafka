/**
 * 
 */
package streams.kafka.testing;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;

/**
 * @author chris
 *
 */
public class DataGenerator {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		FileOutputStream fos = new FileOutputStream(new File("/tmp/generated.dat"));
		DataOutputStream dos = new DataOutputStream(fos);

		int items = 1000000;
		int samples = 300;

		for (int i = 0; i < items; i++) {

			for (int s = 0; s < samples; s++) {
				double d = Math.random();
				dos.writeDouble(d);
				// if (s % 30 == 0) {
				// System.out.print(".");
				// }
			}
			// System.out.println();
		}

		dos.close();
	}

}
