/**
 * 
 */
package streams.kafka.testing;

import streams.kafka.Publish;

/**
 * @author chris
 *
 */
public class PublishWithEOF extends Publish {

    /**
     * @see stream.AbstractProcessor#finish()
     */
    @Override
    public void finish() throws Exception {
        super.finish();
    }
}
