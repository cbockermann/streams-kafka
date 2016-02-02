/**
 * 
 */
package streams.kafka;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chris
 *
 */
public class TestWatcher implements Watcher {

	static Logger log = LoggerFactory.getLogger(TestWatcher.class);

	/**
	 * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
	 */
	@Override
	public void process(WatchedEvent event) {
		log.info("processing event '{}'", event);
	}
}