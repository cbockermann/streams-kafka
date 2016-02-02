/**
 * 
 */
package streams.kafka;

import java.io.File;
import java.net.InetSocketAddress;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chris
 *
 */
public class ZkTestServer {

	static Logger log = LoggerFactory.getLogger(ZkTestServer.class);

	static ZooKeeper zk;
	static ZooKeeperServer zkSrv;

	static {
		zk();
	}

	public static ZooKeeper zk() {
		if (zk == null) {
			log.info("Creating new ZooKeeper...");
			try {
				// String prefix = "zk-" +
				// UUID.randomUUID().toString().substring(0, 4);
				File tmp = new File("/tmp/junit-zk");
				tmp.mkdirs();
				File snapDir = new File(tmp.getCanonicalPath() + "/snapshots2");
				if (snapDir.isFile())
					snapDir.delete();
				snapDir.mkdirs();
				snapDir.mkdirs();
				log.info("   using snapDir '{}'", snapDir.getCanonicalPath());
				File logDir = new File(tmp.getCanonicalPath() + "/logDir2");
				if (logDir.isFile())
					logDir.mkdirs();
				log.info("   using logDir '{}'", logDir);
				logDir.mkdirs();
				zkSrv = new ZooKeeperServer(snapDir, logDir, 10);

				ServerCnxnFactory cnxnFactory = ServerCnxnFactory.createFactory();
				cnxnFactory.configure(new InetSocketAddress(getFreePort()), 10);
				cnxnFactory.startup(zkSrv);
				cnxnFactory.join();
				zkSrv.startdata();
				zkSrv.startup();
				zk = new ZooKeeper("127.0.0.1:" + getFreePort(), 10000, new TestWatcher());
			} catch (Exception e) {
				e.printStackTrace();
				Assert.fail("Failed to initialize global zookeeper-instance!");
			}
		}
		return zk;
	}

	public static int getFreePort() {
		return 2181;
	}
}
