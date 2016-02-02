/**
 * 
 */
package streams.kafka.util;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chris
 *
 */
public class ShutdownListener extends Thread {

    static Logger log = LoggerFactory.getLogger(ShutdownListener.class);
    final static ArrayList<Hook> hooks = new ArrayList<Hook>();

    static ShutdownListener listener = null;

    static {
        init();
    }

    private synchronized static void init() {
        if (listener == null) {
            log.info("Registering ShutdownListener...");
            listener = new ShutdownListener();
            Runtime.getRuntime().addShutdownHook(listener);
        }
    }

    public void run() {
        for (Hook hook : hooks) {
            hook.shutdownSignaled();
        }
    }

    public static synchronized void register(Hook hook) {
        if (!hooks.contains(hook)) {
            hooks.add(hook);
        }
    }

    public static class Hook {
        public void shutdownSignaled() {
        }
    }
}