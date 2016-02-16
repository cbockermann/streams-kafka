/**
 * 
 */
package streams.kafka.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chris
 *
 */
public class Partitioning {

    static Logger log = LoggerFactory.getLogger(Partitioning.class);

    public List<List<TopicPartition>> split(int worker, String topic, int... parts) {
        log.info("need to distribute {} parts to {} workers", parts.length, worker);

        final List<Integer> workload = new ArrayList<Integer>();
        for (int part : parts) {
            workload.add(part);
        }

        List<List<TopicPartition>> sched = new ArrayList<List<TopicPartition>>();

        int share = Math.max(1, parts.length / worker);
        if (parts.length < worker) {
            log.info("Not enough worker!");
            share = 1;
        }

        log.info("share for each worker is: {}", share);

        for (int i = 0; i < worker; i++) {

            List<TopicPartition> wp = new ArrayList<TopicPartition>();

            // adding 'share' partitions to worker i
            while (wp.size() < share && !workload.isEmpty()) {
                int part = workload.remove(0);
                TopicPartition tp = new TopicPartition(topic, part);
                wp.add(tp);
            }

            if (i + 1 >= worker && !workload.isEmpty()) {
                for (int part : workload) {
                    wp.add(new TopicPartition(topic, part));
                }
            }

            sched.add(wp);
        }

        return sched;
    }

    public static void main(String[] args) {

        Partitioning p = new Partitioning();
        List<List<TopicPartition>> work = p.split(4, "test", 0, 1, 2);
        int w = 0;
        for (List<TopicPartition> load : work) {
            log.info("Worker {} has partitions: ", w);
            for (TopicPartition tp : load) {
                log.info("    {}:{}", tp.topic(), tp.partition());
            }
            w++;
        }

    }
}
