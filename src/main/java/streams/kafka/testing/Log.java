/**
 * 
 */
package streams.kafka.testing;

import java.util.Iterator;

import stream.AbstractProcessor;
import stream.Data;
import stream.Keys;
import stream.ProcessContext;
import stream.annotations.Parameter;

/**
 * @author chris
 *
 */
public class Log extends AbstractProcessor {

    @Parameter
    Keys keys = new Keys("*");

    String id;

    /**
     * @see stream.AbstractProcessor#init(stream.ProcessContext)
     */
    @Override
    public void init(ProcessContext ctx) throws Exception {
        super.init(ctx);
        id = ctx.getId();
    }

    /**
     * @see stream.Processor#process(stream.Data)
     */
    @Override
    public Data process(Data input) {
        StringBuffer s = new StringBuffer("{");

        Iterator<String> it = keys.select(input).iterator();
        while (it.hasNext()) {
            String key = it.next();
            s.append(key);
            s.append("=");
            s.append(input.get(key));
            if (it.hasNext()) {
                s.append(", ");
            }
        }
        s.append("}");

        System.out.println("id[" + id + "]  =>  " + s.toString());
        return input;
    }

}
