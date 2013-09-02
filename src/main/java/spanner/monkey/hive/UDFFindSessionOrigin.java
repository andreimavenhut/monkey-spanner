package spanner.monkey.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class UDFFindSessionOrigin extends UDF {

    private static final int SESS_INTERVAL = 1800;

    public List<Long> evaluate(List<Long> tss) {


        Collections.sort(tss);

        List<Long> tmp = new ArrayList<Long>();
        long current = 0;
        for (long l : tss) {
            if ((l - current) > SESS_INTERVAL) {
                tmp.add(l);
            }
            current = l;
        }

        return tmp;
    }

    public List<Long> evaluate(List<Long> tss, int sessInterval) {


        Collections.sort(tss);

        List<Long> tmp = new ArrayList<Long>();
        long current = 0;
        for (long l : tss) {
            if ((l - current) > sessInterval) {
                tmp.add(l);
            }
            current = l;
        }

        return tmp;
    }

}
