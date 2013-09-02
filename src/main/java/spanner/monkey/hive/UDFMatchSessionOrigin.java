package spanner.monkey.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.List;

public class UDFMatchSessionOrigin extends UDF {

    public long evaluate(long ts, List<Long> session_origins) {
        // session_origins should be already sorted

        long result = 0;
        for (long ori : session_origins) {
            if (ts >= ori) {
                result = ori;
            } else {
                break;
            }
        }

        return result;
    }
}
