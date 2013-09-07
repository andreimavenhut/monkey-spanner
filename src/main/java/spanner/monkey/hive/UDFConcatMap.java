package spanner.monkey.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;
import java.util.Map;

public class UDFConcatMap extends UDF {

    public Map<String, String> evaluate(Map<String, String> a, Map<String, String> b) {
        Map<String, String> c = new HashMap();

        c.putAll(a);
        c.putAll(b);

        return c;
    }
}
