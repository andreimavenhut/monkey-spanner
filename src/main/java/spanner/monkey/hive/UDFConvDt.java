package spanner.monkey.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class UDFConvDt extends UDF {

    public static final String YYYY_MM_DD = "%s-%s-%s";

    public UDFConvDt() {
    }

    public String evaluate(String yyyymmdd) {
        return String.format(YYYY_MM_DD, yyyymmdd.substring(0, 4), yyyymmdd.substring(4, 6), yyyymmdd.substring(6));
    }
}
