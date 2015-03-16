package spanner.monkey.hive;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by lan on 3/16/15.
 */

@Description(name = "query_dt_utc",
        value = "_FUNC_([dt_diff[, delimiter]]) - Returns the date when the query was submitted.\n" +
                "e.g., if the query was submitted on 2014/01/23, then\n" +
                "query_dt_utc() => '20140123'\n" +
                "query_dt_utc(-1) => '20140122'\n" +
                "query_dt_utc('-') => '2014-01-23'\n" +
                "query_dt_utc(3, '-') => '2014-01-26'")
public class GenericUDFQueryDateUTC extends GenericUDF
{
    static final Log LOG = LogFactory.getLog(GenericUDFQueryDate.class.getName());
    private transient WritableIntObjectInspector dtDiffOi;
    private transient WritableStringObjectInspector delimiterOi;
    private transient final IntWritable DefaultDtDiff = new IntWritable(0);
    private transient final Text DefaultDelimiter = new Text("");
    transient private Table<Text, IntWritable, Text> cached = HashBasedTable.create();
    private transient final Long initTimeStamp = System.currentTimeMillis();
    public transient static final long DAY_MILLISECONDS = 24l * 3600 * 1000;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException
    {
        initializeInput(arguments);
        LOG.info("new initTimeStamp" + initTimeStamp);
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    protected void initializeInput(ObjectInspector[] arguments) throws UDFArgumentException
    {
        if (arguments.length > 2) {
            throw new UDFArgumentLengthException("The function TO_UNIX_TIMESTAMP " +
                    "requires no more thant 2 argument");
        }

        int nextArgument = 0;
        if (arguments.length > 0) {
            if (arguments[nextArgument] instanceof WritableIntObjectInspector) {
                dtDiffOi = (WritableIntObjectInspector) arguments[0];
                nextArgument++;
                //dtDiff = dtDiffOi.getWritableConstantValue().get();
            }
            if (nextArgument < arguments.length && arguments[nextArgument] instanceof WritableStringObjectInspector) {
                delimiterOi = (WritableStringObjectInspector) arguments[nextArgument];
                nextArgument++;
                //delimiter = ((WritableConstantStringObjectInspector) arguments[0]).getWritableConstantValue().toString();
            }
        }
        if (nextArgument < arguments.length) {
            throw new UDFArgumentException(
                    "The function " + getName().toUpperCase() + " takes only string or int parameter");
        }
    }

    protected String getName()
    {
        return "query_dt_utc";
    }

    private String calculate(String delim, int diff)
    {
        long ts = initTimeStamp + DAY_MILLISECONDS * diff;
        StringBuilder sb = new StringBuilder();
        sb.append("yyyy");
        sb.append(delim);
        sb.append("MM");
        sb.append(delim);
        sb.append("dd");
        SimpleDateFormat sdf = new SimpleDateFormat(sb.toString());
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        return sdf.format(new Date(ts));
    }

    private Text cacheGet(Text delim)
    {
        return cacheGet(delim, DefaultDtDiff);
    }

    private Text cacheGet(IntWritable diff)
    {
        return cacheGet(DefaultDelimiter, diff);
    }

    private Text cacheGet(Text delim, IntWritable diff)
    {
        if (!cached.contains(delim, diff)) {
            String s = delim.toString();
            int i = diff.get();
            cached.put(new Text(s), new IntWritable(i),
                    new Text(calculate(s, i))
            );
        }
        LOG.info("cache get (" + delim + ", " + diff + ") = " + cached.get(delim, diff));
        return cached.get(delim, diff);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException
    {
        LOG.info(arguments);
        if (arguments.length == 0) {
            return cacheGet(DefaultDelimiter, DefaultDtDiff);
        }
        else if (arguments.length == 2) {
            return cacheGet(
                    delimiterOi.getPrimitiveWritableObject(arguments[1].get()),
                    (IntWritable) dtDiffOi.getPrimitiveWritableObject(arguments[0].get())
            );
        }
        else {
            if (dtDiffOi != null) {
                return cacheGet((IntWritable) dtDiffOi.getPrimitiveWritableObject(arguments[0].get()));
            }
            else if (delimiterOi != null) {
                return cacheGet(delimiterOi.getPrimitiveWritableObject(arguments[0].get()));
            }
        }
        return null;
    }

    @Override
    public String getDisplayString(String[] children)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("query_dt_utc(");
        if (children.length > 0) {
            sb.append(children[0]);
            for (int i = 1; i < children.length; i++) {
                sb.append(",");
                sb.append(children[i]);
            }
        }
        sb.append(")");
        return sb.toString();
    }
}
