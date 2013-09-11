package spanner.monkey.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

@UDFType(deterministic = false)
public class PsuedoRank extends GenericUDF {
    /**
     * The rank within the group. Resets whenever the group changes.
     */

    Log LOG = LogFactory.getLog(PsuedoRank.class);

    private long rank = 1;
    /**
     * Key of the group that we are ranking. Use the string form
     * of the objects since deferred object and equals do not work * as expected even for equivalent values.
     */
    private String[] groupKey;

    @Override
    public ObjectInspector initialize(ObjectInspector[] oi) throws UDFArgumentException {
        return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] currentKey) throws HiveException {
        if (!sameAsPreviousKey(currentKey)) {
            rank = 1;
        }
        return new Long(rank++);
    }

    /**
     * Returns true if the current key and the previous keys are the same. * If the keys are not the same, then sets {@link #groupKey} to the
     * current key.
     */
    private boolean sameAsPreviousKey(DeferredObject[] currentKey) throws HiveException {

        if ((null == currentKey || 0 == currentKey.length) && null == groupKey) {
            return true;
        }
        String[] previousKey = groupKey;
        copy(currentKey);
        if (null == groupKey && null != previousKey) {
            return false;
        }
        if (null != groupKey && null == previousKey) {
            return false;
        }
        if (groupKey.length != previousKey.length) {
            return false;
        }
        for (int index = 0; index < previousKey.length; index++) {
            if (!groupKey[index].equals(previousKey[index])) {
                return false;
            }
        }
        return true;
    }

    /**
     * Copies the given key to {@link #groupKey} for future * comparisons.
     */

    private void copy(DeferredObject[] currentKey) throws HiveException {
        if (null == currentKey) {
            groupKey = null;
        } else {
            groupKey = new String[currentKey.length];
            for (int index = 0; index < currentKey.length; index++) {
                groupKey[index] = String.valueOf(currentKey[index].get());
            }
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        StringBuilder sb = new StringBuilder();
        sb.append("PsuedoRank (");
        for (int i = 0; i < children.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(children[i]);
        }
        sb.append(")");
        return sb.toString();
    }
}
