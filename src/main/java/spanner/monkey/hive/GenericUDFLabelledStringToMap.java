package spanner.monkey.hive;

/**
 * Created with IntelliJ IDEA.
 * User: yuyang.lan
 * Date: 2013/06/04
 * Time: 20:49
 * To change this template use File | Settings | File Templates.
 */

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.util.HashMap;

/**
 * GenericUDFStringToMap.
 */
@Description(name = "lstr_to_map", value = "_FUNC_(text, delimiter) - "
        + "Creates a map by parsing text ", extended = "Split text into key-value pairs"
        + " using two delimiters. The first delimiter seperates pairs, and the"
        + " second delimiter sperates key and value. If only one parameter is given, default"
        + " delimiters are used: ',' as delimiter1 and '=' as delimiter2.")
public class GenericUDFLabelledStringToMap extends GenericUDF {
    HashMap<Object, Object> ret = new HashMap<Object, Object>();
    StringObjectInspector soi_text, soi_de1 = null;
    final static String default_de1 = "\t";

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        if (!TypeInfoUtils.getTypeInfoFromObjectInspector(arguments[0]).equals(
                TypeInfoFactory.stringTypeInfo)
                || (arguments.length > 1 &&
                !TypeInfoUtils.getTypeInfoFromObjectInspector(arguments[1]).equals(
                        TypeInfoFactory.stringTypeInfo))) {
            throw new UDFArgumentException("All argument should be string");
        }

        soi_text = (StringObjectInspector) arguments[0];
        if (arguments.length > 1) {
            soi_de1 = (StringObjectInspector) arguments[1];
        }

        return ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        ret.clear();
        String text = soi_text.getPrimitiveJavaObject(arguments[0].get());
        String delimiter1 = (soi_de1 == null) ?
                default_de1 : soi_de1.getPrimitiveJavaObject(arguments[1].get());

        String[] keyValuePairs = text.split(delimiter1);

        for (int i = 0; i < keyValuePairs.length; i += 2) {
            if (i + 1 >= keyValuePairs.length) {
                ret.put(keyValuePairs[i], null);
            } else {
                ret.put(keyValuePairs[i], keyValuePairs[i + 1]);
            }
        }

        return ret;
    }

    @Override
    public String getDisplayString(String[] children) {
        StringBuilder sb = new StringBuilder();
        sb.append("str_to_map(");
        assert (children.length <= 3);
        boolean firstChild = true;
        for (String child : children) {
            if (firstChild) {
                firstChild = false;
            } else {
                sb.append(",");
            }
            sb.append(child);
        }
        sb.append(")");
        return sb.toString();
    }
}

