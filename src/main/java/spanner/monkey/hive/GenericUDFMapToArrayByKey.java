package spanner.monkey.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;

import java.util.ArrayList;
import java.util.Map;

@Description(name = "map_array_by_key", value = "_FUNC_(MAP, default value, key1, key2, ...) - ")
public class GenericUDFMapToArrayByKey extends GenericUDF {

    private final ArrayList<Object> retArray = new ArrayList<Object>();

    private PrimitiveObjectInspector matchKeyOI;
    private PrimitiveObjectInspector defaultValueOI;
    private ObjectInspectorConverters.Converter mapConverter;

    @Override
    public ObjectInspector initialize(ObjectInspector[] parameters) throws UDFArgumentException {

        if (parameters.length < 3) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "At least more than 3 arguments are expected.");
        }

        if (parameters[0].getCategory() != ObjectInspector.Category.MAP) {
            throw new UDFArgumentException("The first argument should be MAP");
        }

        if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("The second argument should be primitive");
        }
        defaultValueOI = (PrimitiveObjectInspector) (parameters[1]);

        for (int i = 2; i < parameters.length; i++) {
            if (parameters[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(i,
                        "Only primitive type arguments are accepted but "
                                + parameters[i].getTypeName() + " was passed as parameter " + (i + 1)
                                + ".");
            }
        }
        matchKeyOI = (PrimitiveObjectInspector) (parameters[2]);

        mapConverter = ObjectInspectorConverters.getConverter(
                parameters[0],
                ObjectInspectorFactory.getStandardMapObjectInspector(
                        (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(matchKeyOI),
                        (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(defaultValueOI)
                )
        );

        return ObjectInspectorFactory.getStandardListObjectInspector(
                (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(defaultValueOI)
        );

    }

    @Override
    public Object evaluate(DeferredObject[] parameters) throws HiveException {

        retArray.clear();

        Map<Object, Object> map = (Map<Object, Object>) mapConverter.convert(parameters[0].get());

        for (int i = 2; i < parameters.length; i++) {
            Object key = parameters[i].get();
            if (map.containsKey(key)) {
                retArray.add(ObjectInspectorUtils.copyToStandardObject(
                        map.get(key),
                        defaultValueOI));
            } else {
                retArray.add(ObjectInspectorUtils.copyToStandardObject(
                        parameters[1].get(),
                        defaultValueOI));
            }
        }

        return retArray.clone();
    }

    @Override
    public String getDisplayString(String[] children) {
        StringBuilder sb = new StringBuilder();
        sb.append("map_array_by_key(");
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

