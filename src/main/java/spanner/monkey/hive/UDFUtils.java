package spanner.monkey.hive;

import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import java.util.ArrayList;

public class UDFUtils {


    public static ObjectInspector solveOi(ObjectInspector arg) {
        switch (arg.getCategory()) {
            case PRIMITIVE:

                // VOID, BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, STRING, TIMESTAMP, BINARY, DECIMAL, UNKNOWN
                PrimitiveObjectInspector poi = (PrimitiveObjectInspector) arg;
                return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                        poi.getPrimitiveCategory()
                );
            case LIST:
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        solveOi(((ListObjectInspector) arg).getListElementObjectInspector())
                );
            case MAP:
                return ObjectInspectorFactory.getStandardMapObjectInspector(
                        solveOi(((MapObjectInspector) arg).getMapKeyObjectInspector()),
                        solveOi(((MapObjectInspector) arg).getMapValueObjectInspector())
                );
            case STRUCT:
                StructObjectInspector soi = (StructObjectInspector) arg;
                int size = soi.getAllStructFieldRefs().size();
                ArrayList<String> fnl = new ArrayList<String>(size);
                ArrayList<ObjectInspector> foil = new ArrayList<ObjectInspector>(size);

                for (StructField sf : ((StructObjectInspector) arg).getAllStructFieldRefs()) {
                    fnl.add(sf.getFieldName());
                    foil.add(solveOi(sf.getFieldObjectInspector()));
                }

                return ObjectInspectorFactory.getStandardStructObjectInspector(fnl, foil);
            default:
                return arg;
        }
    }

}
