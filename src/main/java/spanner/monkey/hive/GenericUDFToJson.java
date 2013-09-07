package spanner.monkey.hive;

import com.alibaba.fastjson.JSON;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;

public class GenericUDFToJson extends GenericUDF {

    ObjectInspectorConverters.Converter converter;
    Log LOG = LogFactory.getLog(GenericUDFToJson.class.getClass());


    private ObjectInspector solveOi(ObjectInspector arg) {

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

                return JsonStructObjectInspector.getJsonStructObjectInspector(fnl, foil);
            default:
                return arg;
        }
    }

    private ObjectInspectorConverters.Converter getConverter(ObjectInspector arg) {

        switch (arg.getCategory()) {
            case PRIMITIVE:
                return ObjectInspectorConverters.getConverter(
                        arg,
                        arg
                );
            case LIST:
            case MAP:
            case STRUCT:
                return ObjectInspectorConverters.getConverter(
                        arg,
                        solveOi(arg)
                );
            default:
                return null;
        }
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        converter = getConverter(arguments[0]);
        return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        //LOG.info(converter.convert(arguments[0].get()));
        String res = JSON.toJSONString(
                converter.convert(arguments[0].get())
        );

        return res;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "to_json";
    }
}
