package spanner.monkey.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.jruby.embed.ScriptingContainer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Description(name = "call_jruby", value = "_FUNC_([RET_TYPE, ]Scriptlet, arg1, arg2, ...) - " +
        "execute a ruby scriptlet. arguments can be accessed from inside the scriptlet " +
        "by using the name 'arg1, arg2, ...", extended = "Example:\n" +
        "> select call_jruby('ret = Math::PI') from sample limit 3;\n" +
        "3.141592653589793\n...")
public class GenericUDFCallJRuby extends GenericUDF {

    StringObjectInspector scriptletOI;
    private ObjectInspector retOI;
    private ObjectInspectorConverters.Converter[] argsConverters;
    private PrimitiveObjectInspector[] argsOI;

    ScriptingContainer container;

    private ObjectInspector getCastedOI(PrimitiveObjectInspector poi) {

        ObjectInspector ret;
        switch (poi.getPrimitiveCategory()) {
            case BOOLEAN:
                ret = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                        PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN);
                break;
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case TIMESTAMP:
                ret = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                        PrimitiveObjectInspector.PrimitiveCategory.LONG);
                break;
            case VOID:
            case FLOAT:
            case DOUBLE:
                ret = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                        PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);
                break;
            case STRING:
                ret = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                        PrimitiveObjectInspector.PrimitiveCategory.STRING);
                break;
            case BINARY:
                ret = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                        PrimitiveObjectInspector.PrimitiveCategory.BINARY);
                break;
            default:
                ret = poi;
                break;
        }

        return ret;
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] parameters) throws UDFArgumentException {

        if (parameters.length < 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "At least one argument is expected.");
        }

        int argStart;

        if (parameters.length >= 2 &&
                parameters[0].getCategory() == ObjectInspector.Category.LIST &&
                parameters[1] instanceof StringObjectInspector) {

            scriptletOI = (StringObjectInspector)
                    ObjectInspectorUtils.getStandardObjectInspector(parameters[1]);
            retOI = ObjectInspectorFactory.getStandardListObjectInspector(
                    getCastedOI((PrimitiveObjectInspector) ((StandardListObjectInspector) parameters[0]).getListElementObjectInspector())
            );
            argStart = 2;
        } else if (parameters.length >= 2 &&
                parameters[0].getCategory() == ObjectInspector.Category.MAP &&
                parameters[1] instanceof StringObjectInspector) {

            scriptletOI = (StringObjectInspector)
                    ObjectInspectorUtils.getStandardObjectInspector(parameters[1]);
            retOI = ObjectInspectorFactory.getStandardMapObjectInspector(
                    getCastedOI((PrimitiveObjectInspector) ((StandardMapObjectInspector) parameters[0]).getMapKeyObjectInspector()),
                    getCastedOI((PrimitiveObjectInspector) ((StandardMapObjectInspector) parameters[0]).getMapValueObjectInspector())
            );
            argStart = 2;
        } else if (parameters[0] instanceof StringObjectInspector) {

            scriptletOI = (StringObjectInspector)
                    ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);

            retOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                    PrimitiveObjectInspector.PrimitiveCategory.STRING);

            argStart = 1;
        } else {
            throw new UDFArgumentTypeException(0,
                    "Wrong type argument: "
                            + parameters[0].getTypeName() + " was passed as parameter 1"
                            + ".");
        }

        argsConverters = new ObjectInspectorConverters.Converter[parameters.length - argStart];

        for (int i = argStart; i < parameters.length; i++) {
            if (parameters[i].getCategory() == ObjectInspector.Category.LIST) {

                argsConverters[i - argStart] = ObjectInspectorConverters.getConverter(
                        parameters[i],
                        ObjectInspectorFactory.getStandardListObjectInspector(
                                PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                                        ((PrimitiveObjectInspector) ((ListObjectInspector) parameters[i]).getListElementObjectInspector()).getPrimitiveCategory()
                                )
                        )
                );
            } else if (parameters[i].getCategory() == ObjectInspector.Category.MAP) {

                argsConverters[i - argStart] = ObjectInspectorConverters.getConverter(
                        parameters[i],
                        ObjectInspectorFactory.getStandardMapObjectInspector(
                                (PrimitiveObjectInspector) ((MapObjectInspector) parameters[i]).getMapKeyObjectInspector(),
                                (PrimitiveObjectInspector) ((MapObjectInspector) parameters[i]).getMapValueObjectInspector()
                        )
                );
            } else {
                argsConverters[i - argStart] =
                        ObjectInspectorConverters.getConverter(
                                parameters[i],
                                PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                                        ((PrimitiveObjectInspector) parameters[i]).getPrimitiveCategory()
                                )
                        );
            }

        }

        this.container = new ScriptingContainer();

        return retOI;
    }

    @Override
    public Object evaluate(DeferredObject[] parameters) throws HiveException {

        String scriptlet;
        int argStart;
        if (retOI instanceof PrimitiveObjectInspector) {
            scriptlet = scriptletOI.getPrimitiveJavaObject(parameters[0].get());
            argStart = 1;
        } else {
            scriptlet = scriptletOI.getPrimitiveJavaObject(parameters[1].get());
            argStart = 2;
        }

        for (int i = argStart; i < parameters.length; i++) {
            container.put("arg" + (i - argStart + 1),
                    argsConverters[i - argStart].convert(parameters[i].get())
            );
        }

        Object ret = container.runScriptlet(scriptlet);

        if (retOI.getCategory() == ObjectInspector.Category.LIST) {
            ArrayList list = new ArrayList();
            list.addAll((List) ret);
            return list;
        } else if (retOI.getCategory() == ObjectInspector.Category.MAP) {
            HashMap map = new HashMap();
            map.putAll((Map) ret);
            return map;
        } else {
            return String.valueOf(ret);
        }

    }

    @Override
    public String getDisplayString(String[] children) {
        return "call_jruby";
    }
}

