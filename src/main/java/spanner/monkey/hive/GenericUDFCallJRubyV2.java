package spanner.monkey.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.jruby.RubyInstanceConfig;
import org.jruby.embed.AttributeName;
import org.jruby.embed.ScriptingContainer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Description(name = "exec_ruby", value = "_FUNC_([RET_TYPE, ]FUNC_DEF, ARG1, ARG2, ...) - " +
        "Evaluate the result by using a 'do' method defined in Ruby scriptlet ",
        extended = "Example:\n" +
                "> select exec_jruby('def exec(foo) \"hello \" + foo end', 'monkey') from sample limit 3;\n" +
                "hello mokey\n...")
public class GenericUDFCallJRubyV2 extends GenericUDF {

    private StringObjectInspector scriptletOI;
    private ObjectInspector retOI;
    private ObjectInspectorConverters.Converter[] argsConverters;
    private PrimitiveObjectInspector[] argsOI;
    public static final String EVALUATE_METHOD = "do";

    private ScriptingContainer container;
    private boolean first = true;
    private Object receiver;
    private Object[] args;
    private Log LOG = LogFactory.getLog(GenericUDFCallJRubyV2.class.getName());

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
            argsConverters[i - argStart] = ObjectInspectorConverters.getConverter(
                    parameters[i],
                    UDFUtils.solveOi(parameters[i])
            );
        }

        System.setProperty("jruby.compile.invokedynamic", "true");
        this.container = new ScriptingContainer();
        container.setAttribute(AttributeName.SHARING_VARIABLES, false);
        container.setCompileMode(RubyInstanceConfig.CompileMode.JIT);

        HiveConf conf = new HiveConf();
        String loadPath = conf.get("jruby.load_path");
        container.getLoadPaths().add(loadPath);

        return retOI;
    }

    @Override
    public Object evaluate(DeferredObject[] parameters) throws HiveException {

        int argStart = (retOI instanceof PrimitiveObjectInspector) ? 1 : 2;

        if (first) {
            // only parse script at the first input
            String scriptlet = scriptletOI.getPrimitiveJavaObject(parameters[argStart - 1].get());
            receiver = container.runScriptlet(scriptlet);
            LOG.info("container.runScriptlet:\n" + scriptlet);
            first = false;
        }


        args = new Object[parameters.length - argStart];
        for (int i = argStart; i < parameters.length; i++) {
            args[i - argStart] =
                    argsConverters[i - argStart].convert(parameters[i].get());

        }

        Object ret = container.callMethod(receiver, EVALUATE_METHOD, args);

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

