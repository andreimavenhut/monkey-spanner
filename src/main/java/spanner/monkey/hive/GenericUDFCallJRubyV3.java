package spanner.monkey.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.jruby.RubyInstanceConfig;
import org.jruby.embed.AttributeName;
import org.jruby.embed.ScriptingContainer;
import org.jruby.util.KCode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Description(name = "exec_ruby", value = "_FUNC_([RET_TYPE, ]FUNC_DEF, ARG1, ARG2, ...) - " +
        "Evaluate the result by using a 'do' method defined in Ruby scriptlet ",
        extended = "Example:\n" +
                "> select exec_jruby('def exec(foo) \"hello \" + foo end', 'monkey') from sample limit 3;\n" +
                "hello mokey\n...")
@UDFType(stateful = false)
public class GenericUDFCallJRubyV3 extends GenericUDF {

    private boolean fixedMethodName = true;
    private ObjectInspector scriptletOI;

    private ObjectInspector retOI;
    private ObjectInspectorConverters.Converter[] argsConverters;
    private PrimitiveObjectInspector[] argsOI;
    public static final String EVALUATE_METHOD = "exec";

    private ScriptingContainer container;
    private boolean first = true;
    private Object receiver;
    private Object[] args;
    private Log LOG = LogFactory.getLog(GenericUDFCallJRubyV3.class.getName());

    private final Text retText = new Text();

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

    // scriptlet can be passed as String or List ([Script:String, Method_Name:String])
    private void parseScriptletObjectInspector(ObjectInspector scriptletParam) {
        if (scriptletParam.getCategory() == ObjectInspector.Category.LIST) {
            fixedMethodName = false;
            scriptletOI = (ListObjectInspector) scriptletParam;
        } else if (scriptletParam instanceof StringObjectInspector) {
            scriptletOI = (StringObjectInspector)
                    ObjectInspectorUtils.getStandardObjectInspector(scriptletParam);
        }
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] parameters) throws UDFArgumentException {

        if (parameters.length < 2) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "At least two arguments are expected.");
        }

        int argStart = 2;

        // the first param is hint of return type
        // currently it can be list, map or String
        if (parameters[0].getCategory() == ObjectInspector.Category.LIST) {

            parseScriptletObjectInspector(parameters[1]);
            retOI = ObjectInspectorFactory.getStandardListObjectInspector(
                    getCastedOI((PrimitiveObjectInspector) ((StandardListObjectInspector) parameters[0]).getListElementObjectInspector())
            );
        } else if (parameters[0].getCategory() == ObjectInspector.Category.MAP) {

            parseScriptletObjectInspector(parameters[1]);
            retOI = ObjectInspectorFactory.getStandardMapObjectInspector(
                    getCastedOI((PrimitiveObjectInspector) ((StandardMapObjectInspector) parameters[0]).getMapKeyObjectInspector()),
                    getCastedOI((PrimitiveObjectInspector) ((StandardMapObjectInspector) parameters[0]).getMapValueObjectInspector())
            );
            // else
        } else {
            parseScriptletObjectInspector(parameters[1]);
            retOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
        }

        if (scriptletOI == null) {
            throw new UDFArgumentTypeException(0,
                    "Wrong scriptlet argument: "
                            + parameters[0].getTypeName() + " (" + parameters[0].getCategory() + ") was passed as parameter 1"
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
        container.setKCode(KCode.UTF8);
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

        // only parse script at the first input
        if (first) {
            String scriptlet;
            if (fixedMethodName) {
                scriptlet = ((StringObjectInspector) scriptletOI).getPrimitiveJavaObject(parameters[argStart - 1].get());
            } else {
                scriptlet = ((Text) ((ListObjectInspector) scriptletOI).getListElement(parameters[argStart - 1].get(), 0)).toString();
            }
            receiver = container.runScriptlet(scriptlet);
            LOG.info("container.runScriptlet:\n" + scriptlet);
            first = false;
        }

        String method;
        if (fixedMethodName) {
            method = EVALUATE_METHOD;
        } else {
            method = ((Text) ((ListObjectInspector) scriptletOI).getListElement(parameters[argStart - 1].get(), 1)).toString();
        }

        args = new Object[parameters.length - argStart];
        for (int i = argStart; i < parameters.length; i++) {
            args[i - argStart] = argsConverters[i - argStart].convert(parameters[i].get());
        }

        Object ret = container.callMethod(receiver, method, args);

        if (retOI.getCategory() == ObjectInspector.Category.LIST) {
            ArrayList list = new ArrayList();
            list.addAll((List) ret);
            return list;
        } else if (retOI.getCategory() == ObjectInspector.Category.MAP) {
            HashMap map = new HashMap();
            map.putAll((Map) ret);
            return map;
        } else {
            retText.set(String.valueOf(ret));
            return retText;
        }

    }

    @Override
    public String getDisplayString(String[] children) {
        return "call_jruby";
    }
}

