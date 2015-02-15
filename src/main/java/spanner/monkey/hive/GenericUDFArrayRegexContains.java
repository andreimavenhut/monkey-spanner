package spanner.monkey.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * GenericUDFArrayContains.
 */
@Description(name = "array_regex_contains",
        value = "_FUNC_(array, regex, index, value) - Returns TRUE if the regex extracted array items contain the value.",
        extended = "Example:\n"
                + "  > SELECT _FUNC_(array('1:one', '2:two', '3:three'), '^\\d+:(.*)$', 1, 'three') FROM src LIMIT 1;\n"
                + "  true")
public class GenericUDFArrayRegexContains extends GenericUDF {
    static final Log LOG = LogFactory.getLog(GenericUDFArrayRegexContains.class.getName());

    private static final int ARRAY_IDX = 0;
    private static final int REGEX_IDX = 1;
    private static final int EXTRACT_IDX_IDX = 2;
    private static final int VALUE_IDX = 3;
    private static final int ARG_COUNT = 4; // Number of arguments to this UDF
    private static final String FUNC_NAME = "ARRAY_REGEX_CONTAINS"; // External Name

    private ObjectInspectorConverters.Converter[] converters;
    private ListObjectInspector arrayOI;

    private Text regex;
    private Pattern pattern;

    private BooleanWritable result;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {

        // Check if two arguments were passed
        if (arguments.length != ARG_COUNT) {
            throw new UDFArgumentException(
                    "The function " + FUNC_NAME + " accepts "
                            + ARG_COUNT + " arguments.");
        }

        // Check if ARRAY_IDX argument is of category LIST
        if (!arguments[ARRAY_IDX].getCategory().equals(Category.LIST)) {
            throw new UDFArgumentTypeException(ARRAY_IDX,
                    "\"" + org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME + "\" "
                            + "expected at function ARRAY_CONTAINS, but "
                            + "\"" + arguments[ARRAY_IDX].getTypeName() + "\" "
                            + "is found");
        }

        arrayOI = (ListObjectInspector) arguments[ARRAY_IDX];
        converters = new ObjectInspectorConverters.Converter[arguments.length];
        converters[ARRAY_IDX] = ObjectInspectorConverters.getConverter(arrayOI.getListElementObjectInspector(),
                PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        converters[REGEX_IDX] = ObjectInspectorConverters.getConverter(arguments[REGEX_IDX],
                PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        converters[EXTRACT_IDX_IDX] = ObjectInspectorConverters.getConverter(arguments[EXTRACT_IDX_IDX],
                PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        converters[VALUE_IDX] = ObjectInspectorConverters.getConverter(arguments[VALUE_IDX],
                PrimitiveObjectInspectorFactory.writableStringObjectInspector);

        /*
        // Check if list element and value are of same type
        if (!ObjectInspectorUtils.compareTypes(arrayElementOI, valueOI)) {
            throw new UDFArgumentTypeException(VALUE_IDX,
                    "\"" + arrayElementOI.getTypeName() + "\""
                            + " expected at function ARRAY_CONTAINS, but "
                            + "\"" + valueOI.getTypeName() + "\""
                            + " is found");
        }

        // Check if the comparison is supported for this type
        if (!ObjectInspectorUtils.compareSupported(valueOI)) {
            throw new UDFArgumentException("The function " + FUNC_NAME
                    + " does not support comparison for "
                    + "\"" + valueOI.getTypeName() + "\""
                    + " types");
        }
        */


        result = new BooleanWritable(false);

        return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    }


    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        result.set(false);

        Object array = arguments[ARRAY_IDX].get();
        Text regex = (Text) converters[REGEX_IDX].convert(arguments[REGEX_IDX].get());
        int extractIndex = ((IntWritable) converters[EXTRACT_IDX_IDX].convert(arguments[EXTRACT_IDX_IDX].get())).get();
        Text value = (Text) converters[VALUE_IDX].convert(arguments[VALUE_IDX].get());

        int arrayLength = arrayOI.getListLength(array);

        // Check if array is null or empty or value is null
        if (value == null || arrayLength <= 0) {
            return result;
        }

        if (this.pattern == null || !this.regex.equals(regex)) {
            this.regex = regex;
            this.pattern = Pattern.compile(regex.toString());
        }

        // Compare the value to each element of array until a match is found
        Text element;
        Matcher matcher;
        for (int i = 0; i < arrayLength; ++i) {
            Object listElement = arrayOI.getListElement(array, i);
            if (listElement != null) {
                element = (Text) converters[ARRAY_IDX].convert(listElement);
                LOG.warn(element.toString());
                matcher = this.pattern.matcher(element.toString());
                if (matcher.find() && value.toString().equals(matcher.group(extractIndex))) {
                    result.set(true);
                    break;
                }
            }
        }

        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == ARG_COUNT);
        return "array_contains(" + children[ARRAY_IDX] + ", "
                + children[REGEX_IDX] + ", "
                + children[EXTRACT_IDX_IDX] + ", "
                + children[VALUE_IDX] + ")";
    }
}
