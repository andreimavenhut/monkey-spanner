package spanner.monkey.hive;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

import java.util.ArrayList;
import java.util.List;

@Description(name = "sum_row_vectors", value = "_FUNC_(Array) - Returns a new array which each element summed. ")
public class GenericUDAFSumRowVectors extends AbstractGenericUDAFResolver {
    static final Log LOG = LogFactory.getLog(GenericUDAFSumRowVectors.class.getName());

    public GenericUDAFSumRowVectors() {
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {

        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Only one arguments are expected.");
        }

        if (parameters[0].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentTypeException(0,
                    "Only list argument are accepted but "
                            + parameters[0].getTypeName() + " was passed as parameter 1.");
        }

        TypeInfo elementTypeInfo = ((ListTypeInfo) parameters[0]).getListElementTypeInfo();

        if (elementTypeInfo.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive array are accepted but array of "
                            + elementTypeInfo.getTypeName() + " was passed as parameter 1.");
        }


        switch (((PrimitiveTypeInfo) elementTypeInfo).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case TIMESTAMP:
                return new GenericUDAFSumLongArray();
            case FLOAT:
            case DOUBLE:
            case STRING:
                //return new GenericUDAFSumLongArray();
            case DECIMAL:
                //return new GenericUDAFSumBigDecimal();
            case BOOLEAN:
                throw new UDFArgumentTypeException(0, "Unsupported yet");
            default:
                throw new UDFArgumentTypeException(0,
                        "Only numeric or string type array are accepted but array of "
                                + elementTypeInfo.getTypeName() + " is passed.");
        }
    }

    public static class GenericUDAFSumLongArray extends GenericUDAFEvaluator {

        private PrimitiveObjectInspector eleOI;
        private ObjectInspectorConverters.Converter inputConverter;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);

            eleOI = PrimitiveObjectInspectorFactory.javaLongObjectInspector;

            inputConverter = ObjectInspectorConverters.getConverter(
                    parameters[0],
                    ObjectInspectorFactory.getStandardListObjectInspector(
                            PrimitiveObjectInspectorFactory.javaLongObjectInspector
                    )
            );

            return ObjectInspectorFactory.getStandardListObjectInspector(
                    PrimitiveObjectInspectorFactory.writableLongObjectInspector
            );
        }

        static class MkArrayAggregationBuffer implements AggregationBuffer {
            List<LongWritable> container;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((MkArrayAggregationBuffer) agg).container = new ArrayList<LongWritable>();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MkArrayAggregationBuffer ret = new MkArrayAggregationBuffer();
            reset(ret);
            return ret;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            assert (parameters.length == 1);
            merge(agg, parameters[0]);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {

            List<Object> p = (List<Object>) inputConverter.convert(partial);
            if (p != null) {
                MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
                sumArray(p, myagg);
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
            ArrayList<LongWritable> ret = new ArrayList<LongWritable>();
            ret.addAll(myagg.container);
            return ret;
        }

        private void sumArray(List<Object> p, MkArrayAggregationBuffer myagg) {

            int currentSize = myagg.container.size();

            for (int i = 0; i < p.size(); i++) {
                long v = PrimitiveObjectInspectorUtils.getLong(p.get(i), eleOI);
                if (i >= currentSize) {
                    myagg.container.add(new LongWritable(v));
                } else {
                    LongWritable current = myagg.container.get(i);
                    current.set(current.get() + v);
                }

            }
        }
    }

}
