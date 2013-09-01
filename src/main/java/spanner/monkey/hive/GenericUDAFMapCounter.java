package spanner.monkey.hive;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;

import java.util.HashMap;
import java.util.Map;

@Description(name = "map_counter",
        value = "_FUNC_(x1[, x2, x3 ...]) - Count occurrence for each input value and " +
                "return a Map<input, IntCount> .")
public class GenericUDAFMapCounter extends AbstractGenericUDAFResolver {
    static final Log LOG = LogFactory.getLog(GenericUDAFMapCounter.class.getName());

    public GenericUDAFMapCounter() {
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {


        for (int i = 0; i < parameters.length; i++) {
            if (parameters[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(i,
                        "Only primitive type arguments are accepted but "
                                + parameters[i].getTypeName() + " was passed as parameter " + (i + 1)
                                + ".");
            }
        }

        return new GenericUDAFMapCounterEvaluator();
    }

    public static class GenericUDAFMapCounterEvaluator extends GenericUDAFEvaluator {

        // input
        private PrimitiveObjectInspector inputOI;

        // output Map<Primitive, IntWritable>
        private StandardMapObjectInspector mc;

        private StandardMapObjectInspector internalMergeOI;


        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);

            // init output object inspectors

            if (!(parameters[0] instanceof StandardMapObjectInspector)) {
                //inputOI = (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
                inputOI = (PrimitiveObjectInspector) parameters[0];

                mc = ObjectInspectorFactory.getStandardMapObjectInspector(
                        (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputOI),
                        PrimitiveObjectInspectorFactory.writableIntObjectInspector);
                return mc;

            } else {
                // param[0] is partial result
                internalMergeOI = (StandardMapObjectInspector) parameters[0];
                inputOI = (PrimitiveObjectInspector) internalMergeOI.getMapKeyObjectInspector();

                mc = ObjectInspectorFactory.getStandardMapObjectInspector(
                        (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputOI),
                        PrimitiveObjectInspectorFactory.writableIntObjectInspector);

                return mc;
            }
        }

        static class MkMapAggregationBuffer implements AggregationBuffer {
            Map<Object, IntWritable> container;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((MkMapAggregationBuffer) agg).container = new HashMap<Object, IntWritable>();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MkMapAggregationBuffer ret = new MkMapAggregationBuffer();
            reset(ret);
            return ret;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {

            for (Object p : parameters) {
                if (p != null) {
                    MkMapAggregationBuffer myagg = (MkMapAggregationBuffer) agg;
                    countUp(inputOI.getPrimitiveWritableObject(p), 1, myagg);
                }
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            MkMapAggregationBuffer myagg = (MkMapAggregationBuffer) agg;

            HashMap<Object, IntWritable> partialResult = (HashMap<Object, IntWritable>) internalMergeOI.getMap(partial);
            for (Map.Entry<Object, IntWritable> i : partialResult.entrySet()) {
                countUp(i.getKey(), i.getValue().get(), myagg);
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MkMapAggregationBuffer myagg = (MkMapAggregationBuffer) agg;
            HashMap<Object, IntWritable> ret = new HashMap<Object, IntWritable>();
            ret.putAll(myagg.container);
            return ret;
        }

        private void countUp(Object v, int count, MkMapAggregationBuffer myagg) {
            Object vCopy = ObjectInspectorUtils.copyToStandardObject(v, this.mc.getMapKeyObjectInspector());

            if (myagg.container.containsKey(vCopy)) {
                IntWritable c = myagg.container.get(vCopy);
                c.set(c.get() + count);
            } else {
                myagg.container.put(vCopy, new IntWritable(count));
            }
        }
    }

}
