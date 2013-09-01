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
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.*;

@Description(name = "to_sorted_array", value = "_FUNC_(x, score) - Returns a list of sorted objects according to the scores. " +
        "CAUTION will easily OOM on large datasets")
public class GenericUDAFToSortedArray extends AbstractGenericUDAFResolver {
    static final Log LOG = LogFactory.getLog(GenericUDAFToSortedArray.class.getName());

    public GenericUDAFToSortedArray() {
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {

        if (parameters.length != 2) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Only two or three arguments are expected.");
        }

        for (int i = 0; i < parameters.length; i++) {
            if (parameters[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(i,
                        "Only primitive type arguments are accepted but "
                                + parameters[i].getTypeName() + " was passed as parameter " + (i + 1) + ".");
            }
        }

        return new GenericUDAFMkSortedListEvaluator();
    }

    public static class GenericUDAFMkSortedListEvaluator extends GenericUDAFEvaluator {

        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
        private PrimitiveObjectInspector inputOI;
        private PrimitiveObjectInspector inputScoreOI;

        // For COMPLETE and FINAL: ObjectInspectors for partial aggregations (list
        // of objs)
        private StandardListObjectInspector loi;

        private StandardMapObjectInspector internalMergeOI;


        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            // init output object inspectors
            // The output of a partial aggregation is a map
            if (m == Mode.PARTIAL1) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
                inputScoreOI = (PrimitiveObjectInspector) parameters[1];

                return ObjectInspectorFactory.getStandardMapObjectInspector(
                        (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputOI),
                        (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputScoreOI)
                );
            }
            if (m == Mode.PARTIAL2) {
                internalMergeOI = (StandardMapObjectInspector) parameters[0];

                inputOI = (PrimitiveObjectInspector) internalMergeOI.getMapKeyObjectInspector();
                inputScoreOI = (PrimitiveObjectInspector) internalMergeOI.getMapValueObjectInspector();

                return ObjectInspectorFactory.getStandardMapObjectInspector(
                        (PrimitiveObjectInspector) internalMergeOI.getMapKeyObjectInspector(),
                        (PrimitiveObjectInspector) internalMergeOI.getMapValueObjectInspector()
                );

            } else {
                // The output of a terminal aggregation is a list

                if (!(parameters[0] instanceof StandardMapObjectInspector)) {
                    // COMPLETE
                    inputOI = (PrimitiveObjectInspector) ObjectInspectorUtils
                            .getStandardObjectInspector(parameters[0]);

                    inputScoreOI = (PrimitiveObjectInspector) ObjectInspectorUtils
                            .getStandardObjectInspector(parameters[1]);

                    loi = ObjectInspectorFactory.getStandardListObjectInspector(
                            (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputOI));
                    return loi;

                } else {
                    // FINAL
                    internalMergeOI = (StandardMapObjectInspector) parameters[0];

                    inputOI = (PrimitiveObjectInspector) internalMergeOI.getMapKeyObjectInspector();
                    inputScoreOI = (PrimitiveObjectInspector) internalMergeOI.getMapValueObjectInspector();

                    loi = ObjectInspectorFactory.getStandardListObjectInspector(
                            (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputOI));
                    return loi;
                }
            }
        }

        static class MkMapAggregationBuffer implements AggregationBuffer {
            Map<Object, Object> container;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((MkMapAggregationBuffer) agg).container = new HashMap<Object, Object>();
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
            assert (parameters.length == 2);
            Object p = parameters[0];
            Object s = parameters[1];

            if (p != null && s != null) {
                MkMapAggregationBuffer myagg = (MkMapAggregationBuffer) agg;
                putIntoMap(p, s, myagg);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            MkMapAggregationBuffer myagg = (MkMapAggregationBuffer) agg;
            HashMap<Object, Object> ret = new HashMap<Object, Object>();
            ret.putAll(myagg.container);
            return ret;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            MkMapAggregationBuffer myagg = (MkMapAggregationBuffer) agg;

            HashMap<Object, Object> partialResult = (HashMap<Object, Object>) internalMergeOI.getMap(partial);
            for (Map.Entry<Object, Object> i : partialResult.entrySet()) {
                putIntoMap(i.getKey(), i.getValue(), myagg);
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MkMapAggregationBuffer myagg = (MkMapAggregationBuffer) agg;
            ArrayList<Object> entries = new ArrayList<Object>(myagg.container.entrySet());

            Collections.sort(entries, new Comparator() {
                @Override
                public int compare(Object o1, Object o2) {
                    Map.Entry e1 = (Map.Entry) o1;
                    Map.Entry e2 = (Map.Entry) o2;

                    return ObjectInspectorUtils.compare(e1.getValue(), inputScoreOI, e2.getValue(), inputScoreOI);
                }
            });

            ArrayList<Object> ret = new ArrayList<Object>(entries.size());

            for (Object entry : entries) {
                ret.add(((Map.Entry) entry).getKey());
            }
            return ret;
        }

        private void putIntoMap(Object p, Object s, MkMapAggregationBuffer myagg) {
            Object pCopy = ObjectInspectorUtils.copyToStandardObject(p, this.inputOI);
            Object sCopy = ObjectInspectorUtils.copyToStandardObject(s, this.inputScoreOI);
            myagg.container.put(pCopy, sCopy);
        }
    }

}
