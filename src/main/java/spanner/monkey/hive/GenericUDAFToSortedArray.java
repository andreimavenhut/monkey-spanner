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
        private ListObjectInspector inputScoreListOI;

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
                        ObjectInspectorFactory.getStandardListObjectInspector(
                                (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputScoreOI)
                        )
                );
            }
            if (m == Mode.PARTIAL2) {
                internalMergeOI = (StandardMapObjectInspector) parameters[0];

                inputOI = (PrimitiveObjectInspector) internalMergeOI.getMapKeyObjectInspector();
                inputScoreListOI = (ListObjectInspector) internalMergeOI.getMapValueObjectInspector();
                inputScoreOI = (PrimitiveObjectInspector) inputScoreListOI.getListElementObjectInspector();

                return ObjectInspectorFactory.getStandardMapObjectInspector(
                        (PrimitiveObjectInspector) internalMergeOI.getMapKeyObjectInspector(),
                        ObjectInspectorFactory.getStandardListObjectInspector(
                                (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputScoreListOI.getListElementObjectInspector())
                        )
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
                    inputScoreListOI = (ListObjectInspector) internalMergeOI.getMapValueObjectInspector();
                    inputScoreOI = (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputScoreListOI.getListElementObjectInspector());

                    loi = ObjectInspectorFactory.getStandardListObjectInspector(
                            (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputOI));
                    return loi;
                }
            }
        }

        static class MkMapAggregationBuffer implements AggregationBuffer {
            Map<Object, List<Object>> container;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((MkMapAggregationBuffer) agg).container = new HashMap<Object, List<Object>>();
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
            HashMap<Object, List<Object>> ret = new HashMap<Object, List<Object>>();
            ret.putAll(myagg.container);
            return ret;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            MkMapAggregationBuffer myagg = (MkMapAggregationBuffer) agg;

            HashMap<Object, Object> partialResult = (HashMap<Object, Object>) internalMergeOI.getMap(partial);
            for (Map.Entry<Object, Object> i : partialResult.entrySet()) {

                List<Object> os = (List<Object>) inputScoreListOI.getList(i.getValue());
                for (Object o : os) {
                    putIntoMap(i.getKey(), o, myagg);
                }
            }
        }

        public class Tuple<X, Y> {
            public final X x;
            public final Y y;

            public Tuple(X x, Y y) {
                this.x = x;
                this.y = y;
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MkMapAggregationBuffer myagg = (MkMapAggregationBuffer) agg;

            ArrayList<Tuple<Object, Object>> entries = new ArrayList<Tuple<Object, Object>>();
            for (Map.Entry<Object, List<Object>> i : myagg.container.entrySet()) {
                for (Object o : i.getValue()) {
                    entries.add(new Tuple<Object, Object>(i.getKey(), o));
                }
            }

            Collections.sort(entries, new Comparator() {
                @Override
                public int compare(Object o1, Object o2) {
                    Tuple<Object, Object> e1 = (Tuple<Object, Object>) o1;
                    Tuple<Object, Object> e2 = (Tuple<Object, Object>) o2;

                    return ObjectInspectorUtils.compare(e1.y, inputScoreOI, e2.y, inputScoreOI);
                }
            });

            ArrayList<Object> ret = new ArrayList<Object>(entries.size());

            for (Tuple<Object, Object> entry : entries) {
                ret.add(entry.x);
            }
            return ret;
        }

        private void putIntoMap(Object p, Object s, MkMapAggregationBuffer myagg) {
            Object pCopy = ObjectInspectorUtils.copyToStandardObject(p, this.inputOI);
            Object sCopy = ObjectInspectorUtils.copyToStandardObject(s, this.inputScoreOI);

            if (myagg.container.containsKey(pCopy)) {
                myagg.container.get(pCopy).add(sCopy);
            } else {
                ArrayList sList = new ArrayList();
                sList.add(sCopy);
                myagg.container.put(pCopy, sList);
            }
        }
    }

}
