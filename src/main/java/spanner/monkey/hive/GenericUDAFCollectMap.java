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

import java.util.HashMap;
import java.util.Map;

@Description(name = "collect_map", value = "_FUNC_(key, value) - Returns a aggregated map")
public class GenericUDAFCollectMap extends AbstractGenericUDAFResolver {
    static final Log LOG = LogFactory.getLog(GenericUDAFCollectMap.class.getName());

    public GenericUDAFCollectMap() {
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {

        if (parameters.length != 2) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Two arguments are expected.");
        }

        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type argument is accepted but "
                            + parameters[0].getTypeName() + " was passed as parameter 1 ");
        }

        return new GenericUDAFMkMapEvaluator();
    }

    public static class GenericUDAFMkMapEvaluator extends GenericUDAFEvaluator {

        private PrimitiveObjectInspector keyOI;
        private ObjectInspector valueOI;
        private ObjectInspectorConverters.Converter valueConv;

        private StandardMapObjectInspector map;

        private StandardMapObjectInspector internalMergeOI;


        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            // init output object inspectors
            // The output of a partial aggregation is a map
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                keyOI = (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
                valueOI = ObjectInspectorUtils.getStandardObjectInspector(parameters[1]);
                valueConv = ObjectInspectorConverters.getConverter(parameters[1], valueOI);

                return ObjectInspectorFactory.getStandardMapObjectInspector(
                        (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(keyOI),
                        valueOI
                );
            } else {
                // PARTIAL 2 or FINAL
                internalMergeOI = (StandardMapObjectInspector) parameters[0];

                keyOI = (PrimitiveObjectInspector) internalMergeOI.getMapKeyObjectInspector();
                valueOI = internalMergeOI.getMapValueObjectInspector();

                return ObjectInspectorFactory.getStandardMapObjectInspector(
                        ObjectInspectorUtils.getStandardObjectInspector(keyOI),
                        ObjectInspectorUtils.getStandardObjectInspector(valueOI)
                );
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
            Object key = parameters[0];
            Object value = parameters[1];

            if (key != null && value != null) {
                MkMapAggregationBuffer myagg = (MkMapAggregationBuffer) agg;
                putIntoMap(key, valueConv.convert(value), myagg);
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
            HashMap<Object, Object> partialResult = (HashMap<Object, Object>) internalMergeOI.getMap(partial);
            for (Map.Entry<Object, Object> e : partialResult.entrySet()) {
                putIntoMap(e.getKey(), e.getValue(), myagg);
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MkMapAggregationBuffer myagg = (MkMapAggregationBuffer) agg;
            HashMap<Object, Object> ret = new HashMap<Object, Object>();
            ret.putAll(myagg.container);
            return ret;
        }

        private void putIntoMap(Object key, Object value, MkMapAggregationBuffer myagg) {
            Object kCopy = ObjectInspectorUtils.copyToStandardObject(key, this.keyOI);
            Object vCopy = ObjectInspectorUtils.copyToStandardObject(value, this.valueOI);

            myagg.container.put(kCopy, vCopy);
        }
    }

}
