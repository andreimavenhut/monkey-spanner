package spanner.monkey.hive;

/**
 * Created by lan on 2/15/15.
 */

import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import spanner.monkey.hive.dynamodb.UpdateHelper;
import spanner.monkey.hive.scripting.RubyScriptingUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * should always run in reducer side !!
 * ddb_update(hash_key[, range_key], attribute, value)
 */
@Description(name = "ddb_update",
        value = "_FUNC_(hash_key, range_key(or null), attribute, value) - " + "Partial update for Dynamodb",
        extended = "")
public class GenericUDFDynamodbUpdate extends GenericUDF
{
    private Log LOG = LogFactory.getLog(GenericUDFDynamodbUpdate.class.getName());

    UpdateHelper updateHelper;

    private List<KeySchemaElement> keySchema;
    private String hashKeyName;
    private String rangeKeyName = null;
    private PrimitiveObjectInspector hashKeyOI;
    private PrimitiveObjectInspector rangeKeyOI = null;
    private StringObjectInspector[] attributeNameOIs;
    private ObjectInspectorConverters.Converter[] attributeValueConverters;
    private IntWritable ret = new IntWritable();

    @Override
    public void configure(MapredContext mapredContext)
    {
        if (mapredContext.isMap()) {
            throw new IllegalStateException("ddb_update should only be called inside reducer");
        }

        JobConf jobConf = mapredContext.getJobConf();
        updateHelper = new UpdateHelper(jobConf);

        LOG.warn(String.format("Gonna update Dynamodb's '%s' table", updateHelper.tableName));

        TableDescription describe = updateHelper.describeTable();
        keySchema = describe.getKeySchema();
        hashKeyName = keySchema.get(0).getAttributeName();
        if (keySchema.size() == 2) {
            rangeKeyName = keySchema.get(1).getAttributeName();
        }

        LOG.warn(String.format("We've got %d reducers, each will write at %d/s rate",
                updateHelper.reduceTasks, updateHelper.writeThroughput));
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] parameters) throws UDFArgumentException
    {
        Preconditions.checkArgument(parameters.length >= 3, "At least 3 arguments are expected.");

        int i = 0;
        Preconditions.checkArgument(parameters[i].getCategory().equals(ObjectInspector.Category.PRIMITIVE));
        hashKeyOI = (PrimitiveObjectInspector) parameters[i];
        PrimitiveCategory hashKeyCategory = hashKeyOI.getPrimitiveCategory();
        Preconditions.checkArgument(hashKeyCategory.equals(PrimitiveCategory.STRING) ||
                        hashKeyCategory.equals(PrimitiveCategory.INT) ||
                        hashKeyCategory.equals(PrimitiveCategory.LONG),
                "only String/int/long type hash key is supported");
        i++;

        Preconditions.checkArgument(parameters[i].getCategory().equals(ObjectInspector.Category.PRIMITIVE));
        rangeKeyOI = (PrimitiveObjectInspector) parameters[i];
        Preconditions.checkArgument(hashKeyCategory.equals(PrimitiveCategory.STRING) ||
                        hashKeyCategory.equals(PrimitiveCategory.INT) ||
                        hashKeyCategory.equals(PrimitiveCategory.LONG),
                "only String/int/long type range key is supported");
        i++;

        if (parameters.length == 3) {
            Preconditions.checkArgument(parameters[i].getCategory() == ObjectInspector.Category.MAP,
                    "Raw item write only accept Map object");
            attributeValueConverters = new ObjectInspectorConverters.Converter[1];
            attributeValueConverters[0] = ObjectInspectorConverters.getConverter(
                    parameters[i],
                    RubyScriptingUtils.resolveOI(parameters[i]));
        }
        else {
            int attrNum = (parameters.length - i) / 2;

            attributeNameOIs = new StringObjectInspector[attrNum];
            attributeValueConverters = new ObjectInspectorConverters.Converter[attrNum];

            for (int n = 0; n < attrNum; n++) {
                Preconditions.checkArgument(parameters[i] instanceof StringObjectInspector,
                        "only support string type attribute name but got " + parameters[i].getTypeName());
                attributeNameOIs[n] = (StringObjectInspector) parameters[i];
                i++;

                attributeValueConverters[n] = ObjectInspectorConverters.getConverter(parameters[i],
                        RubyScriptingUtils.resolveOI(parameters[i]));
                i++;
            }
        }

        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT);
    }

    @Override
    public Object evaluate(DeferredObject[] parameters) throws HiveException
    {
        PrimaryKey key;
        int next = 0;
        if (rangeKeyName != null) {
            key = new PrimaryKey(hashKeyName, hashKeyOI.getPrimitiveJavaObject(parameters[0].get()),
                    rangeKeyName, rangeKeyOI.getPrimitiveJavaObject(parameters[1].get()));
        }
        else {
            key = new PrimaryKey(hashKeyName, hashKeyOI.getPrimitiveJavaObject(parameters[0].get()));
        }
        next = 2;
        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                .withPrimaryKey(key)
                .withReturnValues(ReturnValue.NONE)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);

        if (attributeNameOIs == null) {
            Object value = attributeValueConverters[0].convert(parameters[next].get());
            if (value instanceof Map) {
                Map<Object, Object> map = (Map<Object, Object>) value;
                List<AttributeUpdate> attributeUpdates = new ArrayList<>();
                for (Map.Entry<Object, Object> a : map.entrySet()) {
                    attributeUpdates.add(new AttributeUpdate((String) a.getKey()).put(a.getValue()));
                }

                updateItemSpec.withAttributeUpdate(attributeUpdates);
            }
        }
        else {

            ImmutableList.Builder<AttributeUpdate> builder = ImmutableList.<AttributeUpdate>builder();

            for (int i = 0; i < attributeNameOIs.length; i++) {
                String attr = attributeNameOIs[i].getPrimitiveJavaObject(parameters[next].get()).toString();
                next++;
                Object value = attributeValueConverters[i].convert(parameters[next].get());

                next++;
                if (value instanceof String && ((String) value).isEmpty()) {
                    // skip empty string value...
                    // todo: should perform a delete op here
                    continue;
                }

                // hacking for Set....
                if (attr.equals("w_c") && value instanceof Map) {
                    Map map = (Map) value;
                    builder.add(new AttributeUpdate(attr).put(map.keySet()));
                }
                else {
                    builder.add(new AttributeUpdate(attr).put(value));
                }
            }

            updateItemSpec.withAttributeUpdate(builder.build());
        }

        updateHelper.update(updateItemSpec);
        ret.set(1);
        return ret;
    }

    @Override
    public void close() throws IOException
    {
        LOG.info(String.format("Consumed %f write capacities with %d item updated",
                updateHelper.getConsumedCapacityCounter(),
                updateHelper.getUpdateCounter()));
        LOG.info("Closing UDF ddb_update");
    }

    @Override
    public String getDisplayString(String[] children)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("ddb_update(");
        if (children.length > 0) {
            sb.append(children[0]);
            for (int i = 1; i < children.length; i++) {
                sb.append(", ");
                sb.append(children[i]);
            }
        }
        sb.append(")");
        return sb.toString();
    }
}
