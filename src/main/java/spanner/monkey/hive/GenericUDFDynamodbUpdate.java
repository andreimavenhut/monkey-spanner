package spanner.monkey.hive;

/**
 * Created by lan on 2/15/15.
 */

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;

import java.util.List;

/**
 * should always run in reducer side !!
 * ddb_update(hash_key[, range_key], attribute, value)
 */
@Description(name = "ddb_update",
        value = "_FUNC_(hash_key[, range_key], attribute, value) - " + "Partial update for Dynamodb",
        extended = "")
public class GenericUDFDynamodbUpdate extends GenericUDF {

    private Log LOG = LogFactory.getLog(GenericUDFDynamodbUpdate.class.getName());
    public static final String CONF_DYNAMODB_NAME = "ddb_update.table.name";
    //public static final String CONF_DYNAMODB_UPDATE_EXP = "ddb_update.update.expression";
    public static final String CONF_DYNAMODB_WRITE_THROUGHPUT = "ddb_update.write.throughput";
    public static final String CONF_DYNAMODB_WRITE_THROUGHPUT_PERCENTAGE = "ddb_update.write.throughput.percentage";

    private String tableName;
    private long configWriteThroughput;
    private double configWriteThroughputPercentage;
    private long writeThroughput;

    private DynamoDB ddb;
    private AmazonDynamoDBClient ddbClient;
    private Table ddbTable;
    private List<KeySchemaElement> keySchema;
    private String hashKeyName;
    private String rangeKeyName = null;
    private PrimitiveObjectInspector hashKeyOI;
    private PrimitiveObjectInspector rangeKeyOI = null;
    private StringObjectInspector attributeNameOI;
    private PrimitiveObjectInspector attributeValueOI;
    private IntWritable ret = new IntWritable();

    private long updateCounter = 0;
    private Long start = null;

    @Override
    public void configure(MapredContext mapredContext) {
        if (mapredContext.isMap()) {
            throw new IllegalStateException("ddb_update should only be called inside reducer");
        }

        JobConf jobConf = mapredContext.getJobConf();
        tableName = jobConf.get(CONF_DYNAMODB_NAME);
        configWriteThroughput = jobConf.getLong(CONF_DYNAMODB_WRITE_THROUGHPUT, 0);
        configWriteThroughputPercentage = jobConf.getDouble(CONF_DYNAMODB_WRITE_THROUGHPUT_PERCENTAGE, 0.5);

        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "%s must be set!", CONF_DYNAMODB_NAME);

        ClientConfiguration configuration = new ClientConfiguration();
        ddbClient = Region.getRegion(Regions.AP_NORTHEAST_1).createClient(AmazonDynamoDBClient.class,
                new InstanceProfileCredentialsProvider(), configuration);
        ddb = new DynamoDB(ddbClient);

        ddbTable = ddb.getTable(tableName);
        TableDescription describe = ddbTable.describe();
        keySchema = describe.getKeySchema();
        hashKeyName = keySchema.get(0).getAttributeName();
        if (keySchema.size() == 2) {
            rangeKeyName = keySchema.get(1).getAttributeName();
        }

        LOG.warn(String.format("Gonna update Dynamodb's '%s' table", tableName));

        int tasks = jobConf.getNumReduceTasks();
        Long provisionedWriteCapacityUnits = describe.getProvisionedThroughput().getWriteCapacityUnits();
        if (configWriteThroughput == 0 || configWriteThroughput > provisionedWriteCapacityUnits) {
            writeThroughput = (long) (provisionedWriteCapacityUnits * configWriteThroughputPercentage / tasks);
        } else {
            writeThroughput = configWriteThroughput / tasks;
        }

        Preconditions.checkState(writeThroughput > 0, "writeThroughput can't be 0, please try set %s or %s",
                CONF_DYNAMODB_WRITE_THROUGHPUT, CONF_DYNAMODB_WRITE_THROUGHPUT_PERCENTAGE);

        LOG.warn(String.format("We've got %d reducers, each will write at %d/s rate", tasks, writeThroughput));
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] parameters) throws UDFArgumentException {
        Preconditions.checkArgument(parameters.length >= 3 && parameters.length <= 4,
                "Exactly 3 or 4 arguments are expected.");

        int i = 0;
        Preconditions.checkArgument(parameters[i].getCategory().equals(ObjectInspector.Category.PRIMITIVE));
        hashKeyOI = (PrimitiveObjectInspector) parameters[i];
        PrimitiveCategory hashKeyCategory = hashKeyOI.getPrimitiveCategory();

        Preconditions.checkArgument(hashKeyCategory.equals(PrimitiveCategory.STRING) ||
                        hashKeyCategory.equals(PrimitiveCategory.INT) ||
                        hashKeyCategory.equals(PrimitiveCategory.LONG),
                "only String/int/long type hash key is supported");

        i++;

        if (parameters.length == 4) {
            Preconditions.checkArgument(parameters[i].getCategory().equals(ObjectInspector.Category.PRIMITIVE));
            rangeKeyOI = (PrimitiveObjectInspector) parameters[i];
            Preconditions.checkArgument(hashKeyCategory.equals(PrimitiveCategory.STRING) ||
                            hashKeyCategory.equals(PrimitiveCategory.INT) ||
                            hashKeyCategory.equals(PrimitiveCategory.LONG),
                    "only String/int/long type range key is supported");
            i++;
        }

        Preconditions.checkArgument(parameters[i] instanceof StringObjectInspector,
                "only support string type attribute name but got " + parameters[i].getTypeName());
        attributeNameOI = (StringObjectInspector) parameters[i];
        i++;

        attributeValueOI = (PrimitiveObjectInspector) parameters[i];

        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT);
    }

    @Override
    public Object evaluate(DeferredObject[] parameters) throws HiveException {

        PrimaryKey key;
        int next;
        if (rangeKeyName != null) {
            key = new PrimaryKey(hashKeyName, hashKeyOI.getPrimitiveJavaObject(parameters[0].get()),
                    rangeKeyName, rangeKeyOI.getPrimitiveJavaObject(parameters[1].get()));
            next = 2;
        } else {
            key = new PrimaryKey(hashKeyName, hashKeyOI.getPrimitiveJavaObject(parameters[0].get()));
            next = 1;
        }

        AttributeUpdate update = new AttributeUpdate(
                attributeNameOI.getPrimitiveJavaObject(parameters[next].get())).put(
                attributeValueOI.getPrimitiveJavaObject(parameters[next + 1].get())
        );

        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                .withPrimaryKey(key)
                .withReturnValues(ReturnValue.NONE)
                .withAttributeUpdate(update);

        update(updateItemSpec);
        ret.set(1);
        return ret;
    }

    private void update(UpdateItemSpec spec) throws HiveException {
        if (start == null) {
            start = System.nanoTime();
        } else {
            long end = start + updateCounter * 1_000_000_000L / writeThroughput;
            while (System.nanoTime() < end) ;
        }

        try {
            ddbTable.updateItem(spec);
        } catch (AmazonServiceException e) {
            LOG.error(String.format("update failed on key (%s): %s", 
                    spec.getKeyComponents(), spec.getAttributeUpdate()));
            throw new HiveException(e);
        }

        updateCounter++;
    }

    @Override
    public String getDisplayString(String[] children) {
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
