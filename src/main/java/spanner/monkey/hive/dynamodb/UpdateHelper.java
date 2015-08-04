package spanner.monkey.hive.dynamodb;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.mapred.JobConf;

/**
 * Created by yuyanglan on 6/23/15.
 */
public class UpdateHelper
{
    public static final String CONF_DYNAMODB_NAME = "ddb_update.table.name";
    public static final String CONF_DYNAMODB_WRITE_THROUGHPUT = "ddb_update.write.throughput";
    public static final String CONF_DYNAMODB_WRITE_THROUGHPUT_PERCENTAGE = "ddb_update.write.throughput.percentage";
    public final String tableName;
    public final long configWriteThroughput;
    public final double configWriteThroughputPercentage;
    public final long writeThroughput;
    public final int reduceTasks;
    private final TableDescription tableDescribe;
    private Log LOG = LogFactory.getLog(UpdateHelper.class.getName());
    private DynamoDB ddb;
    private AmazonDynamoDBClient ddbClient;
    private Table ddbTable;
    private long updateCounter = 0;
    private double consumedCapacityCounter = 0;
    private Long start = null;

    public UpdateHelper(JobConf jobConf)
    {
        this.tableName = jobConf.get(UpdateHelper.CONF_DYNAMODB_NAME);
        this.configWriteThroughput = jobConf.getLong(UpdateHelper.CONF_DYNAMODB_WRITE_THROUGHPUT, 0);
        this.configWriteThroughputPercentage = jobConf.getDouble(UpdateHelper.CONF_DYNAMODB_WRITE_THROUGHPUT_PERCENTAGE, 0.5);
        this.reduceTasks = jobConf.getNumReduceTasks();

        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "%s must be set!", CONF_DYNAMODB_NAME);

        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setUseTcpKeepAlive(true);
        configuration.setUseGzip(true);
        ddbClient = Region.getRegion(Regions.AP_NORTHEAST_1).createClient(AmazonDynamoDBClient.class,
                new InstanceProfileCredentialsProvider(), configuration);
        ddb = new DynamoDB(ddbClient);

        ddbTable = ddb.getTable(tableName);
        tableDescribe = ddbTable.describe();

        Long provisionedWriteCapacityUnits = tableDescribe.getProvisionedThroughput().getWriteCapacityUnits();
        if (configWriteThroughput == 0 || configWriteThroughput > provisionedWriteCapacityUnits) {
            writeThroughput = (long) (provisionedWriteCapacityUnits * configWriteThroughputPercentage / reduceTasks);
        }
        else {
            writeThroughput = configWriteThroughput / reduceTasks;
        }

        Preconditions.checkState(writeThroughput > 0, "writeThroughput can't be 0, please try set %s or %s",
                CONF_DYNAMODB_WRITE_THROUGHPUT, CONF_DYNAMODB_WRITE_THROUGHPUT_PERCENTAGE);
    }

    public TableDescription describeTable()
    {
        return tableDescribe;
    }

    public void update(UpdateItemSpec spec) throws HiveException
    {
        if (start == null) {
            start = System.nanoTime();
        }
        else {
            long end = Math.round(start + consumedCapacityCounter * 1_000_000_000L / writeThroughput);
            if (System.nanoTime() < end) {
                LOG.debug(String.format("waiting for throughput (%d writes, %f consumed capacity in %d ns",
                        updateCounter, consumedCapacityCounter, end - start));
            }
            while (System.nanoTime() < end) ;
        }

        try {
            UpdateItemOutcome outcome = ddbTable.updateItem(spec);
            consumedCapacityCounter += outcome.getUpdateItemResult().getConsumedCapacity().getCapacityUnits();
            updateCounter++;
        }
        catch (ProvisionedThroughputExceededException e) {
            LOG.warn(String.format("ProvisionedThroughputExceededException occured, try to speed down!",
                    spec.getKeyComponents(), spec.getAttributeUpdate()));
            throw new HiveException(e);

        }
        catch (AmazonServiceException e) {
            LOG.error(String.format("update failed on key (%s): %s",
                    spec.getKeyComponents(), spec.getAttributeUpdate()));
            throw new HiveException(e);
        }

        if ((updateCounter * 1.0 / writeThroughput) % 30 == 0) {
            LOG.info(String.format("updated %d items, consumed %f capacity", updateCounter, consumedCapacityCounter));
        }
    }

    public long getUpdateCounter()
    {
        return updateCounter;
    }

    public double getConsumedCapacityCounter()
    {
        return consumedCapacityCounter;
    }
}
