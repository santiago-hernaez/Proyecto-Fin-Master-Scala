package project.com.masterbd.Utiles;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A sink that writes its input to an HBase table.
 * To create this sink you need to pass two arguments the name of the HBase table and {@link HBaseMapper}.
 * A boolean field writeToWAL can also be set to enable or disable Write Ahead Log (WAL).
 * HBase config files must be located in the classpath to create a connection to HBase cluster.
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public class HBaseSink<IN> extends RichSinkFunction<IN> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(HBaseSink.class);

    private transient HBaseClient client;
    private transient MutationActions mutActions;
    private String tableName;
    private HBaseMapper<IN> mapper;
    private boolean writeToWAL = true;

    /**
     * The main constructor for creating HBaseSink.
     *
     * @param tableName the name of the HBase table
     * @param mapper the mapping of input a value to a HBase row
     */
    public HBaseSink(String tableName, HBaseMapper<IN> mapper) {
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "Table name cannot be null or empty.");
        Preconditions.checkArgument(mapper != null, "HBase mapper cannot be null.");
        this.tableName = tableName;
        this.mapper = mapper;
    }

    /**
     * Enable or disable WAL when writing to HBase.
     * Set to true (default) if you want to write {@link Mutation}s to the WAL synchronously.
     * Set to false if you do not want to write {@link Mutation}s to the WAL.
     *
     * @param writeToWAL
     * @return the HBaseSink with specified writeToWAL value
     */
    public HBaseSink writeToWAL(boolean writeToWAL) {
        this.writeToWAL = writeToWAL;
        return this;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        this.client = new HBaseClient(HBaseConfiguration.create());
        this.mutActions = new MutationActions();
        try {
            client.connect(tableName);
        } catch (IOException e) {
            LOG.error("HBase sink preparation failed.", e);
            throw e;
        }
    }

    @Override
    public void invoke(IN value) throws Exception {
        mutActions.clearActions();
        byte[] rowKey = mapper.getRowKey(value);
        mapper.addActions(value, mutActions);
        try {
            mutActions.sendActions(client, rowKey, writeToWAL);
        } catch (Exception e) {
            LOG.error("Error while sending data to HBase.", e);
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}