package project.com.masterbd.Utiles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * A client class that serves to create connection and send data to HBase.
 */
class HBaseClient implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseClient.class);

    private Configuration hbConfig;
    private Connection connection;
    private Table table;

    public HBaseClient(Configuration hbConfig) {
        this.hbConfig = hbConfig;
    }

    public void connect(String tableName) throws IOException {
        connection = ConnectionFactory.createConnection(hbConfig);
        TableName name = TableName.valueOf(tableName);
        table = connection.getTable(name);
    }

    public void send(List<Mutation> mutations) throws IOException, InterruptedException {
        Object[] results = new Object[mutations.size()];
        table.batch(mutations, results);
    }

    public Configuration getConfig() {
        return this.hbConfig;
    }

    @Override
    public void close() throws IOException {
        try {
            if (table != null) {
                table.close();
            }
        } catch (Exception e) {
            LOG.error("Error while closing HBase table.", e);
        }
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            LOG.error("Error while closing HBase connection.", e);
        }
    }
}

