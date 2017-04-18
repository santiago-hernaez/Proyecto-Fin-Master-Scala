package project.com.masterbd.Utiles;


         import java.io.IOException;

         import org.apache.flink.api.common.io.OutputFormat;
         import org.apache.flink.configuration.Configuration;
         import org.apache.flink.streaming.api.datastream.DataStream;
         import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
         import org.apache.flink.streaming.api.functions.source.SourceFunction;
         import org.apache.hadoop.hbase.HBaseConfiguration;
         import org.apache.hadoop.hbase.TableName;
         import org.apache.hadoop.hbase.client.Table;
         import org.apache.hadoop.hbase.client.Connection;
         import org.apache.hadoop.hbase.client.ConnectionFactory;
         import org.apache.hadoop.hbase.client.HTable;
         import org.apache.hadoop.hbase.client.Put;
         import org.apache.hadoop.hbase.util.Bytes;
         import project.com.masterbd.Datos.datoEnriquecido;

/**
 *
 * This class that writes streams into HBase.
 *
 */



    public class toHbase implements OutputFormat<datoEnriquecido.enriquecido> {

        private org.apache.hadoop.conf.Configuration conf = null;
        private HTable table;
        private String taskNumber = null;


        private static final long serialVersionUID = 1L;

        @Override
        public void configure(Configuration parameters) {
            conf = HBaseConfiguration.create();
        }

        @Override
        public void open(int taskNumber, int numTasks) throws IOException {
            //Connection connection = ConnectionFactory.createConnection(conf);
            //Table table = connection.getTable(TableName.valueOf("inditexTable"));

            //inditexTable is a table created on Hbase previously
            table = new HTable(conf, "inditexTable");
            this.taskNumber = String.valueOf(taskNumber);
        }

        @Override
        public void writeRecord(datoEnriquecido.enriquecido record) throws IOException {
            // 3 column families: D to data, T to Stores info, P to clothes info
            final byte[] D = Bytes.toBytes("D");
            final byte[] P = Bytes.toBytes("P");
            final byte[] T = Bytes.toBytes("T");

            //Set RowKey as cadena+nombredeprenda
            Put put = new Put(Bytes.toBytes(record.cadena()+record.nombre()));

            //columnFamily,column,value
            put.addColumn(D, Bytes.toBytes("metodoPago"),
                    Bytes.toBytes(record.metodoPago()));
            put.addColumn(D, Bytes.toBytes("fecha"),
                    Bytes.toBytes(record.fecha()));
            put.addColumn(T, Bytes.toBytes("id_Tienda"),
                    Bytes.toBytes(record.id_tienda()));
            put.addColumn(T, Bytes.toBytes("cadena"),
                    Bytes.toBytes(record.cadena()));
            put.addColumn(T, Bytes.toBytes("sexo"),
                    Bytes.toBytes(record.sexo()));
            put.addColumn(T, Bytes.toBytes("pais"),
                    Bytes.toBytes(record.pais()));
            put.addColumn(T, Bytes.toBytes("region"),
                    Bytes.toBytes(record.region()));
            put.addColumn(T, Bytes.toBytes("zona"),
                    Bytes.toBytes(record.zona()));
            put.addColumn(P, Bytes.toBytes("id_prenda"),
                    Bytes.toBytes(record.id_prenda()));
            put.addColumn(P, Bytes.toBytes("precio"),
                    Bytes.toBytes(record.precio()));
            put.addColumn(P, Bytes.toBytes("beneficio"),
                    Bytes.toBytes(record.beneficio()));
            put.addColumn(P, Bytes.toBytes("color"),
                    Bytes.toBytes(record.color()));
            put.addColumn(P, Bytes.toBytes("talla"),
                    Bytes.toBytes(record.talla()));
            put.addColumn(P, Bytes.toBytes("nombre"),
                    Bytes.toBytes(record.nombre()));
            put.addColumn(P, Bytes.toBytes("modelo"),
                    Bytes.toBytes(record.modelo()));
            put.addColumn(P, Bytes.toBytes("clase"),
                    Bytes.toBytes(record.clase()));

            table.put(put);
        }

        @Override
        public void close() throws IOException {
            table.flushCommits();
            table.close();
        }

 }

