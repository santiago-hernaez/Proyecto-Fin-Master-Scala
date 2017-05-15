package project.com.masterbd.Utiles;


         import java.io.IOException;

         import org.apache.flink.api.common.io.OutputFormat;
         import org.apache.flink.configuration.Configuration;
         import org.apache.flink.streaming.api.datastream.DataStream;
         import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
         import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
         import org.apache.flink.streaming.api.functions.source.SourceFunction;
         import org.apache.hadoop.hbase.HBaseConfiguration;
         import org.apache.hadoop.hbase.HTableDescriptor;
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
    //public class toHbase implements RichSinkFunction <datoEnriquecido.enriquecido> {
        private org.apache.hadoop.conf.Configuration conf = null;
        private HTable inditexTable;
        private HTable ventasPorTienda;
        private HTable ventasPorCadena;
        private HTable ventasPorZona;
        private HTable topPrendas;
        private HTable topColores;

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
            //Table ventasPorTienda = connection.getTable(TableName.valueOf("ventasPorTienda"));
           //inditexTable is a table created on Hbase previously
            //TODO: Cambiar el HTable por la nueva sintaxis. Probar si vale para varias tablas...
            inditexTable = new HTable(conf, "INDITEXTABLE");
            ventasPorTienda = new HTable(conf, "VENTASPORTIENDA");
            ventasPorCadena = new HTable(conf, "VENTASPORCADENA");
            ventasPorZona = new HTable(conf, "VENTASPORZONA");
            topPrendas = new HTable(conf, "TOPPRENDAS");
            topColores = new HTable (conf, "TOPCOLORES");
            this.taskNumber = String.valueOf(taskNumber);
        }

        @Override
        public void writeRecord(datoEnriquecido.enriquecido record) throws IOException {
            // 3 column families: D to data, T to Stores info, P to clothes info
            final byte[] D = Bytes.toBytes("D");
            final byte[] P = Bytes.toBytes("P");
            final byte[] T = Bytes.toBytes("T");
            final byte[] data = Bytes.toBytes("DATA");
            //Remove minutes from fecha in order to use it for RowKey.
            String fechaRow = record.fecha().substring(0, 13) + " ";
            Put put = new Put(Bytes.toBytes(record.cadena() + record.id_transaccion()));
            switch (record.destino()) {

                case "inditexTable":
                    //Set RowKey as cadena+nombredeprenda
                    put = new Put(Bytes.toBytes(record.cadena() + record.id_transaccion()));

                    //columnFamily,column,value
                    put.addColumn(D, Bytes.toBytes("ID_TRANSACCION"),
                            Bytes.toBytes(record.id_transaccion()));
                    put.addColumn(D, Bytes.toBytes("METODOPAGO"),
                            Bytes.toBytes(record.metodoPago()));
                    put.addColumn(D, Bytes.toBytes("FECHA"),
                            Bytes.toBytes(record.fecha()));
                    put.addColumn(T, Bytes.toBytes("ID_TIENDA"),
                            Bytes.toBytes(record.id_tienda()));
                    put.addColumn(T, Bytes.toBytes("CADENA"),
                            Bytes.toBytes(record.cadena()));
                    put.addColumn(T, Bytes.toBytes("SEXO"),
                            Bytes.toBytes(record.sexo()));
                    put.addColumn(T, Bytes.toBytes("PAIS"),
                            Bytes.toBytes(record.pais()));
                    put.addColumn(T, Bytes.toBytes("REGION"),
                            Bytes.toBytes(record.region()));
                    put.addColumn(T, Bytes.toBytes("ZONA"),
                            Bytes.toBytes(record.zona()));
                    put.addColumn(P, Bytes.toBytes("ID_PRENDA"),
                            Bytes.toBytes(record.id_prenda()));
                    put.addColumn(P, Bytes.toBytes("PRECIO"),
                            Bytes.toBytes(record.precio()));
                    put.addColumn(P, Bytes.toBytes("BENEFICIO"),
                            Bytes.toBytes(record.beneficio()));
                    put.addColumn(P, Bytes.toBytes("COLOR"),
                            Bytes.toBytes(record.color()));
                    put.addColumn(P, Bytes.toBytes("TALLA"),
                            Bytes.toBytes(record.talla()));
                    put.addColumn(P, Bytes.toBytes("NOMBRE"),
                            Bytes.toBytes(record.nombre()));
                    put.addColumn(P, Bytes.toBytes("MODELO"),
                            Bytes.toBytes(record.modelo()));
                    put.addColumn(P, Bytes.toBytes("CLASE"),
                            Bytes.toBytes(record.clase()));

                    inditexTable.put(put);
                    break;


                case "ventasPorTienda":
                    //Set RowKey as cadena+id_tienda
                    put = new Put(Bytes.toBytes(record.cadena() + record.id_tienda()));

                    //columnFamily,column,value
                    put.addColumn(data, Bytes.toBytes("FECHA"),
                            Bytes.toBytes(record.fecha()));
                    put.addColumn(data, Bytes.toBytes("ID_TIENDA"),
                            Bytes.toBytes(record.id_tienda()));
                    put.addColumn(data, Bytes.toBytes("CADENA"),
                            Bytes.toBytes(record.cadena()));
                    put.addColumn(data, Bytes.toBytes("SEXO"),
                            Bytes.toBytes(record.sexo()));
                    put.addColumn(data, Bytes.toBytes("PAIS"),
                            Bytes.toBytes(record.pais()));
                    put.addColumn(data, Bytes.toBytes("REGION"),
                            Bytes.toBytes(record.region()));
                    put.addColumn(data, Bytes.toBytes("ZONA"),
                            Bytes.toBytes(record.zona()));
                    put.addColumn(data, Bytes.toBytes("TOTAL"),
                            Bytes.toBytes(record.precio()));

                    ventasPorTienda.put(put);
                    break;


                case "ventasPorCadena":

                    //Set RowKey as cadena+id_tienda
                    put = new Put(Bytes.toBytes(fechaRow + record.cadena()));

                    //columnFamily,column,value
                    put.addColumn(data, Bytes.toBytes("FECHA"),
                            Bytes.toBytes(record.fecha()));
                    put.addColumn(data, Bytes.toBytes("CADENA"),
                            Bytes.toBytes(record.cadena()));
                    put.addColumn(data, Bytes.toBytes("TOTAL"),
                            Bytes.toBytes(record.precio()));

                    ventasPorCadena.put(put);
                    break;


                case "ventasPorZona":

                    //Set RowKey as fecha+zona

                    put = new Put(Bytes.toBytes(fechaRow + record.zona()));

                    //columnFamily,column,value
                    put.addColumn(data, Bytes.toBytes("FECHA"),
                            Bytes.toBytes(record.fecha()));
                    put.addColumn(data, Bytes.toBytes("ZONA"),
                            Bytes.toBytes(record.zona()));
                    put.addColumn(data, Bytes.toBytes("TOTAL"),
                            Bytes.toBytes(record.precio()));

                    ventasPorZona.put(put);
                    break;

                case "topPrendas":
                    put = new Put(Bytes.toBytes(fechaRow + record.cadena()+record.modelo()+record.clase()));

                    //columnFamily,column,value
                    put.addColumn(data, Bytes.toBytes("FECHA"),
                            Bytes.toBytes(record.fecha()));
                    put.addColumn(data, Bytes.toBytes("CADENA"),
                            Bytes.toBytes(record.cadena()));
                    put.addColumn(data, Bytes.toBytes("CLASE"),
                            Bytes.toBytes(record.clase()));
                    put.addColumn(data, Bytes.toBytes("MODELO"),
                            Bytes.toBytes(record.modelo()));
                    put.addColumn(data, Bytes.toBytes("CANTIDAD"),
                            Bytes.toBytes((record.precio())));

                    topPrendas.put(put);
                    break;

                case "topColores":
                    put = new Put(Bytes.toBytes(fechaRow + record.color()));

                    //columnFamily,column,value
                    put.addColumn(data, Bytes.toBytes("FECHA"),
                            Bytes.toBytes(record.fecha()));
                    put.addColumn(data, Bytes.toBytes("COLOR"),
                            Bytes.toBytes(record.color()));
                    put.addColumn(data, Bytes.toBytes("CANTIDAD"),
                            Bytes.toBytes(record.precio()));

                    topColores.put(put);
                    break;
            }
        }

        @Override
        public void close() throws IOException {
            inditexTable.flushCommits();
            inditexTable.close();
            ventasPorTienda.flushCommits();
            ventasPorTienda.close();
            ventasPorCadena.flushCommits();
            ventasPorCadena.close();
            ventasPorZona.flushCommits();
            ventasPorZona.close();
            topPrendas.flushCommits();
            topPrendas.close();
            topColores.flushCommits();
            topColores.close();

        }

 }

