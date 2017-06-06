package project.com.masterbd.Utiles;

import org.apache.hadoop.hbase.util.Bytes;
import project.com.masterbd.Datos.datoEnriquecido;


public class HBaseMapperP implements HBaseMapper<datoEnriquecido.enriquecido>{

        private static final long serialVersionUID = 1L;

        final byte[] D = Bytes.toBytes("D");
        final byte[] P = Bytes.toBytes("P");
        final byte[] T = Bytes.toBytes("T");
        final byte[] data = Bytes.toBytes("DATA");

        public HBaseMapperP() {

        }

        @Override
        public byte[] getRowKey(datoEnriquecido.enriquecido value) {
            String fechaRow = value.fecha().substring(0, 13) + " ";
            byte [] rowKey = Bytes.toBytes(value.cadena());
            switch (value.destino()) {

                case "INDITEXTABLE":
                    rowKey =  Bytes.toBytes(value.cadena() + value.id_transaccion());
                break;
                case "VENTASPORTIENDA":
                    rowKey =  Bytes.toBytes(value.cadena() + value.id_tienda());
                break;
                case "VENTASPORCADENA":
                    rowKey =  Bytes.toBytes(fechaRow + value.cadena());
                break;
                case "VENTASPORZONA":
                    rowKey =  Bytes.toBytes(fechaRow + value.zona()+value.pais());
                break;
                case "TOPPRENDAS":
                    rowKey =  Bytes.toBytes(fechaRow + value.cadena()+value.modelo()+value.clase());
                break;
                case "TOPCOLORES":
                    rowKey =  Bytes.toBytes(fechaRow + value.color());
                break;
            }
            return rowKey;
        }

        @Override
        public void addActions(datoEnriquecido.enriquecido value, MutationActions mutActions) {
            switch (value.destino()) {

                case "INDITEXTABLE":
                    mutActions.addPut(D, Bytes.toBytes("ID_TRANSACCION"),
                            Bytes.toBytes(value.id_transaccion()));
                    mutActions.addPut(D, Bytes.toBytes("METODOPAGO"),
                            Bytes.toBytes(value.metodoPago()));
                    mutActions.addPut(D, Bytes.toBytes("FECHA"),
                            Bytes.toBytes(value.fecha()));
                    mutActions.addPut(T, Bytes.toBytes("ID_TIENDA"),
                            Bytes.toBytes(value.id_tienda()));
                    mutActions.addPut(T, Bytes.toBytes("CADENA"),
                            Bytes.toBytes(value.cadena()));
                    mutActions.addPut(T, Bytes.toBytes("SEXO"),
                            Bytes.toBytes(value.sexo()));
                    mutActions.addPut(T, Bytes.toBytes("PAIS"),
                            Bytes.toBytes(value.pais()));
                    mutActions.addPut(T, Bytes.toBytes("REGION"),
                            Bytes.toBytes(value.region()));
                    mutActions.addPut(T, Bytes.toBytes("ZONA"),
                            Bytes.toBytes(value.zona()));
                    mutActions.addPut(P, Bytes.toBytes("ID_PRENDA"),
                            Bytes.toBytes(value.id_prenda()));
                    mutActions.addPut(P, Bytes.toBytes("PRECIO"),
                            Bytes.toBytes(value.precio()));
                    mutActions.addPut(P, Bytes.toBytes("BENEFICIO"),
                            Bytes.toBytes(value.beneficio()));
                    mutActions.addPut(P, Bytes.toBytes("COLOR"),
                            Bytes.toBytes(value.color()));
                    mutActions.addPut(P, Bytes.toBytes("TALLA"),
                            Bytes.toBytes(value.talla()));
                    mutActions.addPut(P, Bytes.toBytes("NOMBRE"),
                            Bytes.toBytes(value.nombre()));
                    mutActions.addPut(P, Bytes.toBytes("MODELO"),
                            Bytes.toBytes(value.modelo()));
                    mutActions.addPut(P, Bytes.toBytes("CLASE"),
                            Bytes.toBytes(value.clase()));
                    break;
                case "VENTASPORTIENDA":
                    mutActions.addPut(data, Bytes.toBytes("FECHA"),
                            Bytes.toBytes(value.fecha()));
                    mutActions.addPut(data, Bytes.toBytes("ID_TIENDA"),
                            Bytes.toBytes(value.id_tienda()));
                    mutActions.addPut(data, Bytes.toBytes("CADENA"),
                            Bytes.toBytes(value.cadena()));
                    mutActions.addPut(data, Bytes.toBytes("SEXO"),
                            Bytes.toBytes(value.sexo()));
                    mutActions.addPut(data, Bytes.toBytes("PAIS"),
                            Bytes.toBytes(value.pais()));
                    mutActions.addPut(data, Bytes.toBytes("REGION"),
                            Bytes.toBytes(value.region()));
                    mutActions.addPut(data, Bytes.toBytes("ZONA"),
                            Bytes.toBytes(value.zona()));
                    mutActions.addPut(data, Bytes.toBytes("TOTAL"),
                            Bytes.toBytes(value.precio()));
                    break;
                case "VENTASPORCADENA":
                    mutActions.addPut(data, Bytes.toBytes("FECHA"),
                            Bytes.toBytes(value.fecha()));
                    mutActions.addPut(data, Bytes.toBytes("CADENA"),
                            Bytes.toBytes(value.cadena()));
                    mutActions.addPut(data, Bytes.toBytes("TOTAL"),
                            Bytes.toBytes(value.precio()));
                    break;
                case "VENTASPORZONA":
                    mutActions.addPut(data, Bytes.toBytes("FECHA"),
                            Bytes.toBytes(value.fecha()));
                    mutActions.addPut(data, Bytes.toBytes("ZONA"),
                            Bytes.toBytes(value.zona()));
                    mutActions.addPut(data, Bytes.toBytes("PAIS"),
                            Bytes.toBytes(value.pais()));
                    mutActions.addPut(data, Bytes.toBytes("TOTAL"),
                            Bytes.toBytes(value.precio()));
                    break;
                case "TOPPRENDAS":
                    mutActions.addPut(data, Bytes.toBytes("FECHA"),
                            Bytes.toBytes(value.fecha()));
                    mutActions.addPut(data, Bytes.toBytes("CADENA"),
                            Bytes.toBytes(value.cadena()));
                    mutActions.addPut(data, Bytes.toBytes("CLASE"),
                            Bytes.toBytes(value.clase()));
                    mutActions.addPut(data, Bytes.toBytes("MODELO"),
                            Bytes.toBytes(value.modelo()));
                    mutActions.addPut(data, Bytes.toBytes("CANTIDAD"),
                            Bytes.toBytes(value.id_transaccion()));
                    break;
                case "TOPCOLORES":
                    mutActions.addPut(data, Bytes.toBytes("FECHA"),
                            Bytes.toBytes(value.fecha()));
                    mutActions.addPut(data, Bytes.toBytes("COLOR"),
                            Bytes.toBytes(value.color()));
                    mutActions.addPut(data, Bytes.toBytes("CANTIDAD"),
                            Bytes.toBytes(value.id_transaccion()));

            }
        }

}
