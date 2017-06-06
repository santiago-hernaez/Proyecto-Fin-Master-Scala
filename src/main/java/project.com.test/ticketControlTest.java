package project.com.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple18;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.flinkspector.core.quantify.MatchTuples;
import org.flinkspector.core.quantify.OutputMatcher;
import org.flinkspector.datastream.DataStreamTestBase;

import static org.hamcrest.Matchers.*;

public class ticketControlTest extends DataStreamTestBase {

/* Para el conjunto de valores dados, confirmamos que el precio total debe ser 807.62 */
/* Confirmamos que no hay mas de 2 de cada color */
/* Y confirmamos que no hay mas de 1 prenda de cada tipo, modelo y cadena. */
    
    public static DataStream<Tuple18<String, Integer, String, String, Integer, String, String, String, String, String, Integer, Double, Double, String, String, String, String, String>> ventasPorTienda(DataStream<Tuple18<String, Integer, String, String, Integer, String, String, String, String, String, Integer, Double, Double, String, String, String, String, String>> stream) {
        return stream.timeWindowAll(Time.of(20, seconds))
                .sum(11);
    }


    @org.junit.Test
    public void testTotalPrecio() {

        // Define the input DataStream:
        DataStream<Tuple18<String, Integer, String, String, Integer, String, String, String, String, String, Integer, Double, Double, String, String, String, String, String>> testStream =
                createTimedTestStreamWith(Tuple18.of("INDITEXTABLE", 0, "2017/05/19 11:27:19", "App", 2294, "PullBear", "Hombre", "ESPANA", "Oeste", "Europa", 10842, 85.26, 9.04, "Marron", "S", "JerseyLaMarS", "Largo", "Jersey"))
                        .emit(Tuple18.of("INDITEXTABLE", 1, "2017/05/19 11:27:20", "App", 2294, "PullBear", "Hombre", "ESPANA", "Oeste", "Europa", 10298, 190.92, 6.4, "Coral", "L", "ChaquetaCoCorL", "Corto", "Chaqueta"))
                        .emit(Tuple18.of("INDITEXTABLE", 2, "2017/05/19 11:27:21", "Tarjeta", 2294, "PullBear", "Hombre", "ESPANA", "Oeste", "Europa", 8531, 97.06, 3.1, "Granate", "XXL", "ChaquetaMiGraXXL", "Midi", "Chaqueta"))
                        .emit(Tuple18.of("INDITEXTABLE", 3, "2017/05/19 11:27:22", "Tarjeta", 2294, "PullBear", "Hombre", "ESPANA", "Oeste", "Europa", 10588, 48.38, 7.56, "Negro", "XS", "FaldaLaNegXS", "Largo", "Falda"))
                        .emit(Tuple18.of("INDITEXTABLE", 10, "2017/05/19 11:27:23", "Efectivo", 7303, "WebPull", "Web", "GEORGIA", "Web", "Europa", 11682, 64.05, 6.29, "Negro", "S", "ChaquetonLaNegS", "Largo", "Chaqueton"))
                        .emit(Tuple18.of("INDITEXTABLE", 20, "2017/05/19 11:27:24", "Affinity", 4035, "Bershka", "Mujer", "ESPANA", "Este", "Europa", 8149, 179.46, 9.76, "Beige", "M", "PantalonLaBeiM", "Largo", "Pantalon"))
                        .emit(Tuple18.of("INDITEXTABLE", 21, "2017/05/19 11:27:25", "App", 4035, "Bershka", "Mujer", "ESPANA", "Este", "Europa", 13061, 142.49, 3.0, "Rosa", "XS", "GabardinaLaRosXS", "Largo", "Gabardina"))
                        .close();
        OutputMatcher<Tuple18<String, Integer, String, String, Integer, String, String, String, String, String, Integer, Double, Double, String, String, String, String, String>> matcher =
                //name the values in the tuple with keys:
                new MatchTuples<Tuple18<String, Integer, String, String, Integer, String, String, String, String, String, Integer, Double, Double, String, String, String, String, String>>("destino", "id_transaccion", "fecha", "metodoPago", "id_tienda", "cadena", "sexo", "pais", "region", "zona", "id_prenda", "precio", "beneficio", "color", "talla", "nombre", "modelo", "clase")
                        //Confirm the price sum is what it should be.
                        .assertThat("precio", is(807.62))
                        //Confirm there are no strange payment methods.
                        .assertThat("metodoPago", either(is("App")).or(is("Credito")).or(is("Efectivo")).or(is("Affinity")))
                        //define how many records need to fulfill the condition
                        .onEachRecord();

        assertStream(ventasPorTienda(testStream), matcher);
    }

    public class mapea18a4 implements MapFunction<Tuple18<String, Integer, String, String, Integer, String, String, String, String, String, Integer, Double, Double, String, String, String, String, String>, Tuple4<String, String, String, Integer>> {
            @Override
            public Tuple4<String, String, String, Integer> map(Tuple18<String, Integer, String, String, Integer, String, String, String, String, String, Integer, Double, Double, String, String, String, String, String> tupla18) throws Exception {
                return new Tuple4<String, String, String, Integer>(tupla18.f0, tupla18.f2, tupla18.f13, 1);
            }
    }


    public DataStream<Tuple4<String, String, String, Integer>>
             TopColores(DataStream<Tuple18<String, Integer, String, String, Integer, String, String, String, String, String, Integer, Double, Double, String, String, String, String, String>> stream) {
            return stream
                    .map(new mapea18a4())
                    .keyBy(2)
                    //agregate purchases per color
                    .reduce(new ReduceFunction<Tuple4<String, String, String, Integer>>() {
                        public Tuple4<String, String, String, Integer> reduce(Tuple4<String, String, String, Integer> a, Tuple4<String, String, String, Integer> b) {
                            return new Tuple4<String, String, String, Integer>(a.f0, b.f1, a.f2, a.f3 + b.f3);
                        }
                    });
    }

    @org.junit.Test
    public void testTotalColores() {

            // Define the input DataStream:
            DataStream<Tuple18<String, Integer, String, String, Integer, String, String, String, String, String, Integer, Double, Double, String, String, String, String, String>> testStream2 =
                    createTimedTestStreamWith(Tuple18.of("INDITEXTABLE", 0, "2017/05/19 11:27:19", "App", 2294, "PullBear", "Hombre", "ESPANA", "Oeste", "Europa", 10842, 85.26, 9.04, "Marron", "S", "JerseyLaMarS", "Largo", "Jersey"))
                            .emit(Tuple18.of("INDITEXTABLE", 1, "2017/05/19 11:27:20", "App", 2294, "PullBear", "Hombre", "ESPANA", "Oeste", "Europa", 10298, 190.92, 6.4, "Coral", "L", "ChaquetaCoCorL", "Corto", "Chaqueta"))
                            .emit(Tuple18.of("INDITEXTABLE", 2, "2017/05/19 11:27:21", "Tarjeta", 2294, "PullBear", "Hombre", "ESPANA", "Oeste", "Europa", 8531, 97.06, 3.1, "Granate", "XXL", "ChaquetaMiGraXXL", "Midi", "Chaqueta"))
                            .emit(Tuple18.of("INDITEXTABLE", 3, "2017/05/19 11:27:22", "Tarjeta", 2294, "PullBear", "Hombre", "ESPANA", "Oeste", "Europa", 10588, 48.38, 7.56, "Negro", "XS", "FaldaLaNegXS", "Largo", "Falda"))
                            .emit(Tuple18.of("INDITEXTABLE", 10, "2017/05/19 11:27:23", "Efectivo", 7303, "WebPull", "Web", "GEORGIA", "Web", "Europa", 11682, 64.05, 6.29, "Negro", "S", "ChaquetonLaNegS", "Largo", "Chaqueton"))
                            .emit(Tuple18.of("INDITEXTABLE", 20, "2017/05/19 11:27:24", "Affinity", 4035, "Bershka", "Mujer", "ESPANA", "Este", "Europa", 8149, 179.46, 9.76, "Beige", "M", "PantalonLaBeiM", "Largo", "Pantalon"))
                            .emit(Tuple18.of("INDITEXTABLE", 21, "2017/05/19 11:27:25", "App", 4035, "Bershka", "Mujer", "ESPANA", "Este", "Europa", 13061, 142.49, 3.0, "Rosa", "XS", "GabardinaLaRosXS", "Largo", "Gabardina"))
                            .close();
            OutputMatcher<Tuple4<String, String, String, Integer>> matcher =
                    //name the values in the tuple with keys:
                    new MatchTuples<Tuple4<String, String, String, Integer>>("destino", "fecha", "color", "cantidad")
                            //Shouldn't be more than 2 of the same color.
                            .assertThat("cantidad", lessThan(3))
                            //Confirm that there are no strange colors on the list.
                            .assertThat("color", either(is("Marron")).or(is("Coral")).or(is("Granate")).or(is("Negro")).or(is("Beige")).or(is("Rosa")))
                            //define how many records need to fulfill the condition
                            .onEachRecord();

            assertStream(TopColores(testStream2), matcher);
        }
    //*******************************************************************************
    public class mapea18a6 implements MapFunction<Tuple18<String, Integer, String, String, Integer, String, String, String, String, String, Integer, Double, Double, String, String, String, String, String>, Tuple6<String, String, String, String, String, Integer>> {
        @Override
        public Tuple6<String, String,String, String, String, Integer> map(Tuple18<String, Integer, String, String, Integer, String, String, String, String, String, Integer, Double, Double, String, String, String, String, String> tupla18) throws Exception {
            return new Tuple6<String, String, String, String, String, Integer>(tupla18.f0, tupla18.f2, tupla18.f5,tupla18.f16,tupla18.f17, 1);
        }
    }


    public DataStream<Tuple6<String,String, String, String, String, Integer>>
    TopPrendas(DataStream<Tuple18<String, Integer, String, String, Integer, String, String, String, String, String, Integer, Double, Double, String, String, String, String, String>> stream) {
        return stream
                .map(new mapea18a6())
                .keyBy(2,3,4)
                //.window(SlidingProcessingTimeWindows.of(Time.hours(1), Time.seconds(30)))
                //agregate purchases per color
                .reduce(new ReduceFunction<Tuple6<String,String,String, String, String, Integer>>() {
                    public Tuple6<String,String,String, String, String, Integer> reduce(Tuple6<String,String,String, String, String, Integer> a, Tuple6<String,String,String, String, String, Integer> b) {
                        return new Tuple6<String,String,String, String, String, Integer>(a.f0, b.f1, a.f2, a.f3,a.f4,a.f5+ b.f5);
                    }
                });
    }

    @org.junit.Test
    public void testTotalPrendas() {

        // Define the input DataStream:
        DataStream<Tuple18<String, Integer, String, String, Integer, String, String, String, String, String, Integer, Double, Double, String, String, String, String, String>> testStream3 =
                createTimedTestStreamWith(Tuple18.of("INDITEXTABLE", 0, "2017/05/19 11:27:19", "App", 2294, "PullBear", "Hombre", "ESPANA", "Oeste", "Europa", 10842, 85.26, 9.04, "Marron", "S", "JerseyLaMarS", "Largo", "Jersey"))
                        .emit(Tuple18.of("INDITEXTABLE", 1, "2017/05/19 11:27:20", "App", 2294, "PullBear", "Hombre", "ESPANA", "Oeste", "Europa", 10298, 190.92, 6.4, "Coral", "L", "ChaquetaCoCorL", "Corto", "Chaqueta"))
                        .emit(Tuple18.of("INDITEXTABLE", 2, "2017/05/19 11:27:21", "Tarjeta", 2294, "PullBear", "Hombre", "ESPANA", "Oeste", "Europa", 8531, 97.06, 3.1, "Granate", "XXL", "ChaquetaMiGraXXL", "Midi", "Chaqueta"))
                        .emit(Tuple18.of("INDITEXTABLE", 3, "2017/05/19 11:27:22", "Tarjeta", 2294, "PullBear", "Hombre", "ESPANA", "Oeste", "Europa", 10588, 48.38, 7.56, "Negro", "XS", "FaldaLaNegXS", "Largo", "Falda"))
                        .emit(Tuple18.of("INDITEXTABLE", 10, "2017/05/19 11:27:23", "Efectivo", 7303, "WebPull", "Web", "GEORGIA", "Web", "Europa", 11682, 64.05, 6.29, "Negro", "S", "ChaquetonLaNegS", "Largo", "Chaqueton"))
                        .emit(Tuple18.of("INDITEXTABLE", 20, "2017/05/19 11:27:24", "Affinity", 4035, "Bershka", "Mujer", "ESPANA", "Este", "Europa", 8149, 179.46, 9.76, "Beige", "M", "PantalonLaBeiM", "Largo", "Pantalon"))
                        .emit(Tuple18.of("INDITEXTABLE", 21, "2017/05/19 11:27:25", "App", 4035, "Bershka", "Mujer", "ESPANA", "Este", "Europa", 13061, 142.49, 3.0, "Rosa", "XS", "GabardinaLaRosXS", "Largo", "Gabardina"))
                        .close();
        OutputMatcher<Tuple6<String,String,String, String, String, Integer>> matcher =
                //name the values in the tuple with keys:
                new MatchTuples<Tuple6<String,String,String, String, String, Integer>>("destino", "fecha", "cadena", "modelo","clase","cantidad")
                        // Shouldn't be more than one of each peace of clothing.
                        .assertThat("cantidad", lessThanOrEqualTo(1))
                        //express how many matchers must return true for your test to pass:
                        //.anyOfThem()
                        //define how many records need to fulfill the condition
                        .onEachRecord();

        assertStream(TopPrendas(testStream3), matcher);
    }
    //*******************************************************************************


}

