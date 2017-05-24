package project.com.masterbd

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * mvn clean package -Pbuild-jar
 */

import project.com.masterbd.Datos.datoOriginal.original
import project.com.masterbd.Datos.datoEnriquecido.enriquecido
import project.com.masterbd.Utiles.{Enricher, HBaseMapperP, HBaseSink}
import project.com.masterbd.Datos.datoEnriquecido
import java.util
import java.util.Properties

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object InditexTicketControl extends App{

    // JSON received: {"metodoPago":"TarjetaRegalo","fecha":1491205741,"Prendas":["9382:51.08","10371:80.28"],"id_Tienda":4879,"id_Transaccion":12345}

        //Kafka Consumer creation
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "localhost:9092")
        properties.setProperty("group.id", "Inditex")
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val consumerKafka = new FlinkKafkaConsumer010[ObjectNode]("inditex", new JSONDeserializationSchema(), properties)
        val stream = env
            .addSource(consumerKafka)

        //Start Parse and Enrich process.

         val streamEnriquecido = stream
           //Parse the entry Json to its primitives
           .map(r => original(r.get("id_Transaccion").asInt(),r.get("id_Tienda").asInt(),r.get("fecha").asInt(),r.get("metodoPago").asText(),r.get("Prendas").elements()))

           //Enriches Json with Redis Data.
           .flatMap(new Enricher())

        //Save enriched data to Hbase.
         streamEnriquecido.addSink(new HBaseSink("INDITEXTABLE",new HBaseMapperP()))

      // STARTING DATA CALCULATION

      // Sliding window with total sales per store and brand last hour calculated every 30 seconds.
          val storesales = streamEnriquecido
              //remove undesired fields from the stream
              .map(r=>(r.destino,r.fecha,r.id_tienda.toString(),r.cadena,r.sexo,r.pais,r.region,r.zona,r.precio))
              //group by id_tienda and set window time.
              .keyBy(_._3)
              .window(SlidingProcessingTimeWindows.of(Time.hours(1), Time.seconds(30)))
              //agregate purchases per id_tienda
              .reduce{(a,b)=>(a._1,b._2,a._3,a._4,a._5,a._6,a._7,a._8,a._9+b._9)}

          //Order stream and print on StdOut the Top5 stores by sales of each brand.
          /*
            storesales.map(r=>(r._2,r._3,r._4,r._5,r._6,r._7,r._8,r._9))
             .keyBy(_._3).timeWindow (Time.minutes(60),Time.seconds(5))
             .apply ((key,window,input,out:Collector[(String)]) => {
               //send the iterator to a List, sort the list by sales and reverse
               var tienda = input.toList
               val top5 = tienda.sortBy(_._8).reverse.take(5)
               out.collect(top5.toString())
             })
             .print()
          */

            //map data into POJO datoEnriquecido adding 'ventasPorTienda' as database to save into and save all in HBase.
            //POJO Fields: (0)destino,(1)id_transaccion,(2)fecha,(3)metodoPago,(4)id_tienda,(5)cadena,(6)sexo,
            // (7)pais,(8)region,(9)zona,(10)prenda,(11)precio,(12)beneficio,(13)color,(14)talla,(15)nombre,(16)modelo,(17)clase

            storesales
             .map(r=>datoEnriquecido.enriquecido ("VENTASPORTIENDA",0,r._2,"",r._3,r._4,r._5,r._6,r._7,r._8,"",r._9,0D,"","","","",""))
             .addSink(new HBaseSink("VENTASPORTIENDA",new HBaseMapperP()))


        // Top 5 clothes model and class sold last hour per brand: Window (1h,30")

          val clothesSales = streamEnriquecido
            .map(r=>("TOPPRENDAS",r.fecha,r.cadena,r.clase,r.modelo,1))
            //agregate purchases per brand, class and model
            .keyBy{r=>r._3+r._4+r._5}
            .window(SlidingProcessingTimeWindows.of(Time.hours(1), Time.seconds(30)))
            .reduce{(a,b)=>(a._1,b._2,a._3,a._4,a._5,a._6+b._6)}

            //Order stream and print on StdOut the Top5 clothes type by brand.
            /*
            clothesSales
              .keyBy(_._3).timeWindow (Time.minutes(60),Time.seconds(30))
              .apply ((key,window,input,out:Collector[(String)]) => {
                //send the iterator to a List, sort the list by num of sold clothes and reverse
                var prendas = input.toList
                val top5 = prendas.sortBy(_._6).reverse.take(5)
                out.collect(top5.toString())
                })
              .print()
            */

              // Save everything onto Hbase. I use id_transaccion to pass the agregate (int)
              clothesSales
              .map(r=>datoEnriquecido.enriquecido(r._1,r._6,r._2,"","",r._3,"","","","","",0D,0D,"","","",r._5,r._4))
              .addSink(new HBaseSink("TOPPRENDAS",new HBaseMapperP()))

        // TOP 5 colours sold last hour: Window (1h,30'')
              val colorSales = streamEnriquecido
                .map(r=>("TOPCOLORES",r.fecha,r.color,1))
                .keyBy(_._3)
                .window(SlidingProcessingTimeWindows.of(Time.hours(1), Time.seconds(30)))
                //agregate purchases per color
                .reduce{(a,b)=>(a._1,b._2,a._3,a._4+b._4)}

             /*
              colorSales
                //Order stream and print on StdOut the Top5 sales by color.
                .keyBy(_._1).timeWindow (Time.minutes(60),Time.seconds(30))
                .apply ((key,window,input,out:Collector[(String)]) => {
                  //send the iterator to a List, sort the list by sales and reverse
                  var colors = input.toList
                  val top5 = colors.sortBy(_._4).reverse.take(5)
                  out.collect(top5.toString())
                })
                .print()
              */

              // save all data onto Hbase. (older with .writeUsingOutputFormat(new toHbase() now with sink)
              colorSales
              .map(r=>datoEnriquecido.enriquecido(r._1,r._4,r._2,"","","","","","","","",0D,0D,r._3,"","","",""))
              .addSink(new HBaseSink("TOPCOLORES",new HBaseMapperP()))


         // Total sales per Zone (Asia, Latinoamerica, Europa) window (1h)

          streamEnriquecido.map(r=>("VENTASPORZONA",r.fecha,r.zona,r.pais,r.precio))
            .keyBy(_._3).window(TumblingProcessingTimeWindows.of(Time.minutes(60)))
            .reduce{(a,b)=>(a._1,b._2,a._3,a._4,a._5+b._5)}
            .map(r=>datoEnriquecido.enriquecido (r._1,0,r._2,"","","","",r._4,"",r._3,"",r._5,0D,"","","","",""))
            .addSink(new HBaseSink("VENTASPORZONA",new HBaseMapperP()))
/*
        // Total Sales per brand: Window (1h)

          streamEnriquecido.map(r=>("VENTASPORCADENA",r.fecha,r.cadena,r.precio))
          .keyBy(_._3).window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
          .reduce{(a,b)=>(a._1,b._2,a._3,a._4+b._4)}
          .map(r=>new datoEnriquecido.enriquecido (r._1,0,r._2,"","",r._3,"","","","","",r._4,0D,"","","","",""))
          .addSink(new HBaseSink("VENTASPORCADENA",new HBaseMapperP()))
*/

        env.execute("Scala-Flink Ticket Control")
}
