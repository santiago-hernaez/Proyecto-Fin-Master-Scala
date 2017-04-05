package com.MasterBD


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
 */

import com.MasterBD.Datos.datoOriginal.original
import com.MasterBD.Datos.datoEnriquecido.enriquecido
import com.MasterBD.Utiles.Enricher
//import MasterBD.Utiles.enriquecedor  <-- Antigua

import java.util
import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema

import com.fasterxml.jackson.databind.node.ObjectNode


object InditexTicketControl {

    // JSON received: {"metodoPago":"TarjetaRegalo","fecha":1491205741,"Prendas":["9382:51.08","10371:80.28"],"id_Tienda":4879}

    def main(args: Array[String]):Unit = {

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
           //Parsea el Json de entrada a sus variables primitivas
           .map(r => original(r.get("id_Tienda").asInt(),r.get("fecha").asInt(),r.get("metodoPago").asText(),r.get("Prendas").elements()))

           //enriquece el origen con los datos de Redis.
           .flatMap(new Enricher())

           .print()

        /*.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(0)
      .sum(1)*/


        env.execute("Scala-Flink Ticket Control")
    }
}
