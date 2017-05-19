package project.com.masterbd.Utiles

import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import org.apache.flink.api.common.functions.RichFlatMapFunction
import com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.util.Collector
import org.apache.flink.configuration.Configuration
import project.com.masterbd.Datos.datoEnriquecido.enriquecido
import project.com.masterbd.Datos.datoOriginal.original

//REDIS:
import scala.collection.JavaConversions._
import redis.clients.jedis.Jedis

/**
  * Enricher class to enrich original Json with info from Redis Database.
  */

class Enricher extends RichFlatMapFunction[original,enriquecido] {

  var jedis : Jedis = _
  var i=0
  override def open(parameters: Configuration): Unit = {
    //Open JEDIS
    jedis = new Jedis("localhost")

    }

  override def close():Unit = {
    //Close JEDIS
    jedis.quit()
  }

  override def flatMap(in:(original),out:Collector[enriquecido]): Unit= {
    val origen: original = in
    val destino = "INDITEXTABLE"

    val id_tienda = origen.id_Tienda.toString()
    val prendas = new Array[String](4)

    val fechaOriginal = new Date(origen.fecha*1000L).toString()
    var formatter: DateFormat = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy")
    var dat = formatter.parse(fechaOriginal)
    val fecha = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(dat)



    val metodoPago = origen.metodoPago
    var pr: JsonNode = null


    //Iterator creation with the JSON Parse of the clothes array
    val it = origen.prendas
    var num_prendas: Int = 0
    //each of the JSON clothes is entered on an array like ["id_prenda:precio","id_prenda:precio",...]
    while (it.hasNext) {
      pr = it.next()
      prendas(num_prendas) = pr.asText()
      num_prendas += 1
    }

    //Store data
    val cadena = jedis.hget(id_tienda, "cadena")
    val sexo = jedis.hget(id_tienda, "sexo")
    val pais = jedis.hget(id_tienda, "pais")
    val region = jedis.hget(id_tienda, "region")
    val zona = jedis.hget(id_tienda, "zona")

    //clothes data. We pull one set of values for each element on the clothes array.
    for (x <- 0 until (num_prendas)) {
      val id_transaccion = (origen.id_transaction*10)+x
      val prenda = prendas(x).split(":")(0)
      val precio = prendas(x).split(":")(1).toDouble
      val beneficio: Double = jedis.hget(prenda, "ben").toDouble
      val color = jedis.hget(prenda, "color")
      val talla = jedis.hget(prenda, "talla")
      val nombre = jedis.hget(prenda, "nombre")
      val modelo = jedis.hget(prenda, "modelo")
      val clase = jedis.hget(prenda, "clase")

      //Set the enriched object with the obtained data.

      out.collect(enriquecido.apply(
        destino,
        id_transaccion,
        fecha,
        metodoPago,
        id_tienda,
        cadena,
        sexo,
        pais,
        region,
        zona,
        prenda,
        precio,
        beneficio,
        color,
        talla,
        nombre,
        modelo,
        clase))
      }
  }

}
