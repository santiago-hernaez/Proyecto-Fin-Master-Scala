package MasterBD.Utiles

import org.apache.flink.api.common.functions.RichFlatMapFunction
import MasterBD.Datos.datoOriginal.original
import MasterBD.Datos.datoEnriquecido.enriquecido
import com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.util.Collector
import org.apache.flink.configuration.Configuration
//REDIS:
import scala.collection.JavaConversions._
import redis.clients.jedis.Jedis

/**
  * Created by sam on 5/04/17.
  */

class Enricher extends RichFlatMapFunction[original,enriquecido] {

  var jedis : Jedis = _

  override def open(parameters: Configuration): Unit = {
    //abrimos JEDIS
    jedis = new Jedis("localhost")
  }

  override def close():Unit = {
    //cerramos JEDIS
    jedis.quit()
  }

  override def flatMap(in:(original),out:Collector[enriquecido]): Unit= {
    val origen: original = in
    val id_tienda = origen.id_Tienda
    val prendas = new Array[String](4)
    val fecha = origen.fecha
    val metodoPago = origen.metodoPago
    var pr: JsonNode = null


    //Creamos un Iterador con el Parse de prendas del JSON
    val it = origen.prendas
    var num_prendas: Int = 0
    //metemos cada una de las prendas del Json en un array de Strings del tipo ["id_prenda:precio","id_prenda:precio",...]
    while (it.hasNext) {
      pr = it.next()
      prendas(num_prendas) = pr.asText()
      num_prendas += 1
    }

    //datos de tienda
    val cadena = jedis.hget(id_tienda.toString, "cadena")
    val sexo = jedis.hget(id_tienda.toString, "sexo")
    val pais = jedis.hget(id_tienda.toString, "pais")
    val region = jedis.hget(id_tienda.toString, "region")
    val zona = jedis.hget(id_tienda.toString, "zona")

    //datos de prenda.Sacaremos una prenda por cada elemento del array de prendas.
    for (x <- 0 until (num_prendas-1)) {
      val prenda = prendas(x).split(":")(0)
      val precio = prendas(x).split(":")(1).toDouble
      val beneficio: Double = jedis.hget(prenda, "ben").toDouble
      val color = jedis.hget(prenda, "color")
      val talla = jedis.hget(prenda, "talla")
      val nombre = jedis.hget(prenda, "nombre")
      val modelo = jedis.hget(prenda, "modelo")
      val clase = jedis.hget(prenda, "clase")

      //Obtenemos la salida del Objeto.Con la tienda y la prenda
      out.collect(enriquecido.apply(
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
