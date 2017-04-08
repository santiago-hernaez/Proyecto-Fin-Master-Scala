package project.com.masterbd.Utiles

/**
  * Objecto que enriquece el flujo original de JSON con datos obtenidos de Redis en base a un Id_Prenda e Id_Tienda
  */


import project.com.masterbd.Datos.datoEnriquecido.enriquecido
import project.com.masterbd.Datos.datoOriginal.original
import com.fasterxml.jackson.databind.JsonNode

//REDIS:
import scala.collection.JavaConversions._
import redis.clients.jedis.Jedis

object enriquecedor{
  def enriquece (origen:original):enriquecido = {
    val id_tienda = origen.id_Tienda
    val prendas = new Array[String](4)
    val fecha = origen.fecha
    val metodoPago = origen.metodoPago
    var pr:JsonNode = null

    //abrimos JEDIS
    val jedis = new Jedis("localhost")

    //Creamos un Iterador con el Parse de prendas del JSON
    val it = origen.prendas
    var i: Int = 0
    //metemos cada una de las prendas del Json en un array de Strings del tipo ["id_prenda:precio","id_prenda:precio",...]
    while (it.hasNext) {
      pr = it.next()
      prendas(i) = pr.asText()
      i += 1
    }
    //datos de tienda
    val cadena = jedis.hget(id_tienda.toString,"cadena")
    val sexo = jedis.hget(id_tienda.toString,"sexo")
    val pais = jedis.hget(id_tienda.toString,"pais")
    val region = jedis.hget(id_tienda.toString,"region")
    val zona = jedis.hget(id_tienda.toString,"zona")
    //datos de prenda, de momento sin iterar por el array de prendas.
    val prenda = prendas(0).split(":")(0)
    val precio = prendas(0).split(":")(1).toDouble
    val color = jedis.hget(prenda,"color")
    val talla = jedis.hget(prenda,"talla")
    val nombre = jedis.hget(prenda,"nombre")
    val modelo = jedis.hget(prenda,"modelo")
    val clase = jedis.hget(prenda,"clase")
    val beneficio = jedis.hget(prenda, "ben").toDouble

    //Obtenemos la salida del Objeto.
    enriquecido(origen.fecha,
      origen.metodoPago,
      origen.id_Tienda,
      cadena.toString,
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
      clase
      )

  }

}
