package com.MasterBD.Datos


import java.util.Iterator

import com.fasterxml.jackson.databind.JsonNode

/**
  * Created by sam on 3/04/17.
  */
object datoOriginal {

  case class original(val id_Tienda: Int,
                    val fecha: Int,
                    val metodoPago: String,
                    // Iterator used here due to array of Strings being received on the Json
                    val prendas: Iterator[JsonNode]) {
    def this() {
      this(0, 0, "", null)
    }

  }

}
