package project.com.masterbd.Datos

import java.util.Iterator

import com.fasterxml.jackson.databind.JsonNode

/**
  * Original object received through JSON.
  */
object datoOriginal {

  case class original(val id_transaction: Int,
                      val id_Tienda: Int,
                      val fecha: Int,
                      val metodoPago: String,
                    // Iterator used here due to array of Strings being received on the Json
                      val prendas: Iterator[JsonNode]) {
    def this() {
      this(0,0, 0, "", null)
    }

  }

}
