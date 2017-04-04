package MasterBD.Datos
/**
  * Created by sam on 3/04/17.
  */
object datoEnriquecido {
    case class enriched (
                        val fecha : Int,
                        val metodoPago: String,
                        val id_tienda:Int,
                        val cadena : String,
                        val sexo: String,
                        val pais: String,
                        val region: String,
                        val zona: String,
                        val id_prenda: String,
                        val precio: Double,
                        val color: String,
                        val talla: String,
                        val nombre: String,
                        val modelo: String,
                        val clase: String,
                        val beneficio: Double
                        )

}
